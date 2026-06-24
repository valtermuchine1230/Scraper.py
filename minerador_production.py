#!/usr/bin/env python3
"""
minerador_production.py — Pipeline offline big-data (sem Bloom Filter).

INSTALAÇÃO:
    pip install -r requirements.txt

ARQUITETURA (4 ETAPAS):
  ETAPA 1 — Extração bruta: torrents → mmap/regex → raw_chunk_*.parquet (sem dedup)
  ETAPA 2 — Deduplicação global em disco: DuckDB DISTINCT + anti-join exported_emails
  ETAPA 3 — Exportação em lotes: emails_batch_0001.(parquet|csv) — 30M linhas/lote
  ETAPA 4 — Upload HF: apenas batches finais (export ledger / exactly-once)

PERSISTÊNCIA:
  state.json          — checkpoint, export_ledger, etapas
  emails.duckdb       — emails_raw, emails_deduplicated, exported_emails
  minerador_checkpoints no HF — retomada
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import tarfile
import signal
import logging
import shutil
import gc
import mmap
import hashlib
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional
from threading import Event, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

try:
    import libtorrent as lt
except ImportError:
    print("❌ ERROR: python-libtorrent not installed")
    print("   Executar: pip install python-libtorrent==2.0.9")
    sys.exit(1)

try:
    from huggingface_hub import HfApi
except ImportError:
    print("❌ ERROR: huggingface_hub not installed")
    print("   Executar: pip install 'huggingface-hub>=0.21.0'")
    sys.exit(1)

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("❌ ERROR: pandas ou pyarrow not installed")
    print("   Executar: pip install 'pandas>=2.0.0' 'pyarrow>=14.0.0'")
    sys.exit(1)

try:
    import duckdb
except ImportError:
    print("❌ ERROR: duckdb not installed")
    print("   Executar: pip install 'duckdb>=0.9.0'")
    sys.exit(1)

try:
    from rich.logging import RichHandler
    from rich.console import Console
except ImportError:
    print("❌ ERROR: rich not installed")
    print("   Executar: pip install 'rich>=13.0.0'")
    sys.exit(1)


# =====================================================================
# ⚙️  CONFIGURAÇÃO
# =====================================================================
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
RAW_CHUNKS_DIR = SAVE_PATH / "raw_chunks"
DEDUP_DIR = SAVE_PATH / "dedup"

DB_PATH = SAVE_PATH / "emails.duckdb"
STATE_PATH = SAVE_PATH / "state.json"
LOG_PATH = SAVE_PATH / "minerador.log"
PROCESSED_CHUNKS_PATH = SAVE_PATH / "processed_chunks.json"
TORRENT_STATE_PATH = SAVE_PATH / "torrent_state.json"

DEDUP_PARQUET_PATH = DEDUP_DIR / "emails_deduplicated.parquet"

for d in [EXPORT_DIR, TEMP_DIR, RAW_CHUNKS_DIR, DEDUP_DIR]:
    d.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO_EMAILS = os.environ.get("HF_REPO_EMAILS", "emails_dataset")
HF_REPO_CHECKPOINT = os.environ.get("HF_REPO_CHECKPOINT", "minerador_checkpoints")

CHECKPOINT_INTERVAL_MIN = int(os.environ.get("CHECKPOINT_INTERVAL_MIN", "15"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "3"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(64 * 1024 * 1024)))
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(512 * 1024 * 1024)))

# Lotes de exportação: 30M (requisito)
EXPORT_BATCH_SIZE = int(os.environ.get("EXPORT_BATCH_SIZE", "30000000"))
EXPORT_FORMAT = os.environ.get("EXPORT_FORMAT", "parquet").lower()  # parquet | csv

DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "8"))
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB")
DUCKDB_TEMP_DIR = os.environ.get("DUCKDB_TEMP_DIR", str(TEMP_DIR))

EXPORT_LEDGER_KEY = "export_ledger"
EXPORT_ACTIVE_PLAN_KEY = "export_active_plan"
STAGE_KEY = "pipeline_stage"  # raw | dedup | export | done

stop_event = Event()
state_lock = Lock()
ledger_lock = Lock()

_g_api: Optional[HfApi] = None
_g_token: Optional[str] = None
_g_checkpoint_repo: Optional[str] = None
_g_periodic_timer: Optional[threading.Timer] = None

_cycle_exported_batch = False
_cycle_had_pending_at_check = False
_cycle_ledger_complete = False

# =====================================================================
# 📙 LOGGING
# =====================================================================
console = Console()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger("minerador")
logger.setLevel(LOG_LEVEL)

file_handler = logging.FileHandler(str(LOG_PATH))
file_handler.setLevel(LOG_LEVEL)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(file_handler)

E = {
    "start": "▶️",
    "download": "📥",
    "extract": "⏬",
    "stats": "📙",
    "space": "🔽",
    "email": "📧",
    "upload": "📨",
    "clean": "♻️",
    "warn": "⚠️",
    "error": "❌",
    "ok": "✅",
    "info": "🔈",
    "cpu": "⏯",
    "db": "💳",
    "skip": "🚫",
    "checkpoint": "📩",
    "signal": "❕",
    "integrity": "❇️",
    "loop": "➿",
    "export": "📤",
    "ledger": "📒",
    "dedup": "🧹",
}

EMAIL_REGEX = re.compile(rb"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", re.IGNORECASE)

MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": (
            "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD"
            "&dn=Collection%20%232-%235%20%26%20Antipublic"
            "&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce"
            "&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce"
            "&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce"
            "&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce"
            "&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce"
            "&tr=udp%3a%2f%2ftracker.opentracker.i2p.rocks%3a6969%2fannounce"
            "&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce"
            "&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
        ),
        "targets": [
            "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
            "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz",
        ],
    },
    {
        "name": "Collection #1",
        "magnet": (
            "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E"
            "&dn=Collection%201"
            "&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce"
            "&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce"
            "&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce"
            "&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce"
        ),
        "targets": [
            "Collection #1/Collection #1_BTC combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_BTC combos.tar.gz",
        ],
    },
]

DISPOSABLE_DOMAINS = {
    "tempmail.com", "temp-mail.org", "10minutemail.com", "throwaway.email",
    "guerrillamail.com", "mailinator.com", "yopmail.com", "maildrop.cc",
    "trashmail.com", "fakeinbox.com", "mailnesia.com", "tempmail.email",
    "sharklasers.com", "spam4.me", "spamgourmet.com", "tempmail.us",
    "mytrashmail.com", "mailnesia.net", "temporary-mail.net",
    "grr.la", "temp-mail.io", "tempmail24.com", "maildisposable.com",
    "temp-mail.info", "minute-mail.com", "trash-mail.com",
    "10minutemailbox.com", "tempmail.it", "fakeemail.net",
    "mailbox.ga", "oneclickmail.com", "temp.email", "trashmail.ws",
    "temp.mail", "speedymail.org", "emailondeck.com", "schrott.email",
    "mail1.eu", "tempmail.pro", "temp-mailbox.com", "mailtest.in",
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com",
    "hotmail.com", "outlook.com", "live.com", "msn.com",
    "aol.com", "mail.com", "inbox.com", "fastmail.com",
    "protonmail.com", "tutanota.com", "zoho.com", "mail.ru",
    "rambler.ru", "yandex.com", "yandex.ru", "mail.ua",
    "ukr.net", "qq.com", "163.com", "126.com",
    "sina.com", "sohu.com", "foxmail.com", "tom.com",
    "vip.qq.com", "vip.sina.com", "163.net", "126.net",
}


def handle_signal(signum, frame):
    logger.warning(f"{E['signal']} Signal {signum} recebido — a encerrar com segurança...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def normalize_string_robust(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    s = re.sub(r"\s+", " ", s)
    s = s.replace("\\", "/")
    return s.strip().lower()


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"


def disk_usage(path: Path = SAVE_PATH) -> Dict[str, int]:
    du = shutil.disk_usage(str(path))
    return {"total": du.total, "used": du.used, "free": du.free}


def is_disposable_email(email: str) -> bool:
    try:
        return email.split("@")[-1].lower() in DISPOSABLE_DOMAINS
    except Exception:
        return False


def save_state(state: Dict[str, Any]):
    with state_lock:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f, indent=2, default=str)


def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_processed_chunks(state: Dict[str, Any]):
    data = {
        "loaded_chunks": state.get("loaded_chunks", []),
        "processed_tars": state.get("processed_tars", []),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    with open(PROCESSED_CHUNKS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def save_torrent_state(state: Dict[str, Any]):
    data = {
        "downloaded_files": state.get("downloaded_files", {}),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    with open(TORRENT_STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


def merge_checkpoint_into_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if PROCESSED_CHUNKS_PATH.exists():
        try:
            with open(PROCESSED_CHUNKS_PATH) as f:
                c = json.load(f)
            state.setdefault("loaded_chunks", c.get("loaded_chunks", []))
            state.setdefault("processed_tars", c.get("processed_tars", []))
            logger.info(f"{E['ok']} processed_chunks.json fundido no estado")
        except Exception as e:
            logger.warning(f"{E['warn']} Falha ao ler processed_chunks.json: {e}")

    if TORRENT_STATE_PATH.exists():
        try:
            with open(TORRENT_STATE_PATH) as f:
                t = json.load(f)
            state.setdefault("downloaded_files", t.get("downloaded_files", {}))
            logger.info(f"{E['ok']} torrent_state.json fundido no estado")
        except Exception as e:
            logger.warning(f"{E['warn']} Falha ao ler torrent_state.json: {e}")

    ensure_export_ledger(state)
    return state


def verify_file_integrity(path: Path, min_size: int = 10) -> bool:
    if not path.exists():
        logger.warning(f"{E['integrity']} Ficheiro não encontrado: {path.name}")
        return False
    size = path.stat().st_size
    if size < min_size:
        logger.warning(
            f"{E['integrity']} Ficheiro suspeito: {path.name} ({size} bytes)"
        )
        return False
    logger.info(f"{E['integrity']} Integridade OK: {path.name} ({human(size)})")
    return True


# =====================================================================
# 📒 EXPORT LEDGER (EXACTLY-ONCE)
# =====================================================================
def ensure_export_ledger(state: Dict[str, Any]) -> Dict[str, Any]:
    with ledger_lock:
        ledger = state.get(EXPORT_LEDGER_KEY)
        if not isinstance(ledger, dict):
            ledger = {"exported_batches": []}
        if "exported_batches" not in ledger or not isinstance(ledger["exported_batches"], list):
            ledger["exported_batches"] = []
        state[EXPORT_LEDGER_KEY] = ledger
        return ledger


def get_export_ledger(state: Dict[str, Any]) -> Dict[str, Any]:
    return ensure_export_ledger(state)


def ledger_find_entry(ledger: Dict[str, Any], batch_id: str) -> Optional[Dict[str, Any]]:
    for entry in ledger.get("exported_batches", []):
        if entry.get("batch_id") == batch_id:
            return entry
    return None


def ledger_is_uploaded(state: Dict[str, Any], batch_id: str) -> bool:
    ledger = get_export_ledger(state)
    entry = ledger_find_entry(ledger, batch_id)
    return bool(entry and entry.get("status") == "uploaded")


def ledger_max_batch_number(state: Dict[str, Any]) -> int:
    ledger = get_export_ledger(state)
    max_n = 0
    for entry in ledger.get("exported_batches", []):
        bid = entry.get("batch_id", "")
        m = re.match(r"^emails_batch_(\d+)$", bid)
        if m:
            max_n = max(max_n, int(m.group(1)))
    plan = state.get(EXPORT_ACTIVE_PLAN_KEY) or {}
    for b in plan.get("batches", []):
        bid = b.get("batch_id", "")
        m = re.match(r"^emails_batch_(\d+)$", bid)
        if m:
            max_n = max(max_n, int(m.group(1)))
    return max_n


def compute_file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ledger_upsert_entry(state: Dict[str, Any], entry: Dict[str, Any]) -> None:
    with ledger_lock:
        ledger = get_export_ledger(state)
        batch_id = entry["batch_id"]
        existing = ledger_find_entry(ledger, batch_id)
        if existing:
            existing.update(entry)
        else:
            ledger["exported_batches"].append(entry)
        state[EXPORT_LEDGER_KEY] = ledger


def persist_export_ledger(state: Dict[str, Any]) -> bool:
    try:
        save_state(state)
        return True
    except Exception as e:
        logger.error(f"{E['error']} Falha ao persistir export_ledger: {e}")
        return False


def build_export_active_plan(state: Dict[str, Any], total_rows: int, batch_size: int) -> Dict[str, Any]:
    existing = state.get(EXPORT_ACTIVE_PLAN_KEY)
    if (
        isinstance(existing, dict)
        and existing.get("total_rows") == total_rows
        and existing.get("batch_size") == batch_size
        and isinstance(existing.get("batches"), list)
        and len(existing["batches"]) > 0
    ):
        logger.info(
            f"{E['ledger']} A retomar export_active_plan existente "
            f"({len(existing['batches'])} batches)"
        )
        return existing

    next_num = ledger_max_batch_number(state) + 1
    batches: List[Dict[str, Any]] = []
    offset = 0
    while offset < total_rows:
        limit = min(batch_size, total_rows - offset)
        batch_id = f"emails_batch_{next_num:04d}"
        status = "uploaded" if ledger_is_uploaded(state, batch_id) else "pending"
        batches.append({
            "batch_id": batch_id,
            "offset": offset,
            "limit": limit,
            "status": status,
        })
        next_num += 1
        offset += limit

    plan = {
        "run_id": datetime.now(timezone.utc).isoformat(),
        "total_rows": total_rows,
        "batch_size": batch_size,
        "batches": batches,
    }
    state[EXPORT_ACTIVE_PLAN_KEY] = plan
    persist_export_ledger(state)
    logger.info(
        f"{E['ledger']} Novo export_active_plan: {len(batches)} batches, "
        f"total_rows={total_rows:,}"
    )
    return plan


def active_plan_all_uploaded(plan: Dict[str, Any]) -> bool:
    batches = plan.get("batches", [])
    if not batches:
        return False
    return all(b.get("status") == "uploaded" for b in batches)


def clear_export_active_plan(state: Dict[str, Any]) -> None:
    if EXPORT_ACTIVE_PLAN_KEY in state:
        del state[EXPORT_ACTIVE_PLAN_KEY]
    persist_export_ledger(state)


# =====================================================================
# 💳 DUCKDB
# =====================================================================
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    Path(DUCKDB_TEMP_DIR).mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    conn.execute(f"SET temp_directory='{DUCKDB_TEMP_DIR}';")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS emails_raw (
            email   VARCHAR,
            nome    VARCHAR,
            origem  VARCHAR,
            data    VARCHAR
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS emails_deduplicated (
            email   VARCHAR,
            nome    VARCHAR,
            origem  VARCHAR,
            data    VARCHAR
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS exported_emails (
            email VARCHAR PRIMARY KEY
        );
    """)
    conn.commit()
    return conn


def table_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        r = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()
        return bool(r and r[0] > 0)
    except Exception:
        try:
            conn.execute(f"SELECT 1 FROM {name} LIMIT 1")
            return True
        except Exception:
            return False


def count_table(conn: duckdb.DuckDBPyConnection, name: str) -> int:
    try:
        if table_exists(conn, name):
            return int(conn.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0])
    except Exception as e:
        logger.warning(f"{E['warn']} COUNT {name} falhou: {e}")
    return 0


def list_unloaded_raw_chunks(state: Dict[str, Any]) -> List[Path]:
    loaded = set(state.get("loaded_chunks", []))
    chunks = sorted(RAW_CHUNKS_DIR.glob("raw_chunk_*.parquet"))
    return [p for p in chunks if str(p) not in loaded]


def load_raw_chunks_into_duckdb(
    conn: duckdb.DuckDBPyConnection,
    state: Dict[str, Any],
    api: HfApi,
    token: str,
    checkpoint_repo: str,
) -> int:
    """Ingestão streaming de shards Parquet → emails_raw (sem dedup)."""
    loaded = state.get("loaded_chunks", [])
    new_files = list_unloaded_raw_chunks(state)
    if not new_files:
        return 0

    logger.info(f"{E['db']} A carregar {len(new_files)} raw chunks → emails_raw")
    n_loaded = 0
    for chunk_file in new_files:
        if stop_event.is_set():
            break
        try:
            conn.execute(f"""
                INSERT INTO emails_raw
                SELECT email, nome, origem, data
                FROM read_parquet('{chunk_file}');
            """)
            conn.commit()
            loaded.append(str(chunk_file))
            state["loaded_chunks"] = loaded
            save_state(state)
            save_processed_chunks(state)
            n_loaded += 1
            logger.info(f"{E['db']} Carregado: {chunk_file.name}")
        except Exception as e:
            logger.exception(f"{E['error']} Falha read_parquet {chunk_file.name}: {e}")
    if n_loaded:
        upload_checkpoint_to_hf(api, token, checkpoint_repo)
    return n_loaded


# =====================================================================
# ETAPA 2 — DEDUPLICAÇÃO GLOBAL (DISCO)
# =====================================================================
def run_global_deduplication(
    conn: duckdb.DuckDBPyConnection,
    state: Dict[str, Any],
) -> int:
    """
    DISTINCT em disco via DuckDB + exclusão de emails já exportados (exported_emails).
    Substitui Bloom Filter — nunca usa set() com dataset inteiro em RAM.
    """
    raw_n = count_table(conn, "emails_raw")
    if raw_n == 0:
        logger.info(f"{E['info']} emails_raw vazio — nada para deduplicar")
        return count_table(conn, "emails_deduplicated")

    logger.info(
        f"{E['dedup']} ETAPA 2: dedup global (raw={raw_n:,}) — DuckDB DISTINCT + spill"
    )

    conn.execute("DROP TABLE IF EXISTS emails_deduplicated_new;")
    conn.execute("""
        CREATE TABLE emails_deduplicated_new AS
        SELECT DISTINCT
            LOWER(TRIM(r.email)) AS email,
            ANY_VALUE(r.nome)    AS nome,
            ANY_VALUE(r.origem)  AS origem,
            ANY_VALUE(r.data)    AS data
        FROM emails_raw r
        WHERE r.email IS NOT NULL AND TRIM(r.email) <> ''
        GROUP BY LOWER(TRIM(r.email));
    """)

    conn.execute("DROP TABLE IF EXISTS emails_to_export;")
    conn.execute("""
        CREATE TABLE emails_to_export AS
        SELECT d.email, d.nome, d.origem, d.data
        FROM emails_deduplicated_new d
        LEFT JOIN exported_emails e ON d.email = e.email
        WHERE e.email IS NULL;
    """)

    conn.execute("DROP TABLE IF EXISTS emails_deduplicated;")
    conn.execute("ALTER TABLE emails_to_export RENAME TO emails_deduplicated;")
    conn.execute("DROP TABLE IF EXISTS emails_deduplicated_new;")
    conn.commit()

    n = count_table(conn, "emails_deduplicated")
    logger.info(f"{E['stats']} emails_deduplicated (novos vs exportados): {n:,}")

    try:
        conn.execute(f"""
            COPY emails_deduplicated TO '{DEDUP_PARQUET_PATH}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        conn.commit()
        logger.info(f"{E['ok']} Snapshot: {DEDUP_PARQUET_PATH.name} ({human(DEDUP_PARQUET_PATH.stat().st_size)})")
    except Exception as e:
        logger.warning(f"{E['warn']} COPY dedup parquet falhou (tabela DuckDB OK): {e}")

    state[STAGE_KEY] = "dedup"
    state["dedup_completed_at"] = datetime.now(timezone.utc).isoformat()
    state["dedup_row_count"] = n
    save_state(state)
    return n


def register_exported_emails_from_batch(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    offset: int,
    limit: int,
) -> None:
    """Persiste emails do lote em exported_emails (dedup global entre corridas)."""
    conn.execute(f"""
        INSERT OR IGNORE INTO exported_emails (email)
        SELECT email FROM emails_deduplicated
        ORDER BY email
        LIMIT {int(limit)} OFFSET {int(offset)};
    """)
    conn.commit()


def clear_raw_after_dedup(conn: duckdb.DuckDBPyConnection, state: Dict[str, Any]):
    """Liberta staging raw após dedup bem-sucedida (disco como fonte de verdade)."""
    logger.info(f"{E['clean']} TRUNCATE emails_raw (pós-dedup)")
    conn.execute("DELETE FROM emails_raw;")
    conn.commit()


# =====================================================================
# ETAPA 3 — EXPORTAÇÃO EM LOTES (30M)
# =====================================================================
def batch_file_extension() -> str:
    return "csv" if EXPORT_FORMAT == "csv" else "parquet"


def export_single_batch_file(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    offset: int,
    limit: int,
) -> Tuple[Optional[Path], int]:
    ext = batch_file_extension()
    out = EXPORT_DIR / f"{batch_id}.{ext}"

    if EXPORT_FORMAT == "csv":
        conn.execute(f"""
            COPY (
                SELECT email, nome, origem, data
                FROM emails_deduplicated
                ORDER BY email
                LIMIT {int(limit)} OFFSET {int(offset)}
            ) TO '{out}' (HEADER, DELIMITER ',');
        """)
    else:
        conn.execute(f"""
            COPY (
                SELECT email, nome, origem, data
                FROM emails_deduplicated
                ORDER BY email
                LIMIT {int(limit)} OFFSET {int(offset)}
            ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
    conn.commit()

    if not out.exists() or out.stat().st_size < 10:
        return None, 0

    row_count = conn.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT email FROM emails_deduplicated
            ORDER BY email
            LIMIT {int(limit)} OFFSET {int(offset)}
        ) t;
    """).fetchone()[0]
    return out, int(row_count)


def run_ledger_export_pipeline(
    conn: duckdb.DuckDBPyConnection,
    api: HfApi,
    token: str,
    emails_repo: str,
    state: Dict[str, Any],
    total_rows: int,
    label: str,
) -> Tuple[bool, int, bool]:
    global _cycle_exported_batch

    batch_size = EXPORT_BATCH_SIZE
    plan = build_export_active_plan(state, total_rows, batch_size)
    batches = plan.get("batches", [])
    uploaded_this_run = 0

    for batch in batches:
        if stop_event.is_set():
            logger.warning(f"{E['warn']} [{label}] Export interrompido (stop_event)")
            return False, uploaded_this_run, False

        batch_id = batch["batch_id"]
        offset = int(batch["offset"])
        limit = int(batch["limit"])
        status = batch.get("status", "pending")

        if status == "uploaded" or ledger_is_uploaded(state, batch_id):
            logger.info(
                f"{E['skip']} [{label}] batch_id={batch_id} já uploaded — IGNORAR"
            )
            batch["status"] = "uploaded"
            state[EXPORT_ACTIVE_PLAN_KEY] = plan
            persist_export_ledger(state)
            continue

        logger.info(
            f"{E['export']} [{label}] ETAPA 3: {batch_id} "
            f"(offset={offset:,}, limit={limit:,})"
        )

        ledger_upsert_entry(state, {
            "batch_id": batch_id,
            "row_count": 0,
            "checksum": "",
            "status": "pending",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        persist_export_ledger(state)

        out_path, row_count = export_single_batch_file(conn, batch_id, offset, limit)
        if out_path is None or row_count == 0:
            logger.error(f"{E['error']} [{label}] Batch vazio para {batch_id}")
            ledger_upsert_entry(state, {
                "batch_id": batch_id,
                "row_count": 0,
                "checksum": "",
                "status": "failed",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            persist_export_ledger(state)
            batch["status"] = "failed"
            state[EXPORT_ACTIVE_PLAN_KEY] = plan
            persist_export_ledger(state)
            return False, uploaded_this_run, False

        checksum = compute_file_sha256(out_path)
        ledger_upsert_entry(state, {
            "batch_id": batch_id,
            "row_count": row_count,
            "checksum": checksum,
            "status": "uploading",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        batch["status"] = "uploading"
        state[EXPORT_ACTIVE_PLAN_KEY] = plan
        persist_export_ledger(state)

        repo_path = out_path.name
        upload_ok = _hf_upload_file(api, token, emails_repo, out_path, repo_path)
        if not upload_ok:
            logger.error(
                f"{E['error']} [{label}] Upload falhou para {batch_id} — "
                f"NÃO marcar uploaded"
            )
            ledger_upsert_entry(state, {
                "batch_id": batch_id,
                "row_count": row_count,
                "checksum": checksum,
                "status": "failed",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            batch["status"] = "failed"
            state[EXPORT_ACTIVE_PLAN_KEY] = plan
            persist_export_ledger(state)
            return False, uploaded_this_run, False

        register_exported_emails_from_batch(conn, batch_id, offset, limit)

        ts_uploaded = datetime.now(timezone.utc).isoformat()
        ledger_upsert_entry(state, {
            "batch_id": batch_id,
            "row_count": row_count,
            "checksum": checksum,
            "status": "uploaded",
            "timestamp": ts_uploaded,
        })
        batch["status"] = "uploaded"
        state[EXPORT_ACTIVE_PLAN_KEY] = plan
        if not persist_export_ledger(state):
            logger.error(
                f"{E['error']} [{label}] Upload OK mas ledger não persistido — abortar"
            )
            return False, uploaded_this_run, False

        try:
            out_path.unlink()
        except Exception:
            pass

        uploaded_this_run += 1
        _cycle_exported_batch = True
        logger.info(
            f"{E['ok']} [{label}] ETAPA 4: {batch_id} no HF ({row_count:,} linhas)"
        )

    all_ok = active_plan_all_uploaded(plan)
    return all_ok, uploaded_this_run, all_ok


def drop_deduplicated_after_confirmed_upload(conn: duckdb.DuckDBPyConnection):
    logger.info(f"{E['clean']} Limpar emails_deduplicated após export completo")
    conn.execute("DELETE FROM emails_deduplicated;")
    conn.commit()
    try:
        if DEDUP_PARQUET_PATH.exists():
            DEDUP_PARQUET_PATH.unlink()
    except Exception:
        pass


def process_pending_export_pipeline(
    conn: duckdb.DuckDBPyConnection,
    api: HfApi,
    token: str,
    emails_repo: str,
    checkpoint_repo: str,
    state: Dict[str, Any],
    label: str = "ciclo",
) -> bool:
    global _cycle_exported_batch, _cycle_ledger_complete

    load_raw_chunks_into_duckdb(conn, state, api, token, checkpoint_repo)

    raw_n = count_table(conn, "emails_raw")
    dedup_n = count_table(conn, "emails_deduplicated")
    exported_registry_n = count_table(conn, "exported_emails")

    logger.info(
        f"{E['db']} [{label}] raw={raw_n:,} dedup_pending={dedup_n:,} "
        f"exported_registry={exported_registry_n:,}"
    )

    plan = state.get(EXPORT_ACTIVE_PLAN_KEY)
    if raw_n == 0 and dedup_n == 0:
        if isinstance(plan, dict) and not active_plan_all_uploaded(plan):
            logger.error(
                f"{E['error']} [{label}] export_active_plan incompleto sem dados — retomar depois"
            )
            _cycle_had_pending_at_check = True
        else:
            logger.info(f"{E['info']} [{label}] NO NEW DATA TO EXPORT")
        return False

    if raw_n > 0:
        dedup_n = run_global_deduplication(conn, state)
        clear_raw_after_dedup(conn, state)

    if dedup_n == 0:
        logger.info(f"{E['info']} [{label}] Nenhum email novo após dedup global")
        drop_deduplicated_after_confirmed_upload(conn)
        clear_export_active_plan(state)
        return False

    all_uploaded, n_up, ledger_complete = run_ledger_export_pipeline(
        conn, api, token, emails_repo, state, dedup_n, label
    )

    if not ledger_complete:
        logger.error(
            f"{E['error']} [{label}] Export ledger INCOMPLETO — manter dados para retomada"
        )
        _cycle_had_pending_at_check = True
        upload_checkpoint_to_hf(api, token, checkpoint_repo)
        return False

    drop_deduplicated_after_confirmed_upload(conn)
    clear_export_active_plan(state)

    state["last_export"] = datetime.now(timezone.utc).isoformat()
    state["last_export_rows"] = dedup_n
    state["last_export_files"] = n_up
    state[STAGE_KEY] = "done"
    persist_export_ledger(state)
    save_processed_chunks(state)
    save_torrent_state(state)
    upload_checkpoint_to_hf(api, token, checkpoint_repo)

    _cycle_ledger_complete = True
    _cycle_exported_batch = True
    logger.info(
        f"{E['ok']} [{label}] Exactly-once: {n_up} batch(es) uploaded"
    )
    return True


def validate_cycle_export_guarantee(
    conn: duckdb.DuckDBPyConnection,
    state: Dict[str, Any],
) -> int:
    global _cycle_exported_batch, _cycle_ledger_complete

    pending_raw = count_table(conn, "emails_raw")
    pending_dedup = count_table(conn, "emails_deduplicated")
    plan = state.get(EXPORT_ACTIVE_PLAN_KEY)

    if pending_raw > 0 or pending_dedup > 0:
        logger.error(
            f"{E['error']} ERRO LÓGICO: dados pendentes raw={pending_raw:,} "
            f"dedup={pending_dedup:,} após ciclo"
        )
        return 3

    if isinstance(plan, dict) and not active_plan_all_uploaded(plan):
        logger.error(f"{E['error']} ERRO LÓGICO: export_active_plan incompleto")
        return 3

    if _cycle_exported_batch or _cycle_ledger_complete:
        logger.info(f"{E['ok']} GARANTIA CICLO: batches no export_ledger")
        return 0

    logger.info(f"{E['info']} NO NEW DATA TO EXPORT")
    return 0


# =====================================================================
# HF CHECKPOINT
# =====================================================================
def _hf_download_single(
    api: HfApi,
    token: str,
    repo_id: str,
    filename: str,
    local_dir: Path,
    repo_type: str = "dataset",
) -> Optional[Path]:
    try:
        local_file = api.hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            local_dir=str(local_dir),
            local_dir_use_symlinks=False,
            token=token,
            repo_type=repo_type,
        )
        local_path = local_dir / filename
        downloaded = Path(local_file)
        if downloaded != local_path and downloaded.exists():
            shutil.copy2(downloaded, local_path)
        if not verify_file_integrity(local_path):
            return None
        return local_path
    except Exception:
        return None


def load_checkpoint_from_hf(api: HfApi, token: str, checkpoint_repo: str) -> Dict[str, bool]:
    logger.info(f"{E['download']} A recuperar checkpoint do HF ({checkpoint_repo})...")
    files = [
        ("state.json", STATE_PATH),
        ("emails.duckdb", DB_PATH),
        ("processed_chunks.json", PROCESSED_CHUNKS_PATH),
        ("torrent_state.json", TORRENT_STATE_PATH),
    ]
    results: Dict[str, bool] = {}
    any_ok = False
    for filename, local_path in files:
        result = _hf_download_single(api, token, checkpoint_repo, filename, SAVE_PATH)
        if result:
            results[filename] = True
            any_ok = True
            logger.info(f"{E['ok']} Descarregado: {filename} ({human(result.stat().st_size)})")
        else:
            results[filename] = False
            logger.info(f"{E['info']} {filename} não encontrado no HF")
    if any_ok:
        logger.info("📥 Checkpoint recuperado (export_ledger + exported_emails no DuckDB)")
    else:
        logger.info(f"{E['info']} Nenhum checkpoint remoto — início do zero")
    return results


def _hf_upload_file(
    api: HfApi,
    token: str,
    repo_id: str,
    local_path: Path,
    repo_path: str,
    max_retries: int = 3,
) -> bool:
    if not local_path.exists():
        logger.warning(f"{E['warn']} Ficheiro não encontrado: {local_path}")
        return False
    size = local_path.stat().st_size
    if size == 0:
        logger.warning(f"{E['warn']} Ficheiro vazio: {local_path.name} — skip upload")
        return False
    for attempt in range(1, max_retries + 1):
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
            logger.info(
                f"{E['upload']} Upload OK: {repo_path} ({human(size)}) → {repo_id}"
            )
            return True
        except Exception as e:
            logger.warning(
                f"{E['warn']} Upload {attempt}/{max_retries} falhou: "
                f"{repo_path} — {str(e)[:120]}"
            )
            if attempt < max_retries:
                time.sleep(5 * attempt)
    logger.error(f"{E['error']} Upload falhou: {repo_path}")
    return False


def upload_checkpoint_to_hf(api: HfApi, token: str, checkpoint_repo: str) -> bool:
    logger.info(f"{E['checkpoint']} Checkpoint → HF ({checkpoint_repo})")
    files = [
        (STATE_PATH, "state.json"),
        (DB_PATH, "emails.duckdb"),
        (PROCESSED_CHUNKS_PATH, "processed_chunks.json"),
        (TORRENT_STATE_PATH, "torrent_state.json"),
    ]
    all_ok = True
    for local_path, repo_path in files:
        if not local_path.exists():
            continue
        if local_path == DB_PATH and local_path.stat().st_size == 0:
            continue
        if not _hf_upload_file(api, token, checkpoint_repo, local_path, repo_path):
            all_ok = False
    return all_ok


def save_full_checkpoint(api: HfApi, token: str, checkpoint_repo: str):
    try:
        upload_checkpoint_to_hf(api, token, checkpoint_repo)
    except Exception as e:
        logger.exception(f"{E['error']} save_full_checkpoint: {e}")


def _periodic_checkpoint_worker():
    global _g_periodic_timer
    if stop_event.is_set():
        return
    logger.info(f"{E['loop']} Checkpoint periódico ({CHECKPOINT_INTERVAL_MIN} min)...")
    if _g_api and _g_token and _g_checkpoint_repo:
        try:
            save_full_checkpoint(_g_api, _g_token, _g_checkpoint_repo)
        except Exception as e:
            logger.error(f"{E['error']} Checkpoint periódico: {e}")
    if not stop_event.is_set():
        _g_periodic_timer = threading.Timer(
            CHECKPOINT_INTERVAL_MIN * 60, _periodic_checkpoint_worker
        )
        _g_periodic_timer.daemon = True
        _g_periodic_timer.start()


def start_periodic_checkpoint(api: HfApi, token: str, checkpoint_repo: str):
    global _g_api, _g_token, _g_checkpoint_repo, _g_periodic_timer
    _g_api = api
    _g_token = token
    _g_checkpoint_repo = checkpoint_repo
    _g_periodic_timer = threading.Timer(
        CHECKPOINT_INTERVAL_MIN * 60, _periodic_checkpoint_worker
    )
    _g_periodic_timer.daemon = True
    _g_periodic_timer.start()


def stop_periodic_checkpoint():
    global _g_periodic_timer
    if _g_periodic_timer is not None:
        _g_periodic_timer.cancel()
        _g_periodic_timer = None


# =====================================================================
# ETAPA 1 — EXTRAÇÃO BRUTA (sem dedup)
# =====================================================================
def process_chunk_worker(chunk_data: bytes, chunk_idx: int, origin: str) -> List[Tuple]:
    results = []
    data_iso = datetime.now(timezone.utc).isoformat()
    for match in EMAIL_REGEX.finditer(chunk_data):
        try:
            email_b = match.group()
            email = email_b.decode("utf8", "ignore").strip().lower()
            if not email or "@" not in email or is_disposable_email(email):
                continue
            local_part = email.split("@")[0]
            local_part = re.sub(r"\d+", "", local_part)
            local_part = re.sub(r"[_.\\-]+", " ", local_part).strip()
            nome = " ".join(p.capitalize() for p in local_part.split()) if local_part else ""
            results.append((email, nome, origin, data_iso))
        except Exception:
            continue
    return results


def process_tar_with_mmap(
    tar_path: Path,
    origin: str,
    api: HfApi,
    token: str,
    checkpoint_repo: str,
    state: Dict[str, Any],
) -> List[Path]:
    cpu_count = min(2, os.cpu_count() or 2)
    chunk_files: List[Path] = []
    logger.info(f"{E['extract']} ETAPA 1: {tar_path.name} (sem dedup)")
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar:
                if stop_event.is_set():
                    break
                if not member.isfile() or not (
                    member.name.endswith(".txt") or member.name.endswith(".csv")
                ):
                    continue
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue
                gc.collect()
                writer: Optional[pq.ParquetWriter] = None
                current_chunk_file = None
                row_count = 0
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    MAX_INFLIGHT = 4
                    inflight: set = set()
                    chunk_idx = 0

                    def drain_futures(inf):
                        for f in list(inf):
                            if f.done():
                                inf.discard(f)
                                try:
                                    r = f.result()
                                    if r:
                                        yield_or_write(r)
                                except Exception:
                                    pass

                    def yield_or_write(records):
                        nonlocal writer, current_chunk_file, row_count
                        if not records:
                            return
                        safe = [r for r in records if isinstance(r, tuple) and len(r) == 4]
                        if not safe:
                            return
                        if writer is None:
                            current_chunk_file = (
                                RAW_CHUNKS_DIR
                                / f"raw_chunk_{len(chunk_files):06d}_{ts}.parquet"
                            )
                            schema = pa.schema([
                                ("email", pa.string()),
                                ("nome", pa.string()),
                                ("origem", pa.string()),
                                ("data", pa.string()),
                            ])
                            writer = pq.ParquetWriter(
                                str(current_chunk_file), schema, compression="snappy"
                            )
                        table = pa.Table.from_arrays(
                            [[r[0] for r in safe], [r[1] for r in safe],
                             [r[2] for r in safe], [r[3] for r in safe]],
                            names=["email", "nome", "origem", "data"],
                        )
                        writer.write_table(table)
                        row_count += len(safe)
                        del table
                        gc.collect()

                    while True:
                        chunk_data = fobj.read(CHUNK_SIZE)
                        if not chunk_data:
                            break
                        if stop_event.is_set():
                            break
                        while len(inflight) >= MAX_INFLIGHT:
                            drain_futures(inflight)
                            time.sleep(0.05)
                        inflight.add(
                            executor.submit(
                                process_chunk_worker, chunk_data, chunk_idx, member.name
                            )
                        )
                        chunk_idx += 1
                        drain_futures(inflight)

                    for f in list(inflight):
                        try:
                            r = f.result()
                            if r:
                                yield_or_write(r)
                        except Exception:
                            pass

                if writer is not None:
                    writer.close()
                    chunk_files.append(current_chunk_file)
                    logger.info(
                        f"{E['ok']} raw_chunk: {current_chunk_file.name} ({row_count:,})"
                    )
                    state[STAGE_KEY] = "raw"
                    save_processed_chunks(state)
                    save_torrent_state(state)
                    save_full_checkpoint(api, token, checkpoint_repo)

        try:
            tar_path.unlink()
        except Exception:
            pass
    except Exception as e:
        logger.exception(f"{E['error']} process_tar: {e}")
    return chunk_files


def create_libtorrent_session() -> lt.session:
    session = lt.session()
    try:
        settings = lt.settings_pack()
        cpu_count = os.cpu_count() or 4
        settings.set_int("connections_limit", min(cpu_count * 100, 800))
        settings.set_int("connections_limit_global", min(cpu_count * 500, 4000))
        settings.set_int("active_limit", min(cpu_count * 50, 200))
        settings.set_int("request_queue_size", 1024)
        settings.set_int("cache_size", 4096)
        settings.set_bool("enable_dht", True)
        settings.set_bool("enable_lsd", True)
        settings.set_bool("enable_pex", True)
        settings.set_int("upload_rate_limit", 0)
        settings.set_int("download_rate_limit", 0)
        session.apply_settings(settings)
    except AttributeError:
        pass
    return session


def find_target_indices(
    torrent_info: lt.torrent_info, targets: List[str]
) -> Tuple[List[int], List[str]]:
    n = torrent_info.num_files()
    files_storage = torrent_info.files()
    file_catalog = {}
    for i in range(n):
        raw_path = files_storage.at(i).path
        norm_path = normalize_string_robust(raw_path)
        basename = norm_path.split("/")[-1]
        file_catalog[i] = {
            "raw": raw_path,
            "norm": norm_path,
            "basename": basename,
            "size": files_storage.at(i).size,
        }
    found_indices = set()
    missing_targets = []
    for t in targets:
        target_norm = normalize_string_robust(t)
        target_basename = target_norm.split("/")[-1]
        matched = False
        for i, fdata in file_catalog.items():
            if fdata["norm"] == target_norm:
                found_indices.add(i)
                matched = True
                break
        if matched:
            continue
        for i, fdata in file_catalog.items():
            if fdata["basename"] == target_basename:
                found_indices.add(i)
                matched = True
                break
        if matched:
            continue
        for i, fdata in file_catalog.items():
            if target_basename in fdata["norm"] or fdata["basename"] in target_norm:
                found_indices.add(i)
                matched = True
                break
        if not matched:
            missing_targets.append(t)
    return sorted(list(found_indices)), missing_targets


def local_path_for_index_robust(
    save_path: Path, torrent_info: lt.torrent_info, index: int
) -> Optional[Path]:
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    basename = Path(file_path).name
    candidate1 = save_path / torrent_name / file_path
    if candidate1.exists() and candidate1.is_file():
        return candidate1
    candidate2 = save_path / file_path
    if candidate2.exists() and candidate2.is_file():
        return candidate2
    torrent_dir = save_path / torrent_name
    if torrent_dir.exists():
        for found in torrent_dir.rglob(basename):
            if found.is_file():
                return found
    for found in save_path.rglob(basename):
        if found.is_file():
            return found
    return None


def wait_for_file_complete(
    handle: lt.torrent_handle, file_index: int, expected_size: int
) -> bool:
    last_log = 0
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        fprog = handle.file_progress()
        got = fprog[file_index] if file_index < len(fprog) else 0
        pct = (got / expected_size * 100) if expected_size else 0.0
        now = time.time()
        if now - last_log >= 5:
            logger.info(f"{E['download']} File[{file_index}]: {pct:.1f}%")
            last_log = now
        if expected_size and got >= expected_size:
            return True
        time.sleep(POLL_INTERVAL)


def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    if not token:
        raise RuntimeError("HF_TOKEN não definido")
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user")
    if not user:
        raise RuntimeError("Utilizador HF inválido")
    emails_repo = f"{user}/{HF_REPO_EMAILS}"
    checkpoint_repo = f"{user}/{HF_REPO_CHECKPOINT}"
    for repo_id in [emails_repo, checkpoint_repo]:
        try:
            api.create_repo(
                repo_id=repo_id, token=token, repo_type="dataset", private=True
            )
        except Exception as e:
            if "already exists" not in str(e).lower() and "409" not in str(e):
                logger.warning(f"{E['warn']} create_repo {repo_id}: {e}")
    return api, emails_repo, checkpoint_repo


def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    logger.info(f"{E['download']} Download: {len(magnets)} torrents")
    completed = {}

    def download_single(item):
        name = item["name"]
        magnet = item["magnet"]
        targets = item.get("targets", [])
        try:
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)
            while not handle.has_metadata() and not stop_event.is_set():
                time.sleep(POLL_INTERVAL)
            info = handle.get_torrent_info()
            found, miss = find_target_indices(info, targets)
            if miss:
                raise RuntimeError(f"Targets em falta: {miss}")
            for i in range(info.num_files()):
                handle.file_priority(i, 7 if i in found else 0)
            return (name, (handle, info, found))
        except Exception:
            logger.exception(f"{E['error']} Torrent {name}")
            return None

    with ThreadPoolExecutor(max_workers=len(magnets)) as ex:
        for fut in as_completed([ex.submit(download_single, m) for m in magnets]):
            r = fut.result()
            if r:
                completed[r[0]] = r[1]
    return completed


def phase2_wait_downloads(
    completed_torrents: Dict,
    state: Dict,
    api: HfApi,
    token: str,
    checkpoint_repo: str,
) -> List[Tuple]:
    logger.info(f"{E['download']} Aguardar ficheiros...")
    all_files = []
    processed_key = state.get("downloaded_files", {})
    processed_tars = state.get("processed_tars", [])

    for tname, (handle, info, indices) in completed_torrents.items():
        if stop_event.is_set():
            break
        for idx in indices:
            if stop_event.is_set():
                break
            file_key = f"{tname}_{idx}"
            expected_size = info.files().at(idx).size
            if file_key in processed_key:
                local_path = local_path_for_index_robust(SAVE_PATH, info, idx)
                if local_path and str(local_path) in processed_tars:
                    continue
                if local_path and local_path.exists():
                    all_files.append((tname, local_path, info))
                    continue
                del processed_key[file_key]
            wait_for_file_complete(handle, idx, expected_size)
            local_path = local_path_for_index_robust(SAVE_PATH, info, idx)
            if local_path:
                all_files.append((tname, local_path, info))
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
                save_torrent_state(state)
                save_full_checkpoint(api, token, checkpoint_repo)
    return all_files


def phase3_process_tars(
    tars: List[Tuple],
    state: Dict,
    api: HfApi,
    token: str,
    checkpoint_repo: str,
) -> List[Path]:
    logger.info(f"{E['extract']} Processar {len(tars)} tars")
    all_chunks = []
    processed_tars = state.get("processed_tars", [])
    for tname, tar_path, info in tars:
        if stop_event.is_set():
            break
        if str(tar_path) in processed_tars:
            continue
        chunks = process_tar_with_mmap(
            tar_path, tname, api, token, checkpoint_repo, state
        )
        all_chunks.extend(chunks)
        processed_tars.append(str(tar_path))
        state["processed_tars"] = processed_tars
        save_state(state)
        save_processed_chunks(state)
        save_full_checkpoint(api, token, checkpoint_repo)
    return all_chunks


def main():
    global _cycle_exported_batch, _cycle_had_pending_at_check, _cycle_ledger_complete

    logger.info(f"{E['start']} Minerador Production v6 (DuckDB disk dedup, sem Bloom)")
    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN não definido")
        sys.exit(2)

    free = disk_usage()["free"]
    if free < MIN_FREE_BYTES:
        logger.warning(f"{E['space']} Pouco espaço livre: {human(free)}")

    exit_code = 0
    conn = None

    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
        logger.info(f"{E['info']} emails_repo = {emails_repo}")
        logger.info(
            f"{E['info']} EXPORT_BATCH_SIZE={EXPORT_BATCH_SIZE:,} "
            f"FORMAT={batch_file_extension()}"
        )

        load_checkpoint_from_hf(api, HF_TOKEN, checkpoint_repo)

        state = merge_checkpoint_into_state(load_state())
        ensure_export_ledger(state)
        conn = init_duckdb(DB_PATH)

        start_periodic_checkpoint(api, HF_TOKEN, checkpoint_repo)

        process_pending_export_pipeline(
            conn, api, HF_TOKEN, emails_repo, checkpoint_repo, state, label="arranque"
        )

        session = create_libtorrent_session()
        t0 = time.time()

        completed = phase1_download_torrents(session, MAGNETS)
        if completed and not stop_event.is_set():
            tars = phase2_wait_downloads(
                completed, state, api, HF_TOKEN, checkpoint_repo
            )
            if tars and not stop_event.is_set():
                phase3_process_tars(tars, state, api, HF_TOKEN, checkpoint_repo)

        if not stop_event.is_set():
            process_pending_export_pipeline(
                conn, api, HF_TOKEN, emails_repo, checkpoint_repo, state, label="final"
            )

        exit_code = validate_cycle_export_guarantee(conn, state)

        logger.info(f"{E['stats']} Tempo: {(time.time() - t0) / 60:.2f} min")
        if exit_code == 0:
            logger.info(f"{E['ok']} Ciclo concluído")
        else:
            logger.error(f"{E['error']} Ciclo terminou com ERRO LÓGICO")

    except KeyboardInterrupt:
        logger.warning(f"{E['signal']} Interrupção")
        exit_code = 130
    except Exception as e:
        logger.exception(f"{E['error']} Fatal: {e}")
        exit_code = 1
    finally:
        stop_periodic_checkpoint()
        try:
            state_current = merge_checkpoint_into_state(load_state())
            ensure_export_ledger(state_current)
            save_processed_chunks(state_current)
            save_torrent_state(state_current)
            if HF_TOKEN:
                api_f = HfApi()
                _, _, cp = hf_setup_datasets(HF_TOKEN)
                save_full_checkpoint(api_f, HF_TOKEN, cp)
        except Exception as e:
            logger.error(f"{E['error']} Checkpoint final: {e}")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
