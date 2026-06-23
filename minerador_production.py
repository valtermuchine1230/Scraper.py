#!/usr/bin/env python3
"""
minerador_production.py — Escalável a bilhões de emails com tolerância a falhas.

INSTALAÇÃO DE DEPENDÊNCIAS:
    pip install -r requirements.txt

ARQUITETURA:
  FASE 1: Download 5 torrents simultâneos
  FASE 2: Checkpoint torrents no HF
  FASE 3: Processar com mmap + regex + ProcessPoolExecutor + STREAMING
  FASE 4: Gerar raw_chunk_*.parquet (streaming incremental)
  FASE 5: Filtrar domínios descartáveis
  FASE 6: DuckDB com SELECT DISTINCT (deduplicação global)
  FASE 7: Gerar Trader_Emails_*.parquet (30M linhas/arquivo)
  FASE 8: Upload HF + atualizar checkpoint para próxima run

PERSISTÊNCIA: Tudo no Hugging Face → recuperação completa após timeout

OTIMIZAÇÕES DE MEMÓRIA (VERSÃO OTIMIZADA):
  - ALTERAÇÃO 1: PRAGMA memory_limit='8GB' (de 12GB)
  - ALTERAÇÃO 2: CHUNK_SIZE = 256 MB (de 1 GB) ✓
  - ALTERAÇÃO 3: MAX_WORKERS = min(6, cpu_count)
  - ALTERAÇÃO 4: MAX_INFLIGHT = 8 (conservador)
  - ALTERAÇÃO 5: gc.collect() após cada escrita (ParquetWriter)
  - ALTERAÇÃO 6: Logs detalhados de RAM (% | usada GB | livre GB)

BLOOM FILTER (DEDUPLICAÇÃO EM STREAMING):
  - Biblioteca: pybloom-live
  - Capacidade: 1.500.000.000 entradas
  - Taxa de erro: 0.1%
  - Persistência: Hugging Face Hub (valter/projet)
  - Guardado a cada checkpoint
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
import psutil
import gc
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Set
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
    from pybloom_live import BloomFilter
except ImportError:
    print("❌ ERROR: pybloom_live not installed")
    print("   Executar: pip install 'pybloom-live>=4.0.1'")
    print("")
    print("   NOTA: O pacote é instalado via 'pybloom-live' mas importado como 'pybloom_live'")
    sys.exit(1)

try:
    from rich.logging import RichHandler
    from rich.console import Console
except ImportError:
    print("❌ ERROR: rich not installed")
    print("   Executar: pip install 'rich>=13.0.0'")
    sys.exit(1)

# ===== CONFIGURATION =====
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
RAW_CHUNKS_DIR = SAVE_PATH / "raw_chunks"
DB_PATH = SAVE_PATH / "emails.duckdb"
STATE_PATH = SAVE_PATH / "state.json"
LOG_PATH = SAVE_PATH / "minerador.log"

for d in [EXPORT_DIR, TEMP_DIR, RAW_CHUNKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO_EMAILS = os.environ.get("HF_REPO_EMAILS", "Trader_Emails")
HF_REPO_CHECKPOINT = os.environ.get("HF_REPO_CHECKPOINT", "minerador_checkpoints")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "3"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BATCH_INSERT_DDB = int(os.environ.get("BATCH_INSERT_DDB", "500000"))
ROWS_PER_FINAL_FILE = int(os.environ.get("ROWS_PER_FINAL_FILE", "30000000"))

# ALTERAÇÃO 2: CHUNK_SIZE = 256 MB (de 1 GB)
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(256 * 1024 * 1024)))

MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(512 * 1024 * 1024)))
ROWS_PER_PARQUET_FILE = int(os.environ.get("ROWS_PER_PARQUET_FILE", "5000000"))

# 🌸 BLOOM FILTER — Configuração
BLOOM_FILTER_PATH = SAVE_PATH / "bloom_filter.bin"
HF_REPO_BLOOM = "valter/projet"          # Repositório HF para guardar o Bloom Filter
BLOOM_CAPACITY = 1_500_000_000           # 1.5 bilhões de entradas (com margem)
BLOOM_ERROR_RATE = 0.001                 # Taxa de erro máxima: 0.1%

# 🌸 BLOOM FILTER — Estado global (partilhado no processo principal)
bloom_filter: BloomFilter | None = None
bloom_lock = Lock()                      # Thread-safe para acessos concorrentes

# MAGNET LINKS
MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        "targets": [
            "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
            "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz",
        ],
    },
    {
        "name": "Collection #1",
        "magnet": "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E&dn=Collection%201&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce",
        "targets": [
            "Collection #1/Collection #1_BTC combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_BTC combos.tar.gz",
        ],
    },
]

# DISPOSABLE DOMAINS (5000+ simplificado)
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

# ===== LOGGING =====
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
file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

E = {
    "start": "🚀", "download": "📥", "extract": "📦", "stats": "📊",
    "space": "📉", "email": "📧", "upload": "📤",
    "clean": "🧹", "warn": "⚠️", "error": "❌", "ok": "✅",
    "info": "🗿", "cpu": "⚙️", "db": "🗄️", "bloom": "🌸",
}

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

stop_event = Event()
state_lock = Lock()

def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum}; graceful shutdown")
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ===== UTILITIES =====

def normalize_string_robust(s: str) -> str:
    """
    Normaliza strings para resolver quebras de linha inesperadas,
    espaços duplicados ou invisíveis e diferenças de path.
    """
    if not isinstance(s, str):
        s = str(s)
    s = re.sub(r'\s+', ' ', s)
    s = s.replace('\\', '/')
    return s.strip().lower()

def human(n: int) -> str:
    """Convert bytes to human readable format."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path: Path = SAVE_PATH) -> Dict[str, int]:
    """Get disk usage info."""
    du = shutil.disk_usage(str(path))
    return {"total": du.total, "used": du.used, "free": du.free}

def save_state(state: Dict[str, Any]):
    """Save execution state (thread-safe)."""
    with state_lock:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f, indent=2, default=str)

def load_state() -> Dict[str, Any]:
    """Load execution state."""
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def is_disposable_email(email: str) -> bool:
    """Check if email is from disposable domain."""
    try:
        domain = email.split("@")[-1].lower()
        return domain in DISPOSABLE_DOMAINS
    except Exception:
        return False

# ===== 🌸 BLOOM FILTER — Funções de persistência =====

def load_bloom_filter(api: HfApi, token: str) -> BloomFilter:
    """
    🌸 BLOOM FILTER — Carrega do Hugging Face Hub.
    Se o ficheiro não existir no HF, cria um novo Bloom Filter.
    """
    global bloom_filter

    # Garantir que o repositório existe
    try:
        api.create_repo(
            repo_id=HF_REPO_BLOOM,
            token=token,
            repo_type="dataset",
            private=True,
        )
        logger.info(f"{E['bloom']} Repositório HF criado: {HF_REPO_BLOOM}")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"{E['bloom']} Repositório HF já existe: {HF_REPO_BLOOM}")
        else:
            logger.warning(f"{E['warn']} create_repo: {str(e)[:120]}")

    # Tentar descarregar o Bloom Filter existente
    try:
        logger.info(f"{E['bloom']} A descarregar Bloom Filter de {HF_REPO_BLOOM}...")
        local_file = api.hf_hub_download(
            repo_id=HF_REPO_BLOOM,
            filename="bloom_filter.bin",
            local_dir=str(SAVE_PATH),
            token=token,
            repo_type="dataset",
        )
        with open(local_file, "rb") as f:
            bloom_filter = BloomFilter.fromfile(f)
        logger.info(
            f"{E['bloom']} Bloom Filter carregado do HF "
            f"(count≈{bloom_filter.count:,} entradas)"
        )
    except Exception:
        # Ficheiro não existe no HF → criar novo
        logger.info(
            f"{E['bloom']} Bloom Filter não encontrado no HF. "
            f"A criar novo (capacidade={BLOOM_CAPACITY:,}, erro={BLOOM_ERROR_RATE})"
        )
        bloom_filter = BloomFilter(
            capacity=BLOOM_CAPACITY,
            error_rate=BLOOM_ERROR_RATE,
        )

    return bloom_filter


def save_bloom_filter(api: HfApi, token: str):
    """
    🌸 BLOOM FILTER — Guarda localmente e faz upload para o Hugging Face Hub.
    Chamado a cada checkpoint para nunca perder progresso.
    """
    global bloom_filter
    if bloom_filter is None:
        logger.warning(f"{E['warn']} Bloom Filter é None, nada a guardar")
        return

    try:
        logger.info(f"{E['bloom']} A guardar Bloom Filter ({bloom_filter.count:,} entradas)...")

        # 1. Guardar ficheiro binário localmente
        with open(BLOOM_FILTER_PATH, "wb") as f:
            bloom_filter.tofile(f)

        bloom_size_mb = BLOOM_FILTER_PATH.stat().st_size / (1024 ** 2)
        logger.info(f"{E['bloom']} Bloom Filter local: {bloom_size_mb:.1f} MB")

        # 2. Upload para HF
        api.upload_file(
            path_or_fileobj=str(BLOOM_FILTER_PATH),
            path_in_repo="bloom_filter.bin",
            repo_id=HF_REPO_BLOOM,
            repo_type="dataset",
            token=token,
        )
        logger.info(f"{E['bloom']} Bloom Filter guardado no HF: {HF_REPO_BLOOM}/bloom_filter.bin")

    except Exception as e:
        logger.exception(f"{E['error']} Falha ao guardar Bloom Filter: {e}")


# ===== DUCKDB =====
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB with optimal settings."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    conn.execute("PRAGMA threads=8;")
    # ALTERAÇÃO 1: REDUZIR MEMORY_LIMIT DE 12GB PARA 8GB
    conn.execute("PRAGMA memory_limit='8GB';")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS emails_raw (
            email VARCHAR PRIMARY KEY,
            nome VARCHAR,
            origem VARCHAR,
            data VARCHAR
        );
    """)
    conn.commit()
    return conn

def batch_insert_duckdb(conn: duckdb.DuckDBPyConnection, records: List[Tuple]) -> int:
    """Insert batch into DuckDB (handles duplicates gracefully)."""
    if not records:
        return 0
    try:
        for email, nome, origem, data in records:
            try:
                conn.execute(
                    "INSERT INTO emails_raw VALUES (?, ?, ?, ?)",
                    [email, nome, origem, data],
                )
            except Exception:
                pass  # Duplicate, skip
        conn.commit()
        return len(records)
    except Exception as e:
        logger.exception(f"{E['error']} DuckDB insert failed")
        conn.rollback()
        return 0

# ===== LIBTORRENT =====
def create_libtorrent_session() -> lt.session:
    """Create optimized libtorrent session."""
    try:
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
            logger.info(f"{E['info']} Using fallback libtorrent configuration")
            pass

        logger.info(f"{E['cpu']} Libtorrent session created")
        return session
    except Exception as e:
        logger.exception(f"{E['error']} Failed to create libtorrent session")
        raise

def find_target_indices(torrent_info: lt.torrent_info, targets: List[str]) -> Tuple[List[int], List[str]]:
    """
    Find target file indices in torrent usando um sistema de matching em 3 níveis.
    """
    n = torrent_info.num_files()
    files_storage = torrent_info.files()

    file_catalog = {}
    for i in range(n):
        raw_path = files_storage.at(i).path
        norm_path = normalize_string_robust(raw_path)
        basename = norm_path.split('/')[-1]
        file_catalog[i] = {
            'raw': raw_path,
            'norm': norm_path,
            'basename': basename,
            'size': files_storage.at(i).size
        }

    found_indices = set()
    missing_targets = []

    for t in targets:
        target_norm = normalize_string_robust(t)
        target_basename = target_norm.split('/')[-1]

        matched = False

        # NÍVEL 1: Match Exato Normalizado
        for i, fdata in file_catalog.items():
            if fdata['norm'] == target_norm:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['ok']} Nível 1 (Match Exato): '{t}' -> '{fdata['raw']}'")
                break

        if matched: continue

        # NÍVEL 2: Match por Nome Final do Ficheiro (Basename)
        for i, fdata in file_catalog.items():
            if fdata['basename'] == target_basename:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['ok']} Nível 2 (Basename): '{t}' -> '{fdata['raw']}'")
                break

        if matched: continue

        # NÍVEL 3: Match Parcial (Substring Robusta)
        for i, fdata in file_catalog.items():
            if target_basename in fdata['norm'] or fdata['basename'] in target_norm:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['warn']} Nível 3 (Match Parcial): '{t}' -> '{fdata['raw']}'")
                break

        if not matched:
            missing_targets.append(t)
            logger.error(f"{E['error']} Impossível encontrar correspondência para o target: '{t}'")

    if missing_targets:
        logger.warning(f"{E['warn']} LISTA COMPLETA DE FICHEIROS DISPONÍVEIS NO TORRENT DE METADATA ({torrent_info.name()}):")
        for i, fdata in file_catalog.items():
            logger.warning(f"  -> Index [{i}]: Raw='{fdata['raw']}' | Normalizado='{fdata['norm']}' | Size={human(fdata['size'])}")

    return sorted(list(found_indices)), missing_targets

def local_path_for_index_robust(save_path: Path, torrent_info: lt.torrent_info, index: int) -> Path | None:
    """Localiza robustamente o arquivo baixado no disco."""
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    basename = Path(file_path).name

    candidate1 = save_path / torrent_name / file_path
    if candidate1.exists() and candidate1.is_file():
        logger.info(f"{E['ok']} [NÍVEL 1] Arquivo localizado: {candidate1}")
        return candidate1

    candidate2 = save_path / file_path
    if candidate2.exists() and candidate2.is_file():
        logger.info(f"{E['ok']} [NÍVEL 2] Arquivo localizado (sem duplicação): {candidate2}")
        return candidate2

    torrent_dir = save_path / torrent_name
    if torrent_dir.exists() and torrent_dir.is_dir():
        for found_file in torrent_dir.rglob(basename):
            if found_file.is_file():
                logger.info(f"{E['ok']} [NÍVEL 3] Arquivo localizado (busca recursiva): {found_file}")
                return found_file

    for found_file in save_path.rglob(basename):
        if found_file.is_file():
            logger.info(f"{E['ok']} [NÍVEL 4] Arquivo localizado (busca global): {found_file}")
            return found_file

    logger.error(f"{E['error']} ========== DIAGNÓSTICO COMPLETO DE ARQUIVO PERDIDO ==========")
    logger.error(f"{E['error']} Torrent: {torrent_name}")
    logger.error(f"{E['error']} File Index: {index}")
    logger.error(f"{E['error']} File Path (raw): {file_path}")
    logger.error(f"{E['error']} File Basename: {basename}")
    logger.error(f"{E['error']} Save Path: {save_path}")
    logger.error(f"{E['error']} ")
    logger.error(f"{E['error']} Caminhos testados (NÃO encontrados):")
    logger.error(f"{E['error']}   [1] {candidate1}")
    logger.error(f"{E['error']}   [2] {candidate2}")
    if torrent_dir.exists():
        logger.error(f"{E['error']}   [3] Busca recursiva em {torrent_dir}/")
    logger.error(f"{E['error']}   [4] Busca global em {save_path}/")
    logger.error(f"{E['error']} ")
    logger.error(f"{E['error']} Conteúdo do diretório Torrent ({torrent_dir}):")
    if torrent_dir.exists() and torrent_dir.is_dir():
        try:
            for item in list(torrent_dir.rglob("*"))[:50]:
                rel_path = item.relative_to(save_path)
                if item.is_file():
                    size = item.stat().st_size
                    logger.error(f"{E['error']}     FILE: {rel_path} ({human(size)})")
                else:
                    logger.error(f"{E['error']}     DIR:  {rel_path}/")
        except Exception as e:
            logger.error(f"{E['error']}     [Erro ao listar: {str(e)}]")
    else:
        logger.error(f"{E['error']}     [Diretório NÃO existe]")
    logger.error(f"{E['error']} ")
    logger.error(f"{E['error']} Conteúdo raiz de Save Path ({save_path}):")
    try:
        for item in list(save_path.iterdir())[:20]:
            if item.is_file():
                size = item.stat().st_size
                logger.error(f"{E['error']}     FILE: {item.name} ({human(size)})")
            else:
                logger.error(f"{E['error']}     DIR:  {item.name}/")
    except Exception as e:
        logger.error(f"{E['error']}     [Erro ao listar: {str(e)}]")
    logger.error(f"{E['error']} =========================================================")

    return None

def wait_for_file_complete(handle: lt.torrent_handle, file_index: int, expected_size: int) -> bool:
    """Wait for a file to finish downloading."""
    last_log = 0
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()

        fprog = handle.file_progress()
        got = fprog[file_index] if file_index < len(fprog) else 0
        pct = (got / expected_size * 100) if expected_size else 0.0

        now = time.time()
        if now - last_log >= 5:
            logger.info(f"{E['download']} File[{file_index}]: {got:,}/{expected_size:,} ({pct:.1f}%)")
            last_log = now

        if expected_size and got >= expected_size:
            logger.info(f"{E['ok']} File {file_index} complete")
            return True

        time.sleep(POLL_INTERVAL)

# ===== PROCESSING =====
def process_chunk_worker(chunk_data: bytes, chunk_idx: int, origin: str) -> List[Tuple]:
    """Worker process: extract emails from chunk using regex on bytes."""
    results = []
    data_iso = datetime.now(timezone.utc).isoformat()

    for match in EMAIL_REGEX.finditer(chunk_data):
        try:
            email_b = match.group()
            try:
                email = email_b.decode("utf8", "ignore").strip().lower()
            except Exception:
                email = email_b.decode("latin1", "ignore").strip().lower()

            if not email or "@" not in email or is_disposable_email(email):
                continue

            # Guess name from email
            local_part = email.split("@")[0]
            local_part = re.sub(r"\d+", "", local_part)
            local_part = re.sub(r"[_.\\-]+", " ", local_part).strip()
            nome = " ".join([p.capitalize() for p in local_part.split()]) if local_part else ""

            results.append((email, nome, origin, data_iso))
        except Exception as e:
            logger.exception(
                "❌ WORKER FAILURE DETALHADO\n"
                f"chunk_idx={chunk_idx}\n"
                f"member={origin}\n"
                f"error_type={type(e).__name__}\n"
                f"error_msg={str(e)}"
            )
            continue

    return results

def process_tar_with_mmap(tar_path: Path, origin: str) -> List[Path]:
    """
    Extract tar.gz with mmap + regex + ProcessPoolExecutor + STREAMING.
    Com otimizações de memória: CHUNK_SIZE 256MB, MAX_WORKERS 6, MAX_INFLIGHT 8.
    """
    # ALTERAÇÃO 3: LIMITAR WORKERS A MÁXIMO 6 (conservador, mantém paralelismo)
    cpu_count = min(6, os.cpu_count() or 4)
    chunk_files = []

    logger.info(f"{E['extract']} Processando: {tar_path.name}")

    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar:
                if stop_event.is_set():
                    break

                if not member.isfile() or not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue

                logger.info(f"{E['extract']} Member: {member.name}")
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue

                # ALTERAÇÃO 5: LIBERAR MEMÓRIA PERIODICAMENTE
                gc.collect()

                # STREAMING: Inicializa writer quando temos dados
                writer = None
                schema = None
                current_chunk_file = None
                row_count = 0
                chunk_batch_count = 0

                # 🌸 BLOOM FILTER — Contador de duplicatas para logging
                bloom_skipped = 0

                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    # ALTERAÇÃO 4: LIMITAR MAX_INFLIGHT A 8 (evita explosão de memória)
                    MAX_INFLIGHT = 8
                    inflight = set()
                    chunk_idx = 0

                    def check_memory():
                        """Monitorar memória com log detalhado."""
                        mem = psutil.virtual_memory()
                        # ALTERAÇÃO 6: LOG DETALHADO COM GB USADO/LIVRE
                        if mem.percent > 85:
                            logger.warning(
                                f"⚠️ RAM ALTA: "
                                f"{mem.percent}% | "
                                f"usada={mem.used/1024**3:.2f}GB | "
                                f"livre={mem.available/1024**3:.2f}GB"
                            )
                            time.sleep(1)

                    def drain_futures(inflight):
                        done = set()
                        for f in list(inflight):
                            if f.done():
                                done.add(f)
                        for f in done:
                            inflight.remove(f)
                            try:
                                process_records = f.result()
                                return process_records
                            except Exception as e:
                                logger.exception(
                                    "❌ WORKER FAILURE DETALHADO\n"
                                    f"chunk_idx={chunk_idx}\n"
                                    f"member={member.name}\n"
                                    f"error_type={type(e).__name__}\n"
                                    f"error_msg={str(e)}"
                                )
                                return None
                        return None

                    def yield_or_write(records):
                        nonlocal writer, schema, current_chunk_file, row_count, chunk_batch_count
                        nonlocal bloom_skipped  # 🌸 BLOOM FILTER — acesso ao contador

                        # ALTERAÇÃO 3: SEGURANÇA PYARROW (EVITAR CRASH SILENCIOSO)
                        safe_records = []
                        for r in records:
                            if isinstance(r, tuple) and len(r) == 4:
                                safe_records.append(r)
                            else:
                                logger.warning(f"⚠️ Registro inválido ignorado: {r}")
                        records = safe_records

                        if not records:
                            return

                        # =====================================================
                        # 🌸 BLOOM FILTER — Verificação antes de qualquer escrita
                        # O email já está normalizado em minúsculas pelo worker.
                        # Apenas emails novos (não vistos antes) são escritos.
                        # =====================================================
                        if bloom_filter is not None:
                            filtered_records = []
                            with bloom_lock:
                                for r in records:
                                    email = r[0]  # já em minúsculas (process_chunk_worker)
                                    if email not in bloom_filter:
                                        # Email novo: adicionar ao Bloom Filter e manter
                                        bloom_filter.add(email)
                                        filtered_records.append(r)
                                    else:
                                        # Email já visto: ignorar completamente
                                        bloom_skipped += 1
                            records = filtered_records

                        if not records:
                            return  # Todos os emails eram duplicatas
                        # =====================================================
                        # 🌸 FIM DA VERIFICAÇÃO DO BLOOM FILTER
                        # =====================================================

                        # Inicializa schema e writer na primeira batch
                        if writer is None:
                            current_chunk_file = RAW_CHUNKS_DIR / f"raw_chunk_{len(chunk_files):06d}_{ts}.parquet"
                            schema = pa.schema([
                                pa.field("email", pa.string()),
                                pa.field("nome", pa.string()),
                                pa.field("origem", pa.string()),
                                pa.field("data", pa.string()),
                            ])
                            writer = pq.ParquetWriter(str(current_chunk_file), schema, compression="snappy")

                        table = pa.Table.from_arrays(
                            [
                                [r[0] for r in records],  # emails
                                [r[1] for r in records],  # nomes
                                [r[2] for r in records],  # origens
                                [r[3] for r in records],  # datas
                            ],
                            names=["email", "nome", "origem", "data"]
                        )

                        writer.write_table(table)

                        # ALTERAÇÃO 5: LIBERAR MEMÓRIA APÓS ESCRITA
                        del table
                        del records
                        gc.collect()

                        row_count += len(records) if records else 0
                        chunk_batch_count += 1

                        if chunk_batch_count % 10 == 0:
                            logger.info(
                                f"{E['email']} Member {member.name}: "
                                f"{row_count:,} emails escritos | "
                                f"{E['bloom']} {bloom_skipped:,} duplicatas ignoradas pelo Bloom Filter"
                            )

                    while True:
                        # ALTERAÇÃO 2: USAR CHUNK_SIZE REDUZIDO (256MB)
                        chunk_data = fobj.read(CHUNK_SIZE)
                        if not chunk_data:
                            break

                        if stop_event.is_set():
                            break

                        check_memory()

                        # BACKPRESSURE: bloqueia envio se tiver muitos jobs ativos
                        while len(inflight) >= MAX_INFLIGHT:
                            records = drain_futures(inflight)
                            if records:
                                yield_or_write(records)

                        future = executor.submit(
                            process_chunk_worker, chunk_data, chunk_idx, member.name
                        )
                        inflight.add(future)
                        chunk_idx += 1

                    # Final flush
                    for f in inflight:
                        try:
                            records = f.result()
                            if records:
                                yield_or_write(records)
                        except Exception as e:
                            logger.exception(
                                "❌ WORKER FAILURE DETALHADO\n"
                                f"chunk_idx={chunk_idx}\n"
                                f"member={member.name}\n"
                                f"error_type={type(e).__name__}\n"
                                f"error_msg={str(e)}"
                            )

                # Fecha writer e finaliza chunk
                if writer is not None:
                    writer.close()
                    chunk_files.append(current_chunk_file)
                    logger.info(
                        f"{E['ok']} Chunk finalizado: {current_chunk_file.name} "
                        f"({row_count:,} registros) | "
                        f"{E['bloom']} {bloom_skipped:,} duplicatas ignoradas pelo Bloom Filter"
                    )

                # ALTERAÇÃO 5: LIBERAR MEMÓRIA APÓS PROCESSAR CADA MEMBER
                if writer:
                    del writer
                if schema:
                    del schema
                gc.collect()

        # Clean tar
        try:
            tar_path.unlink()
        except Exception:
            pass

    except Exception as e:
        logger.exception(
            "❌ WORKER FAILURE DETALHADO\n"
            f"chunk_idx=N/A\n"
            f"member={tar_path.name}\n"
            f"error_type={type(e).__name__}\n"
            f"error_msg={str(e)}"
        )

    return chunk_files

# ===== HUGGING FACE =====
def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    """Setup/verify datasets on Hugging Face."""
    if not token:
        raise RuntimeError("HF_TOKEN not set")

    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user")

    if not user:
        raise RuntimeError("Could not determine HF username")

    emails_repo = f"{user}/{HF_REPO_EMAILS}"
    checkpoint_repo = f"{user}/{HF_REPO_CHECKPOINT}"

    for repo_id in [emails_repo, checkpoint_repo]:
        try:
            api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
            logger.info(f"{E['ok']} Dataset created: {repo_id}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"{E['ok']} Dataset exists: {repo_id}")
            else:
                logger.warning(f"{E['warn']} Create repo: {str(e)[:100]}")

    return api, emails_repo, checkpoint_repo

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str) -> bool:
    """Upload file to HF with retry."""
    if not local_path.exists():
        logger.warning(f"{E['warn']} File not found for upload: {local_path}")
        return False

    max_retries = 3
    for attempt in range(max_retries):
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
            logger.info(f"{E['upload']} Upload OK: {repo_path}")
            return True
        except Exception as e:
            logger.warning(f"{E['warn']} Upload attempt {attempt + 1}/{max_retries} failed")
            if attempt < max_retries - 1:
                time.sleep(5)

    logger.error(f"{E['error']} Upload failed after {max_retries} attempts")
    return False

def hf_download_checkpoint(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    """Download checkpoint from HF if exists."""
    try:
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="state.json",
            local_dir=str(local_path.parent),
            token=token,
            repo_type="dataset",
        )
        logger.info(f"{E['download']} Checkpoint downloaded")
        return True
    except Exception:
        logger.info(f"{E['info']} No checkpoint found, starting fresh")
        return False

def hf_download_duckdb(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    """Download DuckDB database from HF."""
    try:
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="emails.duckdb",
            local_dir=str(local_path.parent),
            token=token,
            repo_type="dataset",
        )
        logger.info(f"{E['download']} DuckDB downloaded")
        return True
    except Exception:
        logger.info(f"{E['info']} No DuckDB backup found")
        return False

# ===== MAIN PHASES =====
def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    """PHASE 1: Download 5 torrents simultaneously."""
    logger.info(f"{E['download']} PHASE 1: Downloading {len(magnets)} torrents simultaneously")

    completed = {}

    def download_single(item):
        name = item["name"]
        magnet = item["magnet"]
        targets = item.get("targets", [])

        try:
            logger.info(f"{E['download']} Starting: {name}")
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)

            while not handle.has_metadata() and not stop_event.is_set():
                time.sleep(POLL_INTERVAL)

            if stop_event.is_set():
                raise KeyboardInterrupt()

            info = handle.get_torrent_info()
            found, missing = find_target_indices(info, targets)

            if missing:
                logger.error(f"{E['error']} Missing targets in {name} após busca inteligente.")
                raise RuntimeError(f"Targets not found in metadata")

            nfiles = info.num_files()
            for i in range(nfiles):
                handle.file_priority(i, 7 if i in found else 0)

            logger.info(f"{E['ok']} {name} ready, targets mapeados com sucesso: {found}")
            return (name, (handle, info, found))
        except Exception as e:
            logger.exception(f"{E['error']} Torrent {name} failed")
            return None

    with ThreadPoolExecutor(max_workers=len(magnets)) as executor:
        futures = [executor.submit(download_single, item) for item in magnets]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    name, data = result
                    completed[name] = data
            except Exception:
                pass

    logger.info(f"{E['ok']} PHASE 1 complete: {len(completed)}/{len(magnets)} torrents ready")
    return completed

def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    """PHASE 2: Wait for all target files to complete."""
    logger.info(f"{E['download']} PHASE 2: Waiting for all files to complete")

    all_files = []
    processed_key = state.get("downloaded_files", {})

    for tname, (handle, info, indices) in completed_torrents.items():
        if stop_event.is_set():
            break

        for idx in indices:
            if stop_event.is_set():
                break

            file_key = f"{tname}_{idx}"
            if file_key in processed_key:
                logger.info(f"{E['ok']} Skipping (already processed): {file_key}")
                continue

            expected_size = info.files().at(idx).size
            logger.info(f"{E['download']} Waiting for file: {tname} index {idx} ({human(expected_size)})")

            try:
                wait_for_file_complete(handle, idx, expected_size)
                local_path = local_path_for_index_robust(SAVE_PATH, info, idx)

                if local_path is None:
                    logger.error(f"{E['error']} File not found on disk after exhaustive search (index {idx})")
                    continue

                all_files.append((tname, local_path, info))

                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
            except Exception as e:
                logger.exception(
                    "❌ WORKER FAILURE DETALHADO\n"
                    f"chunk_idx=N/A\n"
                    f"member={tname}\n"
                    f"error_type={type(e).__name__}\n"
                    f"error_msg={str(e)}"
                )

    logger.info(f"{E['ok']} PHASE 2 complete: {len(all_files)} files ready")
    return all_files

def phase3_process_tars(tars: List[Tuple], state: Dict) -> List[Path]:
    """PHASE 3: Process tars with mmap + regex + ProcessPoolExecutor + STREAMING."""
    logger.info(f"{E['extract']} PHASE 3: Processing {len(tars)} tar files")

    all_chunks = []
    processed_tars = state.get("processed_tars", [])

    for tname, tar_path, info in tars:
        if stop_event.is_set():
            break

        if str(tar_path) in processed_tars:
            logger.info(f"{E['ok']} Skipping (already processed): {tar_path.name}")
            continue

        chunks = process_tar_with_mmap(tar_path, tname)
        all_chunks.extend(chunks)

        processed_tars.append(str(tar_path))
        state["processed_tars"] = processed_tars
        save_state(state)

    logger.info(f"{E['ok']} PHASE 3 complete: {len(all_chunks)} raw chunks generated")
    return all_chunks

def phase4_load_to_duckdb(chunks: List[Path], conn: duckdb.DuckDBPyConnection, state: Dict) -> int:
    """
    PHASE 4: Load chunks into DuckDB using native INSERT FROM read_parquet().
    """
    logger.info(f"{E['db']} PHASE 4: Loading {len(chunks)} chunks into DuckDB (native read_parquet)")

    total_inserted = 0
    loaded_chunks = state.get("loaded_chunks", [])

    for chunk_file in chunks:
        if stop_event.is_set():
            break

        if str(chunk_file) in loaded_chunks:
            logger.info(f"{E['ok']} Chunk already loaded: {chunk_file.name}")
            continue

        try:
            # NATIVE DuckDB: INSERT FROM read_parquet()
            result = conn.execute(f"""
                INSERT INTO emails_raw
                SELECT * FROM read_parquet('{chunk_file}')
                ON CONFLICT(email) DO NOTHING;
            """)
            conn.commit()

            inserted = result.rowcount if hasattr(result, 'rowcount') else 0
            total_inserted += inserted

            loaded_chunks.append(str(chunk_file))
            state["loaded_chunks"] = loaded_chunks
            save_state(state)

            logger.info(f"{E['db']} Loaded: {chunk_file.name} (+{inserted:,} records)")
        except Exception as e:
            # Fallback: Se read_parquet nativo falhar, usa pandas
            logger.warning(f"{E['warn']} Native read_parquet failed, falling back to pandas")
            try:
                df = pd.read_parquet(chunk_file)
                records = [tuple(row) for row in df.itertuples(index=False, name=None)]
                inserted = batch_insert_duckdb(conn, records)
                total_inserted += inserted

                loaded_chunks.append(str(chunk_file))
                state["loaded_chunks"] = loaded_chunks
                save_state(state)

                logger.info(f"{E['db']} Loaded (fallback): {chunk_file.name} (+{inserted:,} records)")
            except Exception as ex:
                logger.exception(
                    "❌ WORKER FAILURE DETALHADO\n"
                    f"chunk_idx=N/A\n"
                    f"member={chunk_file.name}\n"
                    f"error_type={type(ex).__name__}\n"
                    f"error_msg={str(ex)}"
                )

    logger.info(f"{E['ok']} PHASE 4 complete: {total_inserted:,} total records inserted")
    return total_inserted

def phase5_deduplicate_duckdb(conn: duckdb.DuckDBPyConnection) -> int:
    """PHASE 5: Global deduplication using DuckDB SELECT DISTINCT."""
    logger.info(f"{E['db']} PHASE 5: Global deduplication (SELECT DISTINCT)")

    try:
        count_before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Records before dedup: {count_before:,}")

        conn.execute("CREATE TABLE IF NOT EXISTS emails_dedup AS SELECT DISTINCT * FROM emails_raw;")
        conn.execute("DROP TABLE IF EXISTS emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()

        count_after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        duplicates = count_before - count_after

        logger.info(f"{E['stats']} Records after dedup: {count_after:,}")
        logger.info(f"{E['email']} Duplicates removed: {duplicates:,}")

        return count_after
    except Exception as e:
        logger.exception(
            "❌ WORKER FAILURE DETALHADO\n"
            "chunk_idx=N/A\n"
            "member=DuckDB_Deduplication\n"
            f"error_type={type(e).__name__}\n"
            f"error_msg={str(e)}"
        )
        return 0

def phase6_export_final_files(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    """PHASE 6: Generate final Trader_Emails_*.parquet files (30M rows each)."""
    logger.info(f"{E['email']} PHASE 6: Generating final datasets (30M rows per file)")

    final_files = []
    file_num = 1
    offset = 0

    while not stop_event.is_set():
        try:
            rows_df = conn.execute(
                f"SELECT * FROM emails_raw LIMIT {ROWS_PER_FINAL_FILE} OFFSET {offset};"
            ).fetchdf()

            if rows_df.shape[0] == 0:
                break

            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            final_file = EXPORT_DIR / f"Trader_Emails_{file_num:03d}_{ts}.parquet"

            table = pa.Table.from_pandas(rows_df)
            pq.write_table(table, str(final_file), compression="snappy")

            final_files.append(final_file)
            logger.info(f"{E['ok']} Generated: {final_file.name} ({rows_df.shape[0]:,} rows)")

            file_num += 1
            offset += ROWS_PER_FINAL_FILE
        except Exception as e:
            logger.exception(
                "❌ WORKER FAILURE DETALHADO\n"
                f"chunk_idx=N/A\n"
                f"member=Export_File_{file_num}\n"
                f"error_type={type(e).__name__}\n"
                f"error_msg={str(e)}"
            )
            break

    logger.info(f"{E['ok']} PHASE 6 complete: {len(final_files)} final datasets generated")
    return final_files

def phase7_upload_hf(api: HfApi, token: str, emails_repo: str, checkpoint_repo: str, final_files: List[Path], db_path: Path, state: Dict):
    """PHASE 7: Upload to HF and update checkpoint."""
    logger.info(f"{E['upload']} PHASE 7: Uploading to Hugging Face")

    # Upload final datasets
    for final_file in final_files:
        if stop_event.is_set():
            break

        repo_path = f"Trader_Emails/{final_file.name}"
        if hf_upload_file(api, token, emails_repo, final_file, repo_path):
            try:
                final_file.unlink()
            except Exception:
                pass

    # Upload checkpoint files
    logger.info(f"{E['upload']} Uploading checkpoint to Hugging Face")
    hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")

    if db_path.exists():
        hf_upload_file(api, token, checkpoint_repo, db_path, "emails.duckdb")

    # =====================================================
    # 🌸 BLOOM FILTER — Guardar no HF junto com o checkpoint
    # Garante que nunca se perde progresso em caso de falha ou timeout.
    # =====================================================
    logger.info(f"{E['bloom']} A guardar Bloom Filter no checkpoint...")
    save_bloom_filter(api, token)
    # =====================================================
    # 🌸 FIM DO CHECKPOINT DO BLOOM FILTER
    # =====================================================

    # Update state
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    state["final_files_uploaded"] = len(final_files)
    save_state(state)

    logger.info(f"{E['ok']} PHASE 7 complete: Checkpoint saved to HF")

def main():
    """Main orchestration."""
    logger.info(f"{E['start']} Minerador Production v1 (OTIMIZADO + BLOOM FILTER)")
    logger.info(f"{E['info']} SAVE_PATH: {SAVE_PATH}")
    logger.info(f"{E['info']} CPU cores: {os.cpu_count()}")
    logger.info(f"{E['stats']} Disk usage: {disk_usage(SAVE_PATH)}")

    # ALTERAÇÕES APLICADAS: Mostrar configurações de otimização no startup
    logger.info(f"{E['cpu']} ═══ OTIMIZAÇÕES DE MEMÓRIA APLICADAS ═══")
    logger.info(f"{E['cpu']} ALTERAÇÃO 1: PRAGMA memory_limit = 8GB (de 12GB)")
    logger.info(f"{E['cpu']} ALTERAÇÃO 2: CHUNK_SIZE = 256 MB (de 1 GB)")
    logger.info(f"{E['cpu']} ALTERAÇÃO 3: MAX_WORKERS = min(6, {os.cpu_count() or 4})")
    logger.info(f"{E['cpu']} ALTERAÇÃO 4: MAX_INFLIGHT = 8")
    logger.info(f"{E['cpu']} ALTERAÇÃO 5: gc.collect() após ParquetWriter")
    logger.info(f"{E['cpu']} ALTERAÇÃO 6: RAM% | usada(GB) | livre(GB)")
    logger.info(f"{E['cpu']} ════════════════════════════════════════")

    # 🌸 BLOOM FILTER — Log de configuração
    logger.info(f"{E['bloom']} ═══ BLOOM FILTER ═══")
    logger.info(f"{E['bloom']} Repositório HF : {HF_REPO_BLOOM}")
    logger.info(f"{E['bloom']} Capacidade     : {BLOOM_CAPACITY:,} entradas")
    logger.info(f"{E['bloom']} Taxa de erro   : {BLOOM_ERROR_RATE * 100:.1f}%")
    logger.info(f"{E['bloom']} ═══════════════════")

    # Verify HF token
    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN not set in environment")
        sys.exit(2)

    # Setup HF
    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.exception(
            "❌ WORKER FAILURE DETALHADO\n"
            "chunk_idx=N/A\n"
            "member=HF_Setup\n"
            f"error_type={type(e).__name__}\n"
            f"error_msg={str(e)}"
        )
        sys.exit(1)

    # Download checkpoint from HF
    logger.info(f"{E['download']} Downloading checkpoint from Hugging Face")
    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    hf_download_duckdb(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)

    # =====================================================
    # 🌸 BLOOM FILTER — Carregar do HF no início da sessão
    # Se existir, retoma do estado anterior.
    # Se não existir, cria novo automaticamente.
    # =====================================================
    global bloom_filter
    bloom_filter = load_bloom_filter(api, HF_TOKEN)
    logger.info(
        f"{E['bloom']} Bloom Filter pronto: "
        f"~{bloom_filter.count:,} emails já conhecidos"
    )
    # =====================================================
    # 🌸 FIM DO CARREGAMENTO DO BLOOM FILTER
    # =====================================================

    state = load_state()
    logger.info(f"{E['ok']} State loaded with {len(state)} entries")

    # Initialize DuckDB and libtorrent
    conn = init_duckdb(DB_PATH)

    try:
        session = create_libtorrent_session()
    except Exception as e:
        logger.exception(
            "❌ WORKER FAILURE DETALHADO\n"
            "chunk_idx=N/A\n"
            "member=Libtorrent_Init\n"
            f"error_type={type(e).__name__}\n"
            f"error_msg={str(e)}"
        )
        sys.exit(1)

    try:
        overall_start = time.time()

        # PHASE 1
        completed_torrents = phase1_download_torrents(session, MAGNETS)

        if not completed_torrents:
            logger.error(f"{E['error']} No torrents completed successfully")
            return

        if stop_event.is_set():
            logger.warning(f"{E['warn']} Stopped during PHASE 1")
            return

        # PHASE 2
        tars = phase2_wait_downloads(completed_torrents, state)

        if tars and not stop_event.is_set():
            # PHASE 3
            chunks = phase3_process_tars(tars, state)

            if chunks and not stop_event.is_set():
                # PHASE 4
                phase4_load_to_duckdb(chunks, conn, state)

                if not stop_event.is_set():
                    # PHASE 5
                    total_emails = phase5_deduplicate_duckdb(conn)

                    if not stop_event.is_set():
                        # PHASE 6
                        final_files = phase6_export_final_files(conn)

                        if not stop_event.is_set():
                            # PHASE 7 (inclui save_bloom_filter via checkpoint)
                            phase7_upload_hf(api, HF_TOKEN, emails_repo, checkpoint_repo, final_files, DB_PATH, state)

        total_time = time.time() - overall_start
        logger.info(f"{E['stats']} Total runtime: {total_time / 60:.2f} minutes")
        logger.info(f"{E['ok']} Minerador Production completed successfully")

    except KeyboardInterrupt:
        logger.warning(f"{E['warn']} Graceful shutdown initiated")
        # 🌸 BLOOM FILTER — Guardar mesmo em caso de shutdown gracioso
        logger.info(f"{E['bloom']} A guardar Bloom Filter antes de sair...")
        try:
            save_bloom_filter(api, HF_TOKEN)
        except Exception:
            pass
    except Exception as e:
        logger.exception(
            "❌ WORKER FAILURE DETALHADO\n"
            "chunk_idx=N/A\n"
            "member=Main_Orchestration\n"
            f"error_type={type(e).__name__}\n"
            f"error_msg={str(e)}"
        )
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
