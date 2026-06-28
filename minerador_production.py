#!/usr/bin/env python3
"""
minerador_production_v8_email_nome.py

Pipeline com foco em qualidade:
- Apenas colunas: email + nome (sem origem, sem data)
- Dedup por email normalizado (um registro por endereço)
- Disposable = só provedores temporários (Gmail/Yahoo/etc. NÃO são descartados)
- Rejeita emails corporativos/genéricos (support, info, noreply, ...)
- Validação pós-regex mais rigorosa
- Limite 200M inserções brutas → dedup → export Parquet → HF
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
import unicodedata
import warnings
import difflib
import traceback
import uuid
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional
from threading import Event, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

warnings.filterwarnings("ignore", category=DeprecationWarning, module="libtorrent")

import libtorrent as lt
from huggingface_hub import HfApi
import pandas as pd
import duckdb

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("⚠️  psutil não instalado (pip install psutil). Monitoramento desativado.")

# ===== LIMITE =====
EMAIL_LIMIT = 200_000_000
email_counter = 0
email_counter_lock = Lock()


def increment_email_counter(count: int) -> int:
    global email_counter
    with email_counter_lock:
        email_counter += count
        return email_counter


def get_email_counter() -> int:
    with email_counter_lock:
        return email_counter


def has_reached_email_limit() -> bool:
    return get_email_counter() >= EMAIL_LIMIT


# ===== LOGGING =====
class ColoredFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        record.levelname = f"{color}{self.BOLD}{levelname:8s}{self.RESET}"
        record.msg = str(record.msg)
        return super().format(record)


def setup_logging(log_path: Path, log_level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("minerador_v8")
    logger.setLevel(log_level)
    logger.handlers = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(
        ColoredFormatter(fmt="%(asctime)s │ %(levelname)s │ %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )

    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s │ %(levelname)s │ %(funcName)s:%(lineno)d │ %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


# ===== PATHS =====
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
RAW_CHUNKS_DIR = SAVE_PATH / "raw_chunks"
RAW_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = SAVE_PATH / "emails.duckdb"
STATE_PATH = SAVE_PATH / "state.json"
LOG_PATH = SAVE_PATH / "minerador.log"

for d in [EXPORT_DIR, TEMP_DIR]:
    d.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO_EMAILS = os.environ.get("HF_REPO_EMAILS", "Trader_Emails")
HF_REPO_CHECKPOINT = os.environ.get("HF_REPO_CHECKPOINT", "minerador_checkpoints")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "3"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BATCH_INSERT_SIZE = int(os.environ.get("BATCH_INSERT_SIZE", "100000"))
ROWS_PER_FINAL_FILE = int(os.environ.get("ROWS_PER_FINAL_FILE", "30000000"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(512 * 1024 * 1024)))
FILE_DOWNLOAD_TIMEOUT = int(os.environ.get("FILE_DOWNLOAD_TIMEOUT", str(7200)))

logger = setup_logging(LOG_PATH, LOG_LEVEL)

E = {
    "start": "🚀",
    "download": "📥",
    "extract": "📦",
    "stats": "📊",
    "space": "💾",
    "email": "📧",
    "upload": "📤",
    "clean": "🧹",
    "warn": "⚠️",
    "error": "❌",
    "ok": "✅",
    "info": "ℹ️",
    "cpu": "⚙️",
    "clock": "⏱️",
    "list": "📋",
    "db": "🗄️",
    "monitor": "📡",
    "limit": "🛑",
}

# Regex inicial (captura candidatos; validação estrita vem depois)
EMAIL_REGEX = re.compile(
    rb"\b[A-Za-z0-9](?:[A-Za-z0-9._%+-]{0,62}[A-Za-z0-9])?@[A-Za-z0-9](?:[A-Za-z0-9.-]{0,253}[A-Za-z0-9])?\.[A-Za-z]{2,63}\b",
    re.IGNORECASE,
)

stop_event = Event()
state_lock = Lock()


def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum}; graceful shutdown")
    stop_event.set()


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

SUPPORTED_EXTENSIONS = {
    ".txt", ".csv", ".log", ".tsv", ".json", ".sql", ".xml",
    ".lst", ".list", ".cfg", ".conf", ".ini", ".dat",
}

MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2f%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2f%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2f%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2f%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        "targets": [
            "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
        ],
    },
]

# Apenas provedores temporários / descartáveis (NÃO inclui Gmail, Yahoo, etc.)
DISPOSABLE_DOMAINS = {
    "10minutemail.com", "10minutemailbox.com", "tempmail.com", "temp-mail.org",
    "temp-mail.io", "temp-mail.info", "tempmail.email", "tempmail.us", "tempmail.it",
    "tempmail.pro", "tempmail24.com", "temp-mailbox.com", "temporary-mail.net",
    "throwaway.email", "guerrillamail.com", "guerrillamail.net", "mailinator.com",
    "yopmail.com", "maildrop.cc", "trashmail.com", "trashmail.ws", "trash-mail.com",
    "fakeinbox.com", "mailnesia.com", "mailnesia.net", "sharklasers.com", "spam4.me",
    "spamgourmet.com", "mytrashmail.com", "grr.la", "minute-mail.com",
    "maildisposable.com", "fakeemail.net", "mailbox.ga", "oneclickmail.com",
    "temp.email", "speedymail.org", "emailondeck.com", "schrott.email",
    "mail1.eu", "mailtest.in", "getnada.com", "mintemail.com", "dispostable.com",
    "mailcatch.com", "tempinbox.com", "mohmal.com", "emailfake.com",
}

# Local-parts corporativos / genéricos (não são pessoas)
ROLE_LOCAL_EXACT = {
    "support", "info", "information", "admin", "administrator", "sales", "marketing",
    "contact", "contacts", "hello", "help", "helpdesk", "service", "services",
    "customerservice", "customer.service", "customercare", "care", "billing",
    "accounts", "accounting", "finance", "hr", "jobs", "careers", "recruitment",
    "noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon", "postmaster",
    "webmaster", "hostmaster", "abuse", "security", "privacy", "legal", "compliance",
    "newsletter", "news", "notifications", "notification", "alerts", "alert",
    "team", "office", "reception", "enquiries", "inquiry", "inquiries", "feedback",
    "orders", "order", "shipping", "returns", "refunds", "payments", "payment",
    "subscribe", "unsubscribe", "register", "registration", "signup", "sign-up",
    "bot", "system", "daemon", "root", "null", "test", "testing", "demo", "sample",
    "officeadmin", "techsupport", "itsupport", "supportteam", "salesteam",
}

ROLE_LOCAL_PREFIXES = (
    "noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon",
    "support+", "info+", "admin+", "sales+", "contact+",
)

ROLE_LOCAL_RE = re.compile(
    r"^(?:support|info|admin|sales|contact|help|service|billing|accounts|"
    r"customerservice|customercare|noreply|no[\-_.]?reply|donotreply|"
    r"newsletter|notification|webmaster|postmaster|abuse|team|office|"
    r"orders|shipping|feedback|enquir(?:y|ies)|helpdesk|mailer)[\-_.+]?.*$",
    re.IGNORECASE,
)


def normalize_email(email: str) -> str:
    return email.strip().lower()


def is_disposable_domain(domain: str) -> bool:
    d = domain.lower().strip()
    if d in DISPOSABLE_DOMAINS:
        return True
    # subdomínios de alguns temp-mail
    for base in DISPOSABLE_DOMAINS:
        if d.endswith("." + base):
            return True
    return False


def is_role_or_corporate_local(local: str) -> bool:
    if not local:
        return True
    l = local.lower().strip()
    if l in ROLE_LOCAL_EXACT:
        return True
    for p in ROLE_LOCAL_PREFIXES:
        if l.startswith(p):
            return True
    if ROLE_LOCAL_RE.match(l):
        return True
    # só números ou muito curto sem letras
    if len(l) < 2:
        return True
    if not re.search(r"[a-z]", l):
        return True
    return False


def is_valid_email_strict(email: str) -> bool:
    if not email or len(email) > 254:
        return False
    if "@" not in email:
        return False
    local, _, domain = email.rpartition("@")
    if not local or not domain or "." not in domain:
        return False
    if len(local) > 64:
        return False
    if ".." in email or local.startswith(".") or local.endswith("."):
        return False
    if domain.startswith(".") or domain.endswith(".") or ".." in domain:
        return False
    labels = domain.split(".")
    if len(labels) < 2:
        return False
    tld = labels[-1]
    if not tld.isalpha() or len(tld) < 2:
        return False
    for label in labels:
        if not label or len(label) > 63:
            return False
        if label.startswith("-") or label.endswith("-"):
            return False
    if is_disposable_domain(domain):
        return False
    if is_role_or_corporate_local(local):
        return False
    return True


def derive_nome_from_email(email: str) -> str:
    local = email.split("@", 1)[0]
    # remove tags comuns +suffix
    local = re.sub(r"\+.*$", "", local)
    # separadores → espaço
    local = re.sub(r"[._\-]+", " ", local)
    # remove dígitos isolados no fim/início mas mantém letras
    parts = []
    for p in local.split():
        p_clean = re.sub(r"^\d+|\d+$", "", p)
        p_clean = re.sub(r"[^a-zA-ZÀ-ÿ]", "", p_clean)
        if len(p_clean) >= 2:
            parts.append(p_clean.capitalize())
    if parts:
        return " ".join(parts[:4])  # máx. 4 tokens de nome
    # fallback: primeira letra run do local
    letters = re.findall(r"[A-Za-zÀ-ÿ]{2,}", local)
    if letters:
        return " ".join(w.capitalize() for w in letters[:3])
    return ""


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"


def disk_usage(path: Path = SAVE_PATH) -> Dict[str, str]:
    try:
        du = shutil.disk_usage(str(path))
        return {
            "total": human(du.total),
            "used": human(du.used),
            "free": human(du.free),
            "percent": f"{(du.used / du.total * 100):.1f}%",
        }
    except Exception as e:
        return {"error": str(e)}


def check_disk_space(path: Path, min_free_gb: int = 5) -> bool:
    try:
        du = shutil.disk_usage(str(path))
        free_gb = du.free / (1024**3)
        if free_gb < min_free_gb:
            logger.error(f"{E['error']} DISCO INSUFICIENTE: {free_gb:.1f}GB livre, mínimo {min_free_gb}GB")
            raise RuntimeError("Espaço insuficiente")
        logger.info(f"{E['space']} Disco: {free_gb:.1f}GB livre (OK)")
        return True
    except Exception as e:
        logger.error(f"{E['error']} Erro disco: {e}")
        return False


def start_resource_monitor(interval: int = 10):
    if not HAS_PSUTIL:
        return None

    def monitor_loop():
        while not stop_event.is_set():
            try:
                mem = psutil.virtual_memory()
                cpu = psutil.cpu_percent(interval=1)
                disk = shutil.disk_usage(str(SAVE_PATH))
                disk_free_gb = disk.free / (1024**3)
                current_emails = get_email_counter()
                logger.info(
                    f"{E['monitor']} RAM: {mem.percent:.1f}% ({human(mem.used)}/{human(mem.total)}) | "
                    f"CPU: {cpu:.1f}% | Disco: {disk_free_gb:.1f}GB | "
                    f"Inserções brutas: {current_emails:,}/{EMAIL_LIMIT:,}"
                )
                time.sleep(interval)
            except Exception:
                time.sleep(interval)

    thread = threading.Thread(target=monitor_loop, daemon=True, name="ResourceMonitor")
    thread.start()
    return thread


def save_state(state: Dict[str, Any]):
    with state_lock:
        try:
            with open(STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro salvando state: {e}")


def load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Erro carregando state: {e}")
        return {}


def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute("SET threads=8;")
    if HAS_PSUTIL:
        duckdb_mem_gb = max(int((psutil.virtual_memory().total * 0.6) / (1024**3)), 2)
    else:
        duckdb_mem_gb = 12
    conn.execute(f"SET memory_limit='{duckdb_mem_gb}GB';")
    conn.execute(f"SET temp_directory='{str(TEMP_DIR)}';")

    # Migração: se tabela antiga tiver origem/data, recria estrutura nova
    tables = [r[0] for r in conn.execute("SHOW TABLES;").fetchall()]
    if "emails_raw" in tables:
        cols = {r[1] for r in conn.execute("PRAGMA table_info('emails_raw');").fetchall()}
        if cols != {"email", "nome"}:
            logger.warning(f"{E['warn']} Schema antigo detectado {cols}; recriando emails_raw (email, nome)")
            conn.execute("DROP TABLE IF EXISTS emails_raw;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS emails_raw (
            email VARCHAR NOT NULL,
            nome VARCHAR
        );
        """
    )
    conn.commit()
    logger.info(f"{E['ok']} DuckDB OK (email + nome)")
    return conn


def insert_records_into_duckdb(conn: duckdb.DuckDBPyConnection, records: List[Tuple[str, str]]) -> int:
    if not records:
        return 0
    try:
        df = pd.DataFrame(records, columns=["email", "nome"])
        tmp_name = f"tmp_df_{uuid.uuid4().hex[:8]}"
        conn.register(tmp_name, df)
        conn.execute("BEGIN TRANSACTION;")
        conn.execute(f"INSERT INTO emails_raw SELECT email, nome FROM {tmp_name};")
        conn.execute("COMMIT;")
        try:
            conn.unregister(tmp_name)
        except Exception:
            pass
        return len(df)
    except Exception as e:
        logger.error(f"{E['error']} Insert: {e}")
        try:
            conn.execute("ROLLBACK;")
        except Exception:
            pass
        return 0


# ===== LIBTORRENT (inalterado em essência) =====
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
    except Exception:
        pass
    return session


def list_all_torrent_files(torrent_info) -> Dict[int, Dict]:
    files_map: Dict[int, Dict] = {}
    try:
        n = getattr(torrent_info, "num_files")()
    except Exception:
        try:
            n = len(torrent_info.files())
        except Exception:
            return files_map
    for i in range(n):
        try:
            file_path = file_size = None
            try:
                fe = torrent_info.files().at(i)
                file_path, file_size = fe.path, fe.size
            except Exception:
                try:
                    fe = torrent_info.files()[i]
                    file_path, file_size = fe.path, fe.size
                except Exception:
                    try:
                        file_path = torrent_info.file_path(i)
                        file_size = torrent_info.file_size(i)
                    except Exception:
                        pass
            if file_path is None:
                continue
            files_map[i] = {"path": file_path, "size": int(file_size or 0), "basename": Path(file_path).name}
        except Exception:
            continue
    return files_map


def normalize_str(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.casefold()


def parse_target_index(target: str) -> Optional[int]:
    if target is None:
        return None
    t = str(target).strip()
    m = re.search(r"\[\s*(\d+)\s*\]", t)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(?:index|idx)\s*[:=]\s*(\d+)\b", t, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    if re.fullmatch(r"\d+", t):
        return int(t)
    return None


def find_targets_exact(torrent_info, targets: List[str]) -> Tuple[List[int], Dict[int, Dict]]:
    files_map = list_all_torrent_files(torrent_info)
    if not files_map:
        return [], {}
    normalized_path_map = {idx: normalize_str(info["path"]) for idx, info in files_map.items()}
    normalized_basename_map = {idx: normalize_str(info["basename"]) for idx, info in files_map.items()}
    all_paths = list(normalized_path_map.values())
    all_basenames = list(normalized_basename_map.values())
    found_indices = []
    for target in targets:
        t_raw = str(target)
        idx_hint = parse_target_index(t_raw)
        if idx_hint is not None and idx_hint in files_map:
            found_indices.append(idx_hint)
            continue
        t_normalized = normalize_str(t_raw)
        t_normalized = re.sub(r"^\W*\[\s*\d+\s*\]\s*", "", t_normalized).strip()
        matched = False
        for idx, norm_path in normalized_path_map.items():
            if norm_path == t_normalized:
                found_indices.append(idx)
                matched = True
                break
        if matched:
            continue
        target_basename = normalize_str(Path(t_raw).name)
        if target_basename:
            for idx, norm_base in normalized_basename_map.items():
                if norm_base == target_basename:
                    found_indices.append(idx)
                    matched = True
                    break
            if matched:
                continue
        close = difflib.get_close_matches(t_normalized, all_paths, n=1, cutoff=0.82)
        if close:
            idx_chosen = [i for i, p in normalized_path_map.items() if p == close[0]]
            if idx_chosen:
                found_indices.append(idx_chosen[0])
                continue
        close_base = difflib.get_close_matches(target_basename, all_basenames, n=1, cutoff=0.82)
        if close_base:
            idx_chosen = [i for i, b in normalized_basename_map.items() if b == close_base[0]]
            if idx_chosen:
                found_indices.append(idx_chosen[0])
    return sorted(set(found_indices)), files_map


def local_path_for_index(save_path: Path, torrent_info, index: int) -> Optional[Path]:
    try:
        torrent_name = torrent_info.name()
    except Exception:
        torrent_name = "unknown"
    try:
        fe = torrent_info.files().at(index)
        file_path = fe.path
    except Exception:
        try:
            file_path = torrent_info.files()[index].path
        except Exception:
            return None
    return save_path / torrent_name / file_path


def wait_for_file_complete(handle, file_index: int, expected_size: int, timeout: int = FILE_DOWNLOAD_TIMEOUT) -> bool:
    last_log = start_time = time.time()
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        try:
            fprog = handle.file_progress()
            got = int(fprog[file_index]) if file_index < len(fprog) else 0
            if time.time() - last_log >= 5:
                pct = (got / expected_size * 100) if expected_size else 0.0
                logger.info(f"{E['download']} [{file_index}]: {human(got)}/{human(expected_size)} ({pct:.1f}%)")
                last_log = time.time()
            if expected_size and got >= expected_size:
                return True
            if time.time() - start_time > timeout:
                return False
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            raise
        except Exception:
            time.sleep(POLL_INTERVAL)


def process_chunk_worker(chunk_data: bytes, chunk_idx: int) -> List[Tuple[str, str]]:
    """Extrai (email, nome) válidos — sem origem/data."""
    results: List[Tuple[str, str]] = []
    seen_in_chunk: set[str] = set()
    try:
        for match in EMAIL_REGEX.finditer(chunk_data):
            try:
                email_b = match.group()
                try:
                    raw = email_b.decode("utf-8", "ignore")
                except Exception:
                    raw = email_b.decode("latin-1", "ignore")
                email = normalize_email(raw)
                if not is_valid_email_strict(email):
                    continue
                if email in seen_in_chunk:
                    continue
                seen_in_chunk.add(email)
                nome = derive_nome_from_email(email)
                results.append((email, nome))
            except Exception:
                continue
    except Exception as e:
        logger.error(f"{E['error']} Worker chunk {chunk_idx}: {e}")
    return results


def process_tar_streaming_and_insert(tar_path: Path, conn: duckdb.DuckDBPyConnection) -> int:
    cpu_count = os.cpu_count() or 4
    max_workers = min(4, cpu_count // 2)
    total_records_inserted = 0
    logger.info(f"{E['extract']} TAR: {tar_path.name} ({human(tar_path.stat().st_size)})")

    try:
        with tarfile.open(tar_path, "r:*") as tar:
            member_count = 0
            for member in tar:
                if has_reached_email_limit() or stop_event.is_set():
                    break
                if not member.isfile():
                    continue
                ext = Path(member.name).suffix.lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue
                member_count += 1
                logger.info(f"{E['extract']} [{member_count}] {member.name}")
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue

                batch: List[Tuple[str, str]] = []
                futures: Dict[Any, int] = {}
                with ProcessPoolExecutor(max_workers=max_workers) as executor:
                    reading_complete = False
                    chunk_idx = 0

                    while not reading_complete or futures:
                        if has_reached_email_limit():
                            reading_complete = True
                        if not reading_complete:
                            chunk_data = fobj.read(CHUNK_SIZE)
                            if chunk_data:
                                fut = executor.submit(process_chunk_worker, chunk_data, chunk_idx)
                                futures[fut] = chunk_idx
                                chunk_idx += 1
                            else:
                                reading_complete = True
                        if futures:
                            done, _ = wait(futures.keys(), timeout=0.1, return_when=FIRST_COMPLETED)
                            for fut in done:
                                try:
                                    records = fut.result()
                                    if records:
                                        batch.extend(records)
                                        if len(batch) >= BATCH_INSERT_SIZE:
                                            inserted = _flush_batch_with_limit(conn, batch)
                                            total_records_inserted += inserted
                                            batch = []
                                            if has_reached_email_limit():
                                                reading_complete = True
                                                break
                                except Exception as e:
                                    logger.error(f"{E['error']} Future: {e}")
                                finally:
                                    del futures[fut]

                    while futures and not has_reached_email_limit():
                        done, _ = wait(futures.keys(), timeout=1.0)
                        for fut in done:
                            try:
                                records = fut.result()
                                if records:
                                    batch.extend(records)
                                    if len(batch) >= BATCH_INSERT_SIZE:
                                        total_records_inserted += _flush_batch_with_limit(conn, batch)
                                        batch = []
                            except Exception as e:
                                logger.error(f"{E['error']} Future: {e}")
                            finally:
                                del futures[fut]

                if batch and not has_reached_email_limit():
                    total_records_inserted += _flush_batch_with_limit(conn, batch)

        try:
            tar_path.unlink()
            logger.info(f"{E['clean']} TAR removido")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"{E['error']} TAR: {e}")
        logger.debug(traceback.format_exc())

    logger.info(f"{E['ok']} TAR: {total_records_inserted:,} inserções brutas\n")
    return total_records_inserted


def _flush_batch_with_limit(conn: duckdb.DuckDBPyConnection, batch: List[Tuple[str, str]]) -> int:
    if not batch:
        return 0
    current = get_email_counter()
    if current >= EMAIL_LIMIT:
        return 0
    if current + len(batch) > EMAIL_LIMIT:
        batch = batch[: EMAIL_LIMIT - current]
    inserted = insert_records_into_duckdb(conn, batch)
    increment_email_counter(inserted)
    logger.info(f"{E['ok']} Batch: {inserted:,} | Bruto: {get_email_counter():,}/{EMAIL_LIMIT:,}")
    return inserted


# ===== HF =====
def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    if not token:
        raise RuntimeError("HF_TOKEN não definido")
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user")
    emails_repo = f"{user}/{HF_REPO_EMAILS}"
    checkpoint_repo = f"{user}/{HF_REPO_CHECKPOINT}"
    for repo_id in [emails_repo, checkpoint_repo]:
        try:
            api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        except Exception as e:
            if "already exists" not in str(e).lower():
                logger.warning(f"{E['warn']} create_repo: {e}")
    return api, emails_repo, checkpoint_repo


def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str) -> bool:
    if not local_path.exists():
        return False
    for attempt in range(3):
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
            return True
        except Exception as e:
            logger.warning(f"{E['warn']} Upload {attempt + 1}/3: {e}")
            time.sleep((attempt + 1) * 10)
    return False


def hf_download_checkpoint(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    try:
        api.hf_hub_download(
            repo_id=checkpoint_repo, filename="state.json",
            local_dir=str(local_path), token=token, repo_type="dataset",
        )
        return True
    except Exception:
        return False


def hf_download_duckdb(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    try:
        api.hf_hub_download(
            repo_id=checkpoint_repo, filename="emails.duckdb",
            local_dir=str(local_path), token=token, repo_type="dataset",
        )
        return True
    except Exception:
        return False


def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    logger.info(f"\n{'='*100}\n{E['download']} FASE 1: Download\n{'='*100}\n")
    completed: Dict[str, Tuple] = {}

    def download_single(item):
        name, magnet, targets = item["name"], item["magnet"], item.get("targets", [])
        try:
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)
            for _ in range(600):
                if stop_event.is_set():
                    return None
                try:
                    if handle.has_metadata():
                        break
                except Exception:
                    try:
                        handle.torrent_info()
                        break
                    except Exception:
                        pass
                time.sleep(1)
            else:
                return None
            info = handle.torrent_info()
            found, all_files = find_targets_exact(info, targets)
            if not found:
                return None
            nfiles = max(all_files.keys()) + 1
            for i in range(nfiles):
                try:
                    handle.file_priority(i, 7 if i in found else 0)
                except Exception:
                    pass
            return (name, (handle, info, found, all_files))
        except Exception as e:
            logger.error(f"{E['error']} {name}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=min(len(magnets), 5)) as ex:
        for fut in as_completed([ex.submit(download_single, m) for m in magnets]):
            r = fut.result()
            if r:
                completed[r[0]] = r[1]
    return completed


def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    logger.info(f"\n{'='*100}\n{E['download']} FASE 2: Downloads\n{'='*100}\n")
    all_files_ready: List[Tuple] = []
    processed_key = state.get("downloaded_files", {})
    for tname, (handle, info, indices, all_files_map) in completed_torrents.items():
        if stop_event.is_set():
            break
        for idx in indices:
            file_key = f"{tname}_{idx}"
            if file_key in processed_key:
                continue
            expected_size = all_files_map.get(idx, {}).get("size", 0)
            if not wait_for_file_complete(handle, idx, expected_size):
                continue
            local_path = local_path_for_index(SAVE_PATH, info, idx)
            if local_path and not local_path.exists():
                basename = Path(all_files_map[idx]["path"]).name
                found = list((SAVE_PATH / info.name()).rglob(basename)) if hasattr(info, "name") else []
                if found:
                    local_path = found[0]
                else:
                    continue
            if local_path:
                all_files_ready.append((tname, local_path, info))
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
    return all_files_ready


def phase3_process_tars(tars: List[Tuple], state: Dict, conn: duckdb.DuckDBPyConnection) -> int:
    logger.info(f"\n{'='*100}\n{E['extract']} FASE 3 (limite brutas {EMAIL_LIMIT:,})\n{'='*100}\n")
    if not check_disk_space(SAVE_PATH, 5):
        raise RuntimeError("Disco insuficiente")
    total = 0
    processed = state.get("processed_tars", [])
    for tname, tar_path, _ in tars:
        if has_reached_email_limit() or stop_event.is_set():
            break
        if str(tar_path) in processed:
            continue
        total += process_tar_streaming_and_insert(tar_path, conn)
        processed.append(str(tar_path))
        state["processed_tars"] = processed
        save_state(state)
    return total


def phase5_deduplicate(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Um email = uma linha. Escolhe o nome mais informativo (maior length).
    """
    logger.info(f"\n{'='*100}\n{E['db']} FASE 5: Dedup por email\n{'='*100}\n")
    try:
        before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Linhas brutas: {before:,}")

        conn.execute("DROP TABLE IF EXISTS emails_dedup;")
        conn.execute(
            """
            CREATE TABLE emails_dedup AS
            SELECT
                lower(trim(email)) AS email,
                arg_max(
                    COALESCE(NULLIF(trim(nome), ''), ''),
                    length(COALESCE(NULLIF(trim(nome), ''), ''))
                ) AS nome
            FROM emails_raw
            WHERE email IS NOT NULL AND trim(email) <> ''
            GROUP BY 1;
            """
        )
        conn.execute("DROP TABLE IF EXISTS emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()

        after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        unique_check = conn.execute(
            "SELECT COUNT(*) = COUNT(DISTINCT email) FROM emails_raw;"
        ).fetchone()[0]
        logger.info(f"{E['stats']} Emails únicos: {after:,}")
        logger.info(f"{E['email']} Removidas duplicatas: {before - after:,}")
        logger.info(f"{E['ok']} Garantia 1 email/linha: {bool(unique_check)}")
        return after
    except Exception as e:
        logger.error(f"{E['error']} Dedup: {e}")
        logger.debug(traceback.format_exc())
        return 0


def phase6_export(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    logger.info(f"\n{'='*100}\n{E['email']} FASE 6: Export (email, nome)\n{'='*100}\n")
    final_files: List[Path] = []
    file_num = 1

    cols = [r[1] for r in conn.execute("PRAGMA table_info('emails_raw');").fetchall()]
    if "id" not in cols:
        conn.execute(
            """
            CREATE TABLE emails_raw_id AS
            SELECT ROW_NUMBER() OVER (ORDER BY email) AS id, email, nome
            FROM emails_raw;
            """
        )
        conn.execute("DROP TABLE emails_raw;")
        conn.execute("ALTER TABLE emails_raw_id RENAME TO emails_raw;")
        conn.commit()

    max_id = conn.execute("SELECT MAX(id) FROM emails_raw;").fetchone()[0] or 0
    start = 1
    while start <= max_id and not stop_event.is_set():
        end = min(start + ROWS_PER_FINAL_FILE - 1, max_id)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        final_file = EXPORT_DIR / f"Trader_Emails_{file_num:03d}_{ts}.parquet"
        sql = (
            f"COPY (SELECT email, nome FROM emails_raw WHERE id BETWEEN {start} AND {end}) "
            f"TO '{final_file}' (FORMAT PARQUET, COMPRESSION SNAPPY);"
        )
        try:
            conn.execute(sql)
            conn.commit()
            final_files.append(final_file)
            conn.execute(f"DELETE FROM emails_raw WHERE id BETWEEN {start} AND {end};")
            conn.commit()
            file_num += 1
            start = end + 1
        except Exception as e:
            logger.error(f"{E['error']} Export {start}-{end}: {e}")
            break
    return final_files


def phase7_upload(api, token, emails_repo, checkpoint_repo, final_files, db_path, state):
    logger.info(f"\n{'='*100}\n{E['upload']} FASE 7: Upload\n{'='*100}\n")
    for f in final_files:
        if stop_event.is_set():
            break
        if hf_upload_file(api, token, emails_repo, f, f"Trader_Emails/{f.name}"):
            try:
                f.unlink()
            except Exception:
                pass
    hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")
    if db_path.exists():
        hf_upload_file(api, token, checkpoint_repo, db_path, "emails.duckdb")
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    state["schema_version"] = "v8_email_nome"
    save_state(state)


def main():
    logger.info(f"\n{'#'*100}")
    logger.info("# MINERADOR V8 — email + nome, dedup real, filtros de qualidade")
    logger.info(f"{'#'*100}\n")

    if not HF_TOKEN:
        sys.exit(2)

    start_resource_monitor(10)
    api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    # Opcional: apagar DB antigo se quiser recomeçar limpo — descomente:
    # if DB_PATH.exists(): DB_PATH.unlink()

    state = load_state()
    conn = init_duckdb(DB_PATH)
    session = create_libtorrent_session()

    try:
        t0 = time.time()
        completed = phase1_download_torrents(session, MAGNETS)
        if not completed:
            return
        tars = phase2_wait_downloads(completed, state)
        if tars:
            phase3_process_tars(tars, state, conn)
        if not stop_event.is_set():
            total_unique = phase5_deduplicate(conn)
            files = phase6_export(conn)
            phase7_upload(api, HF_TOKEN, emails_repo, checkpoint_repo, files, DB_PATH, state)
            logger.info(
                f"\n{E['ok']} SUCESSO | { (time.time()-t0)/60:.1f} min | "
                f"únicos: {total_unique:,} | brutas inseridas: {get_email_counter():,}\n"
            )
    except KeyboardInterrupt:
        logger.warning("Interrompido")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        stop_event.set()


if __name__ == "__main__":
    main()
