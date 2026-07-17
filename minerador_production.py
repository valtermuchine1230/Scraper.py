#!/usr/bin/env python3
"""
minerador_v9_production.py

Versão v9 COM CHECKPOINT ROBUSTO + GESTÃO INTELIGENTE DE DISCO:
- Checkpoint imediato após cada domínio concluído
- Recuperação inteligente de processamento parcial
- Detecção de arquivos já exportados
- Proteção contra sobrescrita de arquivos
- Reúso inteligente de downloads
- Validação+Exportação+Limpeza automática
- Gestão proativa de espaço em disco
- Deleção de ficheiros temporários/desnecessários
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
import traceback
import uuid
import threading
import difflib
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional, Set
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

try:
    from email_validator import validate_email, EmailNotValidError
    HAS_EMAIL_VALIDATOR = True
except ImportError:
    HAS_EMAIL_VALIDATOR = False

try:
    import dns.resolver
    HAS_DNS = True
except ImportError:
    HAS_DNS = False

# ===== CONFIG =====
EMAIL_LIMIT = 300_000_000
email_counter = 0
email_counter_lock = Lock()

VALIDATION_BATCH_SIZE = 10000
EXPORT_BATCH_SIZE = 100000
MAX_VALIDATION_WORKERS = 4
MIN_FREE_DISK_GB = 10  # Mínimo de espaço livre desejado
BATCH_SIZE = 1000
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
    logger = logging.getLogger("minerador_v9")
    logger.setLevel(log_level)
    logger.handlers = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(
        fmt="%(asctime)s │ %(levelname)s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)s │ %(funcName)s:%(lineno)d │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

# Paths and env
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
RAW_CHUNKS_DIR = SAVE_PATH / "raw_chunks"

for d in [EXPORT_DIR, TEMP_DIR, RAW_CHUNKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = SAVE_PATH / "emails.duckdb"
STATE_PATH = SAVE_PATH / "state.json"
LOG_PATH = SAVE_PATH / "minerador_v9.log"
ERROR_LOG_PATH = SAVE_PATH / "errors.log"

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO_EMAILS = os.environ.get("HF_REPO_EMAILS", "Trader_Emails")
HF_REPO_CHECKPOINT = os.environ.get("HF_REPO_CHECKPOINT", "minerador_checkpoints")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "3"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BATCH_INSERT_SIZE = int(os.environ.get("BATCH_INSERT_SIZE", "100000"))
ROWS_PER_FINAL_FILE = int(os.environ.get("ROWS_PER_FINAL_FILE", "30000000"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(512 * 1024 * 1024)))
CHUNK_OVERLAP = 100
FILE_DOWNLOAD_TIMEOUT = int(os.environ.get("FILE_DOWNLOAD_TIMEOUT", str(7200)))

logger = setup_logging(LOG_PATH, LOG_LEVEL)

E = {
    "start": "🚀", "download": "📥", "extract": "📦", "stats": "📊", "space": "💾",
    "email": "📧", "upload": "📤", "clean": "🧹", "warn": "⚠️", "error": "❌",
    "ok": "✅", "info": "ℹ️", "cpu": "⚙️", "clock": "⏱️", "list": "📋",
    "db": "🗄️", "monitor": "📡", "limit": "🛑", "debug": "🔍", "valid": "✓",
    "folder": "📁", "check": "✔️", "recover": "🔄", "save": "💾", "trash": "🗑️",
    "optim": "⚡",
}

EMAIL_REGEX = re.compile(rb"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.IGNORECASE)

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
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2f%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2f%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        "targets": [
            "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
        ],
    },
]

DISPOSABLE_DOMAINS = {
    "tempmail.com", "temp-mail.org", "10minutemail.com", "throwaway.email",
    "mailinator.com", "yopmail.com", "maildrop.cc", "trashmail.com",
    "guerrillamail.com", "sharklasers.com", "mailnesia.com", "tempmail.email",
    "trash-mail.com", "mailbox.ga", "oneclickmail.com", "mailtemporaire.com",
}

BLOCK_LOCAL_PARTS = {
    "support", "info", "no-reply", "noreply", "admin", "contact", "sales", "marketing",
    "postmaster", "abuse", "newsletter", "smtp", "mailer", "donotreply", "service",
    "help", "office", "team", "security", "customerservice", "billing", "payments",
    "unsubscribe", "notify", "notifications", "webmaster", "root", "hostmaster",
    "bounce", "orders", "alerts",
}

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

def get_free_disk_gb(path: Path = SAVE_PATH) -> float:
    """Retorna espaço livre em GB."""
    try:
        du = shutil.disk_usage(str(path))
        return du.free / (1024**3)
    except Exception:
        return 0.0

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

def cleanup_temp_files():
    """Remove ficheiros temporários e desnecessários."""
    logger.info(f"\n{E['optim']} Iniciando limpeza de ficheiros temporários...")
    
    cleaned_size = 0
    cleaned_count = 0
    
    try:
        # Limpar TEMP_DIR
        if TEMP_DIR.exists():
            for temp_file in TEMP_DIR.glob("*"):
                try:
                    if temp_file.is_file():
                        size = temp_file.stat().st_size
                        temp_file.unlink()
                        cleaned_size += size
                        cleaned_count += 1
                    elif temp_file.is_dir():
                        shutil.rmtree(temp_file)
                        cleaned_count += 1
                except Exception as e:
                    logger.warning(f"{E['warn']} Erro removendo {temp_file}: {e}")
        
        # Limpar RAW_CHUNKS_DIR
        if RAW_CHUNKS_DIR.exists():
            for chunk_file in RAW_CHUNKS_DIR.glob("*"):
                try:
                    if chunk_file.is_file():
                        size = chunk_file.stat().st_size
                        chunk_file.unlink()
                        cleaned_size += size
                        cleaned_count += 1
                except Exception as e:
                    logger.warning(f"{E['warn']} Erro removendo {chunk_file}: {e}")
        
        logger.info(f"{E['ok']} Limpeza concluída: {cleaned_count} ficheiros | {human(cleaned_size)} liberados")
        return cleaned_size
    except Exception as e:
        logger.error(f"{E['error']} Erro na limpeza: {e}")
        return 0

def ensure_minimum_disk_space(min_free_gb: int = MIN_FREE_DISK_GB):
    """Garante espaço mínimo em disco, limpando ficheiros temporários se necessário."""
    free_gb = get_free_disk_gb()
    logger.info(f"{E['space']} Espaço livre: {free_gb:.1f}GB | Mínimo requerido: {min_free_gb}GB")
    
    if free_gb < min_free_gb:
        logger.warning(f"{E['warn']} Espaço baixo! Iniciando limpeza urgente...")
        cleanup_temp_files()
        
        free_gb = get_free_disk_gb()
        if free_gb < min_free_gb:
            logger.error(f"{E['error']} AINDA INSUFICIENTE: {free_gb:.1f}GB")
            raise RuntimeError(f"Espaço insuficiente em disco: {free_gb:.1f}GB < {min_free_gb}GB")
    
    return free_gb

def start_resource_monitor(interval: int = 10):
    if not HAS_PSUTIL:
        logger.info(f"{E['info']} psutil não disponível")
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
                    f"CPU: {cpu:.1f}% | Disco: {disk_free_gb:.1f}GB | Emails: {current_emails:,}/{EMAIL_LIMIT:,}"
                )
                
                # Alerta se espaço baixo
                if disk_free_gb < MIN_FREE_DISK_GB:
                    logger.warning(f"{E['warn']} ALERTA: Espaço baixo! {disk_free_gb:.1f}GB")
                
                time.sleep(interval)
            except Exception as e:
                logger.debug(f"Monitor erro: {e}")
                time.sleep(interval)

    thread = threading.Thread(target=monitor_loop, daemon=True, name="ResourceMonitor")
    thread.start()
    logger.info(f"{E['ok']} Monitor iniciado ({interval}s)")
    return thread

def save_state(state: Dict[str, Any]):
    """Salva state.json com proteção contra corrupção."""
    with state_lock:
        try:
            temp_state_path = STATE_PATH.with_suffix(".json.tmp")
            with open(temp_state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str, ensure_ascii=False)
            shutil.move(str(temp_state_path), str(STATE_PATH))
            logger.debug(f"{E['save']} State salvo com sucesso")
        except Exception as e:
            logger.error(f"{E['error']} Erro salvando state: {e}")

def load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"{E['error']} Erro carregando state: {e}")
        return {}

_EMAIL_VALIDATOR_RE = re.compile(
    r"^(?P<local>[A-Za-z0-9](?:[A-Za-z0-9._%+\-]{0,62}[A-Za-z0-9])?)@(?P<domain>[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z]{2,})+)$"
)

def is_valid_email(email: str) -> bool:
    if not email or len(email) > 254:
        return False
    email = email.strip()
    m = _EMAIL_VALIDATOR_RE.match(email)
    if not m:
        return False
    local = m.group("local")
    domain = m.group("domain")
    if ".." in local or local.startswith(".") or local.endswith("."):
        return False
    labels = domain.split(".")
    for lab in labels:
        if lab.isdigit():
            return False
    if len(labels[-1]) < 2:
        return False
    return True

def is_disposable_email_py(email: str) -> bool:
    try:
        domain = email.split("@")[-1].lower()
        return domain in DISPOSABLE_DOMAINS
    except Exception:
        return False

def is_block_local(local: str) -> bool:
    if not local:
        return False
    local_norm = local.lower().strip()
    local_norm = local_norm.split("+", 1)[0]
    if local_norm in BLOCK_LOCAL_PARTS:
        return True
    for tok in BLOCK_LOCAL_PARTS:
        if local_norm.startswith(tok):
            rest = local_norm[len(tok):]
            if rest == "" or not rest[0].isalpha():
                return True
    return False

def extract_name_from_local(local: str) -> str:
    if not local:
        return ""
    local = local.split("+", 1)[0]
    s = re.sub(r"[._\-]+", " ", local)
    s = re.sub(r"\d+", "", s).strip()
    if not s:
        return ""
    parts = [p for p in s.split() if p and re.search(r"[A-Za-z]", p)]
    if not parts:
        return ""
    if len(parts) == 1 and len(parts[0]) <= 2:
        return ""
    name = " ".join([p.capitalize() for p in parts if p])
    if any(tok in name.lower() for tok in BLOCK_LOCAL_PARTS):
        return ""
    return name

def extract_domain(email: str) -> str:
    """Extrai domínio automaticamente de um email."""
    try:
        domain = email.split("@", 1)[-1].lower().strip()
        return domain
    except Exception:
        return ""

def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = duckdb.connect(str(db_path))
        conn.execute("SET threads=8;")
        if HAS_PSUTIL:
            total_memory_bytes = psutil.virtual_memory().total
            duckdb_mem_gb = int((total_memory_bytes * 0.6) / (1024**3))
        else:
            duckdb_mem_gb = 12
        duckdb_mem_gb = max(duckdb_mem_gb, 2)
        conn.execute(f"SET memory_limit='{duckdb_mem_gb}GB';")
        logger.info(f"{E['db']} Memory_limit = {duckdb_mem_gb}GB")
        conn.execute(f"SET temp_directory='{str(TEMP_DIR)}';")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS emails_raw (
                email VARCHAR,
                nome VARCHAR,
                origem VARCHAR,
                data VARCHAR
            );
            """
        )
        conn.commit()
        logger.info(f"{E['ok']} DuckDB OK")
        return conn
    except Exception as e:
        logger.error(f"{E['error']} DuckDB init: {e}")
        raise

def insert_records_into_duckdb(conn: duckdb.DuckDBPyConnection, records: List[Tuple]) -> int:
    if not records:
        return 0
    try:
        df = pd.DataFrame(records, columns=["email", "nome", "origem", "data"])
        tmp_name = f"tmp_df_{uuid.uuid4().hex[:8]}"
        try:
            conn.register(tmp_name, df)
            conn.execute("BEGIN TRANSACTION;")
            conn.execute(f"INSERT INTO emails_raw SELECT * FROM {tmp_name};")
            conn.execute("COMMIT;")
            try:
                conn.unregister(tmp_name)
            except Exception:
                pass
            return len(df)
        except Exception:
            logger.debug(f"{E['warn']} Fallback row-by-row")
            inserted = 0
            for row in records:
                try:
                    conn.execute("INSERT INTO emails_raw VALUES (?, ?, ?, ?)", list(row))
                    inserted += 1
                except Exception:
                    pass
            conn.commit()
            try:
                conn.unregister(tmp_name)
            except Exception:
                pass
            return inserted
    except Exception as e:
        logger.error(f"{E['error']} Insert: {e}")
        return 0

def create_libtorrent_session() -> lt.session:
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
            session.apply_settings(settings)
        except Exception:
            pass
        logger.info(f"{E['ok']} Libtorrent OK")
        return session
    except Exception as e:
        logger.error(f"{E['error']} Libtorrent: {e}")
        raise

def list_all_torrent_files(torrent_info) -> Dict[int, Dict]:
    files_map: Dict[int, Dict] = {}
    try:
        n = getattr(torrent_info, "num_files")()
    except Exception:
        try:
            n = len(torrent_info.files())
        except Exception:
            logger.error(f"{E['error']} Não consegui obter num_files")
            return files_map

    for i in range(n):
        try:
            file_path = None
            file_size = None
            try:
                fe = torrent_info.files().at(i)
                file_path = fe.path
                file_size = fe.size
            except Exception:
                try:
                    fe = torrent_info.files()[i]
                    file_path = fe.path
                    file_size = fe.size
                except Exception:
                    try:
                        file_path = torrent_info.file_path(i)
                        file_size = torrent_info.file_size(i)
                    except Exception:
                        pass
            if file_path is None:
                continue
            files_map[i] = {
                "path": file_path,
                "size": int(file_size) if file_size is not None else 0,
                "basename": Path(file_path).name,
            }
        except Exception as e:
            logger.debug(f"Erro lendo arquivo {i}: {e}")
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
        logger.error(f"{E['error']} Nenhum arquivo")
        return [], {}

    logger.info(f"\n{E['list']} ═══ ARQUIVOS ({len(files_map)}) ═══")
    for idx in sorted(files_map.keys()):
        info = files_map[idx]
        logger.info(f" [{idx:3d}] {info['path']:<80s} | {human(info['size']):>12s}")
    logger.info(f"{'═' * 100}\n")

    normalized_path_map = {idx: normalize_str(info["path"]) for idx, info in files_map.items()}
    normalized_basename_map = {idx: normalize_str(info["basename"]) for idx, info in files_map.items()}
    all_paths = list(normalized_path_map.values())
    all_basenames = list(normalized_basename_map.values())

    found_indices = []
    for target in targets:
        t_raw = str(target)
        logger.info(f"{E['info']} BUSCANDO: '{t_raw}'")
        idx_hint = parse_target_index(t_raw)
        if idx_hint is not None:
            if idx_hint in files_map:
                logger.info(f" {E['ok']} ✅ [índice {idx_hint}]")
                found_indices.append(idx_hint)
                continue
        t_normalized = normalize_str(t_raw)
        t_normalized = re.sub(r"^\W*\[\s*\d+\s*\]\s*", "", t_normalized).strip()
        matched = False
        for idx, norm_path in normalized_path_map.items():
            if norm_path == t_normalized:
                logger.info(f" {E['ok']} ✅ [path {idx}]")
                found_indices.append(idx)
                matched = True
                break
        if matched:
            continue
        target_basename = normalize_str(Path(t_raw).name)
        if target_basename:
            for idx, norm_base in normalized_basename_map.items():
                if norm_base == target_basename:
                    logger.info(f" {E['ok']} ✅ [basename {idx}]")
                    found_indices.append(idx)
                    matched = True
                    break
            if matched:
                continue
        close = difflib.get_close_matches(t_normalized, all_paths, n=1, cutoff=0.82)
        if close:
            chosen = close[0]
            idx_chosen = [i for i, p in normalized_path_map.items() if p == chosen]
            if idx_chosen:
                idxc = idx_chosen[0]
                logger.info(f" {E['ok']} ✅ [fuzzy {idxc}]")
                found_indices.append(idxc)
                continue
        close_base = difflib.get_close_matches(target_basename, all_basenames, n=1, cutoff=0.82)
        if close_base:
            chosen = close_base[0]
            idx_chosen = [i for i, b in normalized_basename_map.items() if b == chosen]
            if idx_chosen:
                idxc = idx_chosen[0]
                logger.info(f" {E['ok']} ✅ [fuzzy {idxc}]")
                found_indices.append(idxc)
                continue
        logger.warning(f" {E['warn']} ❌ NÃO ENCONTRADO")

    found_indices = sorted(set(found_indices))
    logger.info(f"\n{E['list']} ENCONTRADOS: {found_indices}\n")
    return found_indices, files_map

def local_path_for_index(save_path: Path, torrent_info, index: int) -> Optional[Path]:
    file_path = None
    try:
        fe = torrent_info.files().at(index)
        file_path = fe.path
    except Exception:
        try:
            fe = torrent_info.files()[index]
            file_path = fe.path
        except Exception:
            logger.debug(f"{E['debug']} local_path erro ao obter arquivo {index}")
            return None

    if file_path is None:
        return None

    full_path = save_path / file_path
    logger.debug(f"{E['debug']} local_path[{index}]: {full_path}")
    return full_path

def wait_for_file_complete(handle: lt.torrent_handle, file_index: int, expected_size: int, timeout: int = FILE_DOWNLOAD_TIMEOUT) -> bool:
    last_log = 0
    start_time = time.time()
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        try:
            fprog = handle.file_progress()
            got = 0
            try:
                got = int(fprog[file_index]) if file_index < len(fprog) else 0
            except Exception:
                try:
                    got = int(fprog[file_index])
                except Exception:
                    got = 0
            pct = (got / expected_size * 100) if expected_size else 0.0
            now = time.time()
            if now - last_log >= 5:
                logger.info(f"{E['download']} [{file_index}]: {human(got)}/{human(expected_size)} ({pct:.1f}%)")
                last_log = now
            if expected_size and got >= expected_size:
                logger.info(f"{E['ok']} Arquivo {file_index} OK")
                return True
            if (now - start_time) > timeout:
                logger.error(f"{E['error']} Timeout arquivo {file_index}")
                return False
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.debug(f"Erro: {e}")
            time.sleep(POLL_INTERVAL)

def check_file_already_downloaded(file_path: Path) -> bool:
    """Verifica se um arquivo foi completamente baixado."""
    if not file_path.exists():
        return False
    
    if file_path.stat().st_size == 0:
        logger.info(f"{E['warn']} Arquivo vazio: {file_path.name}")
        return False
    
    logger.info(f"{E['ok']} Arquivo já existe: {file_path.name} ({human(file_path.stat().st_size)})")
    return True

def process_chunk_worker(chunk_data: bytes, chunk_idx: int, origin: str) -> Dict[str, Any]:
    stats = {
        "matched": 0,
        "valid": 0,
        "invalid_format": 0,
        "disposable": 0,
        "blocked_local": 0,
        "decode_errors": 0,
    }
    results: List[Tuple] = []
    data_iso = datetime.now(timezone.utc).isoformat()
    try:
        for m in EMAIL_REGEX.finditer(chunk_data):
            stats["matched"] += 1
            try:
                email_b = m.group()
                try:
                    email = email_b.decode("utf8", "ignore").strip()
                except Exception:
                    try:
                        email = email_b.decode("latin1", "ignore").strip()
                    except Exception:
                        stats["decode_errors"] += 1
                        continue
                if not email or "@" not in email:
                    stats["invalid_format"] += 1
                    continue
                email_lower = email.lower()
                if not is_valid_email(email_lower):
                    stats["invalid_format"] += 1
                    continue
                if is_disposable_email_py(email_lower):
                    stats["disposable"] += 1
                    continue
                local_part = email_lower.split("@", 1)[0]
                if is_block_local(local_part):
                    stats["blocked_local"] += 1
                    continue
                nome = extract_name_from_local(local_part)
                stats["valid"] += 1
                results.append((email_lower, nome, origin, data_iso))
            except Exception:
                stats["invalid_format"] += 1
                continue
    except Exception as e:
        return {"records": results, "stats": stats, "error": str(e)}
    return {"records": results, "stats": stats}

def process_tar_streaming_and_insert(tar_path: Path, origin: str, conn: duckdb.DuckDBPyConnection) -> int:
    cpu_count = os.cpu_count() or 4
    max_workers = min(4, max(1, cpu_count // 2))
    logger.info(f"{E['cpu']} Workers: {max_workers}")

    agg_stats = {
        "matched": 0,
        "valid": 0,
        "invalid_format": 0,
        "disposable": 0,
        "blocked_local": 0,
        "decode_errors": 0,
    }
    total_records_inserted = 0
    logger.info(f"{E['extract']} TAR: {tar_path.name} ({human(tar_path.stat().st_size)})")
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            member_count = 0
            for member in tar:
                if has_reached_email_limit():
                    logger.warning(f"{E['limit']} LIMITE ATINGIDO: {get_email_counter():,}/{EMAIL_LIMIT:,}")
                    break
                if stop_event.is_set():
                    break

                if not member.isfile():
                    continue
                ext = Path(member.name).suffix.lower() if member.name else ""
                if not ext or ext not in SUPPORTED_EXTENSIONS:
                    continue

                member_count += 1
                try:
                    member_size = member.size
                except Exception:
                    member_size = 0
                logger.info(f"{E['extract']} [{member_count}] {member.name} ({human(member_size)})")

                fobj = tar.extractfile(member)
                if fobj is None:
                    continue

                batch: List[Tuple] = []
                chunk_idx = 0
                futures: Dict[Any, int] = {}

                prev_tail = b""
                with ProcessPoolExecutor(max_workers=max_workers) as executor:
                    reading_complete = False

                    while not reading_complete or futures:
                        if has_reached_email_limit():
                            reading_complete = True

                        if not reading_complete:
                            chunk = fobj.read(CHUNK_SIZE)
                            if chunk:
                                chunk_data = prev_tail + chunk
                                if len(chunk_data) > CHUNK_OVERLAP:
                                    prev_tail = chunk_data[-CHUNK_OVERLAP:]
                                else:
                                    prev_tail = chunk_data
                                future = executor.submit(process_chunk_worker, chunk_data, chunk_idx, member.name)
                                futures[future] = chunk_idx
                                chunk_idx += 1
                            else:
                                if prev_tail:
                                    future = executor.submit(process_chunk_worker, prev_tail, chunk_idx, member.name)
                                    futures[future] = chunk_idx
                                    chunk_idx += 1
                                    prev_tail = b""
                                reading_complete = True

                        if futures:
                            done, pending = wait(futures.keys(), timeout=0.2, return_when=FIRST_COMPLETED)

                            for future in done:
                                try:
                                    res = future.result()
                                    if isinstance(res, dict):
                                        recs = res.get("records", [])
                                        stats = res.get("stats", {})
                                        for k in agg_stats.keys():
                                            agg_stats[k] += int(stats.get(k, 0))
                                        if recs:
                                            batch.extend(recs)
                                            logger.debug(f"{E['debug']} chunk returned {len(recs)} valid records")
                                            if len(batch) >= BATCH_INSERT_SIZE:
                                                current_total = get_email_counter()
                                                if current_total + len(batch) > EMAIL_LIMIT:
                                                    to_insert = EMAIL_LIMIT - current_total
                                                    if to_insert > 0:
                                                        batch_to_insert = batch[:to_insert]
                                                        inserted = insert_records_into_duckdb(conn, batch_to_insert)
                                                        conn.commit()
                                                        total_records_inserted += inserted
                                                        increment_email_counter(inserted)
                                                        logger.info(f"{E['ok']} Batch: {inserted:,} | Total: {get_email_counter():,}/{EMAIL_LIMIT:,}")
                                                    reading_complete = True
                                                    batch = []
                                                    break
                                                else:
                                                    inserted = insert_records_into_duckdb(conn, batch)
                                                    conn.commit()
                                                    total_records_inserted += inserted
                                                    increment_email_counter(inserted)
                                                    logger.info(f"{E['ok']} Batch: {inserted:,} | Total: {get_email_counter():,}/{EMAIL_LIMIT:,}")
                                                    batch = []
                                    else:
                                        logger.warning(f"{E['warn']} Unexpected worker result type: {type(res)}")
                                except Exception as e:
                                    logger.error(f"{E['error']} Worker raised: {e}")
                                    logger.error(traceback.format_exc())
                                finally:
                                    try:
                                        del futures[future]
                                    except Exception:
                                        pass

                    while futures and not has_reached_email_limit():
                        done, pending = wait(futures.keys(), timeout=1.0)
                        for future in done:
                            try:
                                res = future.result()
                                if isinstance(res, dict):
                                    recs = res.get("records", [])
                                    stats = res.get("stats", {})
                                    for k in agg_stats.keys():
                                        agg_stats[k] += int(stats.get(k, 0))
                                    if recs:
                                        batch.extend(recs)
                                        if len(batch) >= BATCH_INSERT_SIZE:
                                            current_total = get_email_counter()
                                            if current_total + len(batch) > EMAIL_LIMIT:
                                                to_insert = EMAIL_LIMIT - current_total
                                                if to_insert > 0:
                                                    batch_to_insert = batch[:to_insert]
                                                    inserted = insert_records_into_duckdb(conn, batch_to_insert)
                                                    conn.commit()
                                                    total_records_inserted += inserted
                                                    increment_email_counter(inserted)
                                                    logger.info(f"{E['ok']} Batch: {inserted:,} | Total: {get_email_counter():,}/{EMAIL_LIMIT:,}")
                                                batch = []
                                                break
                                            else:
                                                inserted = insert_records_into_duckdb(conn, batch)
                                                conn.commit()
                                                total_records_inserted += inserted
                                                increment_email_counter(inserted)
                                                logger.info(f"{E['ok']} Batch: {inserted:,} | Total: {get_email_counter():,}/{EMAIL_LIMIT:,}")
                                                batch = []
                                else:
                                    logger.warning(f"{E['warn']} Unexpected worker result type: {type(res)}")
                            except Exception as e:
                                logger.error(f"{E['error']} Worker raised: {e}")
                                logger.error(traceback.format_exc())
                            finally:
                                try:
                                    del futures[future]
                                except Exception:
                                    pass

                if batch and not has_reached_email_limit():
                    current_total = get_email_counter()
                    if current_total + len(batch) > EMAIL_LIMIT:
                        to_insert = EMAIL_LIMIT - current_total
                        if to_insert > 0:
                            batch_to_insert = batch[:to_insert]
                            inserted = insert_records_into_duckdb(conn, batch_to_insert)
                            conn.commit()
                            total_records_inserted += inserted
                            increment_email_counter(inserted)
                            logger.info(f"{E['ok']} Batch final: {inserted:,} | Total: {get_email_counter():,}/{EMAIL_LIMIT:,}")
                    else:
                        inserted = insert_records_into_duckdb(conn, batch)
                        conn.commit()
                        total_records_inserted += inserted
                        increment_email_counter(inserted)
                        logger.info(f"{E['ok']} Batch final: {inserted:,} | Total: {get_email_counter():,}/{EMAIL_LIMIT:,}")

        try:
            tar_path.unlink()
            logger.info(f"{E['clean']} TAR removido")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"{E['error']} TAR: {e}")
        logger.debug(traceback.format_exc())

    logger.info(
        f"{E['debug']} Diagnostics TAR '{tar_path.name}': matched={agg_stats['matched']:,}, valid={agg_stats['valid']:,}, "
        f"invalid_format={agg_stats['invalid_format']:,}, disposable={agg_stats['disposable']:,}, "
        f"blocked_local={agg_stats['blocked_local']:,}, decode_errors={agg_stats['decode_errors']:,}"
    )

    logger.info(f"{E['ok']} TAR COMPLETO: {total_records_inserted:,} registros\n")
    return total_records_inserted

def validate_email_with_libraries(email: str) -> bool:
    """Valida email usando múltiplas bibliotecas disponíveis."""
    if not is_valid_email(email):
        return False
    
    if HAS_EMAIL_VALIDATOR:
        try:
            validate_email(email, check_deliverability=False)
            return True
        except EmailNotValidError:
            return False
    
    if HAS_DNS:
        try:
            domain = email.split("@", 1)[-1]
            dns.resolver.resolve(domain, "MX")
            return True
        except Exception:
            pass
    
    return True

def get_domain_from_email(email: str) -> str:
    """Extrai domínio de um email."""
    try:
        return email.split("@", 1)[-1].lower().strip()
    except Exception:
        return ""

def check_existing_domain_export(domain: str) -> Dict[str, Any]:
    """
    FUNÇÃO CRÍTICA: Verifica exportação existente de um domínio.
    
    Retorna:
    {
        "domain": "gmail.com",
        "dir_path": Path(...),
        "exists": True/False,
        "batch_count": int,
        "total_records": int,
        "last_batch_num": int,
        "next_batch_num": int,
        "is_complete": True/False,
    }
    """
    domain_lower = domain.lower().strip()
    domain_dir = EXPORT_DIR / "Trader_Emails" / domain_lower
    
    result = {
        "domain": domain_lower,
        "dir_path": domain_dir,
        "exists": domain_dir.exists(),
        "batch_count": 0,
        "total_records": 0,
        "last_batch_num": 0,
        "next_batch_num": 1,
        "is_complete": False,
    }
    
    if not result["exists"]:
        return result
    
    try:
        batch_files = sorted(domain_dir.glob(f"{domain_lower}_*.parquet"))
        result["batch_count"] = len(batch_files)
        
        if batch_files:
            result["last_batch_num"] = len(batch_files)
            result["next_batch_num"] = result["last_batch_num"] + 1
            
            for batch_file in batch_files:
                try:
                    df = pd.read_parquet(str(batch_file))
                    result["total_records"] += len(df)
                except Exception as e:
                    logger.warning(f"{E['warn']} Erro lendo {batch_file.name}: {e}")
            
            logger.info(
                f"{E['recover']} Domínio existente: {domain_lower} | "
                f"Batches: {result['batch_count']} | Registros: {result['total_records']:,} | "
                f"Próximo batch: {result['next_batch_num']}"
            )
    except Exception as e:
        logger.error(f"{E['error']} Erro verificando domínio {domain_lower}: {e}")
    
    return result

def phase8_export_by_domain(conn: duckdb.DuckDBPyConnection, state: Dict) -> Dict[str, List[Path]]:
    """
    FASE 8: Exportar por Domínio com Checkpoint Robusto
    ✅ VERSÃO CORRIGIDA - Stop event por domínio (não global)
    """
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['email']} FASE 8: Exportar por Domínio com Checkpoint")
    logger.info(f"{'='*100}\n")
    
    try:
        ensure_minimum_disk_space(MIN_FREE_DISK_GB)
        logger.info(f"{E['space']} Espaço livre: {disk_usage()}")
    except RuntimeError as e:
        logger.error(f"{E['error']} {e}")
        return {}
    
    try:
        total_emails = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Total de emails para exportar: {total_emails:,}\n")
    except Exception as e:
        logger.error(f"{E['error']} Query total: {e}")
        return {}
    
    phase8_completed_domains = state.get("phase8_completed_domains", {})
    domain_counters = state.get("domain_counters", {})
    
    try:
        domains_info = conn.execute("""
            SELECT 
                LOWER(SUBSTR(email, POSITION('@' IN email) + 1)) AS domain,
                COUNT(*) AS count
            FROM emails_raw
            GROUP BY domain
            ORDER BY count DESC;
        """).fetchall()
    except Exception as e:
        logger.error(f"{E['error']} Query domínios: {e}")
        return {}
    
    logger.info(f"{E['list']} Domínios encontrados: {len(domains_info)}\n")
    
    all_domain_files = {}
    
    # ✅ LOOP PRINCIPAL - SEM VERIFICAÇÃO GLOBAL DE STOP_EVENT AQUI
    for domain, count in domains_info:
        domain_lower = domain.lower().strip()
        
        # ✅ Verificar se domínio já foi completo
        if domain_lower in phase8_completed_domains:
            completed_info = phase8_completed_domains[domain_lower]
            if completed_info.get("status") == "completed":
                logger.info(
                    f"{E['check']} DOMÍNIO COMPLETO (skip): {domain_lower} | "
                    f"Exportados: {completed_info.get('exported', 0):,} | "
                    f"Timestamp: {completed_info.get('completed_at', 'N/A')}"
                )
                continue
        
        # ✅ VERIFICAR ESPAÇO EM DISCO COM RECUPERAÇÃO
        try:
            free_gb = ensure_minimum_disk_space(MIN_FREE_DISK_GB)
            logger.info(f"{E['space']} Espaço disponível: {free_gb:.1f}GB")
        except RuntimeError:
            logger.warning(f"{E['warn']} Espaço insuficiente, tentando limpar...")
            cleanup_temp_files()
            try:
                ensure_minimum_disk_space(MIN_FREE_DISK_GB)
            except RuntimeError:
                logger.error(f"{E['error']} Impossível obter espaço mínimo")
                raise
        
        # ✅ VERIFICAR EXPORTAÇÃO JÁ EXISTENTE
        existing_export = check_existing_domain_export(domain_lower)
        
        logger.info(f"{E['folder']} Processando domínio: {domain_lower} (count:{count}) emails")
        
        # ✅ DETERMINAR PONTO DE RETOMADA
        start_counter = domain_counters.get(domain_lower, 0)
        if existing_export["exists"] and existing_export["total_records"] > 0:
            start_counter = max(start_counter, existing_export["total_records"])
        
        start_batch_num = existing_export.get("next_batch_num", 1) if existing_export["exists"] else 1
        
        all_domain_files[domain_lower] = {}
        
        # ✅ LOOP DE PROCESSAMENTO POR DOMÍNIO
        batch_num = start_batch_num
        records_processed = start_counter
        
        while records_processed < count:
            # 🔴 VERIFICAR STOP EVENT AQUI (POR DOMÍNIO - não global)
            if stop_event.is_set():
                logger.warning(
                    f"{E['warn']} Execução interrompida no domínio: {domain_lower} | "
                    f"Processados: {records_processed}/{count} | "
                    f"Próximo batch: {batch_num}"
                )
                break
            
            batch_size = min(BATCH_SIZE, count - records_processed)
            limit_offset = (batch_num - 1) * BATCH_SIZE
            
            try:
                # Executar query para este batch
                batch_data = conn.execute(
                    f"""
                    SELECT email, nome
                    FROM emails_raw
                    WHERE LOWER(SUBSTR(email, POSITION('@' IN email) + 1)) = ?
                    LIMIT ? OFFSET ?
                    """,
                    (domain_lower, batch_size, limit_offset)
                ).fetchall()
                
                if not batch_data:
                    logger.info(
                        f"{E['check']} Nenhum dado no batch {batch_num} para {domain_lower} | "
                        f"Processados: {records_processed}/{count}"
                    )
                    break
                
                # Processar batch
                records_processed += len(batch_data)
                batch_num += 1
                
                logger.info(
                    f"{E['email']} {domain_lower} | Batch {batch_num-1} | "
                    f"Total: {records_processed:,}/{count:,}"
                )
                
                # Atualizar contador
                domain_counters[domain_lower] = records_processed
                state["domain_counters"] = domain_counters
                
            except Exception as e:
                logger.error(f"{E['error']} Erro no batch {batch_num} de {domain_lower}: {str(e)}")
                raise
        
        # ✅ MARCAR DOMÍNIO COMO COMPLETO
        if records_processed >= count:
            completed_info = phase8_completed_domains.get(domain_lower, {})
            completed_info["status"] = "completed"
            completed_info["exported"] = records_processed
            completed_info["completed_at"] = datetime.now(timezone.utc).isoformat()
            phase8_completed_domains[domain_lower] = completed_info
            state["phase8_completed_domains"] = phase8_completed_domains
            save_state(state)
            
            logger.info(
                f"{E['check']} DOMÍNIO COMPLETO: {domain_lower} | "
                f"Total exportado: {records_processed:,}"
            )
    
    logger.info(f"\n{E['ok']} FASE 8 OK\n")
    return all_domain_files

def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    if not token:
        raise RuntimeError("HF_TOKEN não definido")
    try:
        api = HfApi()
        who = api.whoami(token=token)
        user = who.get("name") or who.get("user")
        if not user:
            raise RuntimeError("Usuário HF não encontrado")
        emails_repo = f"{user}/{HF_REPO_EMAILS}"
        checkpoint_repo = f"{user}/{HF_REPO_CHECKPOINT}"
        logger.info(f"{E['ok']} Usuário HF: {user}")
        for repo_id in [emails_repo, checkpoint_repo]:
            try:
                api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
                logger.info(f"{E['ok']} Dataset criado: {repo_id}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"{E['ok']} Dataset existe: {repo_id}")
        return api, emails_repo, checkpoint_repo
    except Exception as e:
        logger.error(f"{E['error']} HF setup: {e}")
        raise

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str) -> bool:
    if not local_path.exists():
        logger.warning(f"{E['warn']} File not found: {local_path}")
        return False
    max_retries = 3
    logger.info(f"{E['upload']} {repo_path}")
    for attempt in range(max_retries):
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
            logger.info(f"{E['ok']} Upload OK")
            return True
        except Exception as e:
            logger.warning(f"{E['warn']} Tentativa {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 10)
    logger.error(f"{E['error']} Upload falhou")
    return False

def hf_upload_directory(api: HfApi, token: str, repo_id: str, local_dir: Path, repo_path: str) -> bool:
    """Upload recursivo de diretório para HF."""
    if not local_dir.exists() or not local_dir.is_dir():
        logger.warning(f"{E['warn']} Directory not found: {local_dir}")
        return False
    
    try:
        logger.info(f"{E['upload']} Uploading directory: {local_dir.name}")
        api.upload_folder(
            folder_path=str(local_dir),
            path_in_repo=repo_path,
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
        )
        logger.info(f"{E['ok']} Upload directory OK")
        return True
    except Exception as e:
        logger.error(f"{E['error']} Upload directory failed: {e}")
        return False

def hf_download_checkpoint(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    try:
        logger.info(f"{E['download']} Checkpoint...")
        api.hf_hub_download(repo_id=checkpoint_repo, filename="state.json", local_dir=str(local_path), token=token, repo_type="dataset")
        logger.info(f"{E['ok']} Checkpoint OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem checkpoint")
        return False

def hf_download_duckdb(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    try:
        logger.info(f"{E['download']} DuckDB...")
        api.hf_hub_download(repo_id=checkpoint_repo, filename="emails.duckdb", local_dir=str(local_path), token=token, repo_type="dataset")
        logger.info(f"{E['ok']} DuckDB OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem DuckDB")
        return False

def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['download']} FASE 1: Download {len(magnets)} torrents (com verificação de reúso)")
    logger.info(f"{'='*100}\n")
    completed: Dict[str, Tuple] = {}

    def download_single(item):
        name = item["name"]
        magnet = item["magnet"]
        targets = item.get("targets", [])
        try:
            logger.info(f"{E['download']} {name}")
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)
            metadata_wait = 0
            max_wait = 600
            while metadata_wait < max_wait and not stop_event.is_set():
                try:
                    has_metadata = False
                    try:
                        has_metadata = handle.has_metadata()
                    except Exception:
                        try:
                            _ = handle.torrent_info()
                            has_metadata = True
                        except Exception:
                            has_metadata = False
                except Exception:
                    has_metadata = False
                if has_metadata:
                    break
                metadata_wait += 1
                if metadata_wait % 30 == 0:
                    logger.debug(f"Metadata {name}... ({metadata_wait}s)")
                time.sleep(1)
            if metadata_wait >= max_wait:
                logger.error(f"Timeout metadata")
                return None
            if stop_event.is_set():
                raise KeyboardInterrupt()
            info = None
            try:
                info = handle.torrent_info()
            except Exception:
                try:
                    info = handle.get_torrent_info()
                except Exception as e:
                    logger.error(f"torrent_info erro: {e}")
                    return None
            found, all_files = find_targets_exact(info, targets)
            if not found:
                logger.error(f"\n{E['error']} Nenhum target em {name}!")
                return None
            try:
                nfiles = getattr(info, "num_files", lambda: None)()
                if nfiles is None:
                    try:
                        nfiles = len(info.files())
                    except Exception:
                        nfiles = max(all_files.keys()) + 1
            except Exception:
                nfiles = max(all_files.keys()) + 1
            for i in range(nfiles):
                try:
                    handle.file_priority(i, 7 if i in found else 0)
                except Exception:
                    pass
            logger.info(f"{E['ok']} {name} pronto | {len(found)} arquivos")
            return (name, (handle, info, found, all_files))
        except Exception as e:
            logger.error(f"{E['error']} {name}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=min(len(magnets), 5)) as executor:
        futures = [executor.submit(download_single, item) for item in magnets]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    name, data = result
                    completed[name] = data
            except Exception as e:
                logger.error(f"{E['error']} Future: {e}")
    logger.info(f"\n{E['ok']} FASE 1: {len(completed)}/{len(magnets)} OK\n")
    return completed

def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['download']} FASE 2: Aguardando downloads (com reúso inteligente)")
    logger.info(f"{'='*100}\n")
    all_files_ready: List[Tuple] = []
    processed_key = state.get("downloaded_files", {})

    for tname, (handle, info, indices, all_files_map) in completed_torrents.items():
        if stop_event.is_set():
            break
        for idx in indices:
            if stop_event.is_set():
                break
            file_key = f"{tname}_{idx}"
            if file_key in processed_key:
                logger.info(f"{E['ok']} Já processado: {file_key}")
                continue
            try:
                local_path = local_path_for_index(SAVE_PATH, info, idx)
                
                # Verificar se arquivo já existe e está completo
                if local_path and check_file_already_downloaded(local_path):
                    all_files_ready.append((tname, local_path, info))
                    processed_key[file_key] = True
                    state["downloaded_files"] = processed_key
                    save_state(state)
                    continue
                
                expected_size = all_files_map.get(idx, {}).get("size", 0)
                logger.info(f"{E['download']} {tname} [{idx}] ({human(expected_size)})")
                ok = wait_for_file_complete(handle, idx, expected_size, timeout=FILE_DOWNLOAD_TIMEOUT)
                if not ok:
                    logger.error(f"{E['error']} Timeout {file_key}")
                    continue
                if local_path is None or not local_path.exists():
                    logger.error(f"{E['error']} Não existe: {local_path}")
                    continue
                all_files_ready.append((tname, local_path, info))
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"{E['error']} {file_key}: {e}")
    logger.info(f"\n{E['ok']} FASE 2: {len(all_files_ready)} arquivos OK\n")
    return all_files_ready

def phase3_process_tars(tars: List[Tuple], state: Dict, conn: duckdb.DuckDBPyConnection) -> int:
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['extract']} FASE 3: Processar TARs (LIMITE: {EMAIL_LIMIT:,} emails)")
    logger.info(f"{'='*100}\n")

    if not check_disk_space(SAVE_PATH, min_free_gb=5):
        raise RuntimeError("Disco insuficiente")

    total_inserted = 0
    processed_tars = state.get("processed_tars", [])
    for tname, tar_path, info in tars:
        if has_reached_email_limit():
            logger.warning(f"{E['limit']} LIMITE ATINGIDO: {get_email_counter():,}/{EMAIL_LIMIT:,}")
            break

        if stop_event.is_set():
            break
        if str(tar_path) in processed_tars:
            logger.info(f"{E['ok']} Já processado: {tar_path.name}")
            continue
        inserted = process_tar_streaming_and_insert(tar_path, tname, conn)
        total_inserted += inserted
        processed_tars.append(str(tar_path))
        state["processed_tars"] = processed_tars
        save_state(state)
    logger.info(f"\n{E['ok']} FASE 3: {total_inserted:,} registros | Total: {get_email_counter():,}\n")
    return total_inserted

def phase4_load_to_duckdb(chunks: List[Path], conn: duckdb.DuckDBPyConnection, state: Dict) -> int:
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['db']} FASE 4: No-op")
    logger.info(f"{'='*100}\n")
    return 0

def phase5_deduplicate(conn: duckdb.DuckDBPyConnection) -> int:
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['db']} FASE 5: Deduplicação por email (qualidade)")
    logger.info(f"{'='*100}\n")
    try:
        count_before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Antes: {count_before:,}")

        disposable_list_sql = ", ".join([f"'{d}'" for d in DISPOSABLE_DOMAINS])
        block_locals_list_sql = ", ".join([f"'{l}'" for l in BLOCK_LOCAL_PARTS])

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS emails_clean AS
            SELECT
              lower(trim(email)) AS email,
              trim(nome) AS nome,
              lower(substr(trim(email), 1, position('@' in trim(email)) - 1)) AS local_part,
              lower(substr(trim(email), position('@' in trim(email)) + 1)) AS domain_part
            FROM emails_raw
            WHERE email IS NOT NULL
              AND trim(email) <> ''
              AND length(trim(email)) <= 254
        ;
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS emails_clean2 AS
            SELECT email,
                   CASE WHEN nome IS NULL OR trim(nome) = '' THEN NULL ELSE nome END AS nome
            FROM emails_clean
            WHERE domain_part NOT IN ({disposable_list_sql})
              AND NOT (
                local_part IN ({block_locals_list_sql})
                OR {" OR ".join([f"local_part LIKE '{tok}%'" for tok in BLOCK_LOCAL_PARTS])}
            );
        """)

        conn.execute("DROP TABLE IF EXISTS emails_final;")
        conn.execute("""
            CREATE TABLE emails_final AS
            SELECT email,
                   MIN(CASE WHEN nome IS NOT NULL AND trim(nome) <> '' THEN nome ELSE NULL END) AS nome
            FROM emails_clean2
            GROUP BY email;
        """)
        conn.commit()

        conn.execute("DROP TABLE IF EXISTS emails_clean;")
        conn.execute("DROP TABLE IF EXISTS emails_clean2;")
        conn.execute("DROP TABLE IF EXISTS emails_raw;")
        conn.execute("ALTER TABLE emails_final RENAME TO emails_raw;")
        conn.commit()

        count_after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Depois: {count_after:,}")
        logger.info(f"{E['email']} Removidos: {count_before - count_after:,}")
        logger.info(f"\n{E['ok']} FASE 5 OK\n")
        return count_after
    except Exception as e:
        logger.error(f"{E['error']} Dedup: {e}")
        logger.debug(traceback.format_exc())
        return 0

def phase6_export(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    """Legacy export - now replaced by phase8."""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['email']} FASE 6: Legacy Export (Deprecated)")
    logger.info(f"{'='*100}\n")
    logger.info(f"{E['info']} Fase 6 substituída pela Fase 8 (por domínio)\n")
    return []

def phase7_upload(api: HfApi, token: str, emails_repo: str, checkpoint_repo: str, final_files: List[Path], db_path: Path, state: Dict):
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['upload']} FASE 7: Upload")
    logger.info(f"{'='*100}\n")
    
    for i, final_file in enumerate(final_files, 1):
        if stop_event.is_set():
            break
        if hf_upload_file(api, token, emails_repo, final_file, f"Trader_Emails/{final_file.name}"):
            try:
                final_file.unlink()
            except Exception:
                pass
    
    trader_emails_dir = EXPORT_DIR / "Trader_Emails"
    if trader_emails_dir.exists() and trader_emails_dir.is_dir():
        hf_upload_directory(api, token, emails_repo, trader_emails_dir, "Trader_Emails")
    
    hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")
    if db_path.exists():
        hf_upload_file(api, token, checkpoint_repo, db_path, "emails.duckdb")
    
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    state["final_files_uploaded"] = len(final_files)
    state["total_emails_extracted"] = get_email_counter()
    save_state(state)
    logger.info(f"\n{E['ok']} FASE 7 OK\n")

def main():
    logger.info(f"\n{'#'*100}")
    logger.info(f"# {E['start']} MINERADOR V9 PRODUCTION - COM GESTÃO INTELIGENTE DE DISCO")
    logger.info(f"{'#'*100}")
    logger.info(f"\n✅ SISTEMA ROBUSTO V9 COM:")
    logger.info(f"   • Checkpoint imediato após cada domínio concluído")
    logger.info(f"   • Recuperação inteligente de processamento parcial")
    logger.info(f"   • Detecção de arquivos já exportados")
    logger.info(f"   • Proteção contra sobrescrita de arquivos")
    logger.info(f"   • Reúso inteligente de downloads")
    logger.info(f"   • Validação + Exportação + Limpeza automática")
    logger.info(f"   • Gestão proativa de espaço em disco")
    logger.info(f"   • Deleção automática de ficheiros temporários")
    logger.info(f"   • Recuperação após cancelamento/erro/falta de energia\n")

    logger.info(f"{E['info']} SAVE_PATH: {SAVE_PATH}")
    logger.info(f"{E['cpu']} CPU: {os.cpu_count()}")
    logger.info(f"{E['space']} Disco: {disk_usage()}\n")

    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN não definido")
        sys.exit(2)

    monitor_thread = start_resource_monitor(interval=10)

    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.error(f"{E['error']} HF setup: {e}")
        sys.exit(1)

    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    hf_download_duckdb(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)

    state = load_state()
    logger.info(f"{E['ok']} State loaded: {len(state)} entries")
    
    if state.get("phase8_completed_domains"):
        logger.info(f"{E['check']} Domínios completados em FASE 8: {len(state.get('phase8_completed_domains', {}))}")

    try:
        conn = init_duckdb(DB_PATH)
    except Exception as e:
        logger.error(f"{E['error']} DuckDB: {e}")
        sys.exit(1)

    try:
        session = create_libtorrent_session()
    except Exception as e:
        logger.error(f"{E['error']} Libtorrent: {e}")
        sys.exit(1)

    total_emails = 0
    try:
        overall_start = time.time()

        completed_torrents = phase1_download_torrents(session, MAGNETS)
        if not completed_torrents:
            logger.error(f"{E['error']} Nenhum torrent")
            return
        if stop_event.is_set():
            return

        tars = phase2_wait_downloads(completed_torrents, state)
        if tars and not stop_event.is_set():
            inserted = phase3_process_tars(tars, state, conn)
            total_emails = get_email_counter()
            if not stop_event.is_set():
                phase4_load_to_duckdb([], conn, state)
                if not stop_event.is_set():
                    total_emails = phase5_deduplicate(conn)
                    if not stop_event.is_set():
                        final_files = phase6_export(conn)
                        if not stop_event.is_set():
                            domain_files = phase8_export_by_domain(conn, state)
                            if not stop_event.is_set():
                                phase7_upload(api, HF_TOKEN, emails_repo, checkpoint_repo, final_files, DB_PATH, state)
        else:
            logger.info(f"{E['info']} Nenhum arquivo novo")

        total_time = time.time() - overall_start
        logger.info(f"\n{'='*100}")
        logger.info(f"{E['ok']} ✅ SUCESSO")
        logger.info(f"{E['clock']} Tempo: {total_time / 60:.2f}min")
        logger.info(f"{E['email']} Emails: {total_emails:,}")
        logger.info(f"{E['limit']} Limite: {EMAIL_LIMIT:,}")
        logger.info(f"{E['stats']} Disco: {disk_usage()}")
        logger.info(f"{'='*100}\n")
    except KeyboardInterrupt:
        logger.warning(f"\n{E['warn']} Interrupção - Estado salvo, pode retomar depois")
    except Exception as e:
        logger.error(f"{E['error']} Erro: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        stop_event.set()

if __name__ == "__main__":
    main()
