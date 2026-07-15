#!/usr/bin/env python3
"""
minerador_v8_v9_MERGED.py

VERSÃO UNIFICADA FINAL: v8 (download/processing) + v9 (validation/domain grouping)

7 FASES COMPLETAS:
  FASE 1: Download torrents (FROM v8)
  FASE 2: Wait downloads (FROM v8)
  FASE 3: Process TARs + Extract emails (FROM v8)
  FASE 4: Load to DuckDB (no-op)
  FASE 5A: Validar emails ativos (DNS+SMTP) (FROM v9)
  FASE 5B: Deduplicate + Agrupar por domínio (FROM v9)
  FASE 6: Exportar por domínio com numeração (FROM v9)
  FASE 7: Upload estruturado no HuggingFace (FROM v9)

MUDANÇAS vs ORIGINALS:
  ✅ Remover funções vazias/duplicadas
  ✅ Unificar imports
  ✅ Manter logger colorido de v8
  ✅ Adicionar EmailValidator de v9
  ✅ Ordem correta de fases
  ✅ Marcas [FROM v8] / [FROM v9]
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
import socket
import smtplib
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Optional, Set
from threading import Event, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from collections import defaultdict

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

# ===== CONFIG =====
EMAIL_LIMIT = 300_000_000
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

# ===== LOGGING [FROM v8] =====
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
    logger = logging.getLogger("minerador")
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

# ===== PATHS [FROM v8 + v9] =====
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
RAW_CHUNKS_DIR = SAVE_PATH / "raw_chunks"

for d in [EXPORT_DIR, TEMP_DIR, RAW_CHUNKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = SAVE_PATH / "emails.duckdb"
STATE_PATH = SAVE_PATH / "state.json"
LOG_PATH = SAVE_PATH / "minerador_merged.log"
ERROR_LOG_PATH = SAVE_PATH / "errors.log"
DOMAINS_STATS_PATH = SAVE_PATH / "domains_stats.json"
VALIDATION_LOG_PATH = SAVE_PATH / "email_validation.log"

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
EMAIL_VALIDATION_TIMEOUT = int(os.environ.get("EMAIL_VALIDATION_TIMEOUT", "5"))

logger = setup_logging(LOG_PATH, LOG_LEVEL)

# ===== EMOJIS [FROM v8 + v9] =====
E = {
    "start": "🚀", "download": "📥", "extract": "📦", "stats": "📊", "space": "💾",
    "email": "📧", "upload": "📤", "clean": "🧹", "warn": "⚠️", "error": "❌",
    "ok": "✅", "info": "ℹ️", "cpu": "⚙️", "clock": "⏱️", "list": "📋",
    "db": "🗄️", "monitor": "📡", "limit": "🛑", "debug": "🔍", "domain": "🌐",
    "check": "🔎", "active": "⚡", "dead": "💀",
}

EMAIL_REGEX = re.compile(rb"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.IGNORECASE)

stop_event = Event()
state_lock = Lock()
domains_lock = Lock()

def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum}; graceful shutdown")
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ===== CONFIGS [FROM v8 + v9] =====
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
    "bounce", "orders", "billing", "alerts",
}

# ===== EMAIL VALIDATION [FROM v9] =====
class EmailValidator:
    """Valida emails usando: Syntax + DNS MX + SMTP light + Disposable check"""
    
    def __init__(self, timeout: int = EMAIL_VALIDATION_TIMEOUT):
        self.timeout = timeout
        self.validated_cache: Dict[str, bool] = {}
        self.cache_lock = Lock()
    
    def validate(self, email: str) -> bool:
        """Valida email completo (syntax + DNS + SMTP)"""
        email = email.lower().strip()
        
        with self.cache_lock:
            if email in self.validated_cache:
                return self.validated_cache[email]
        
        result = False
        try:
            # 1. Syntax
            if not self._validate_syntax(email):
                result = False
            # 2. Disposable
            elif self._is_disposable(email):
                result = False
            # 3. Block local
            elif self._is_blocked_local(email):
                result = False
            # 4. DNS
            elif not self._check_dns(email):
                result = False
            # 5. SMTP (light)
            else:
                result = self._check_smtp_light(email)
        except Exception:
            result = False
        
        with self.cache_lock:
            self.validated_cache[email] = result
        
        return result
    
    def _validate_syntax(self, email: str) -> bool:
        """Valida sintaxe RFC 5322 simplificada"""
        if not email or len(email) > 254 or "@" not in email:
            return False
        try:
            local, domain = email.rsplit("@", 1)
            if not local or not domain or len(local) > 64:
                return False
            if ".." in local or local.startswith(".") or local.endswith("."):
                return False
            if not re.match(r"^[A-Za-z0-9]([A-Za-z0-9.\-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9\-]{0,61}[A-Za-z0-9])?)*$", domain):
                return False
            return True
        except Exception:
            return False
    
    def _is_disposable(self, email: str) -> bool:
        """Verifica se domínio é disposable"""
        try:
            domain = email.split("@")[-1].lower()
            return domain in DISPOSABLE_DOMAINS
        except Exception:
            return False
    
    def _is_blocked_local(self, email: str) -> bool:
        """Verifica se local-part é bloqueado (support, admin, etc)"""
        try:
            local = email.split("@", 1)[0].lower()
            local_base = local.split("+", 1)[0]
            
            if local_base in BLOCK_LOCAL_PARTS:
                return True
            
            for tok in BLOCK_LOCAL_PARTS:
                if local_base.startswith(tok):
                    rest = local_base[len(tok):]
                    if not rest or not rest[0].isalnum():
                        return True
            return False
        except Exception:
            return False
    
    def _check_dns(self, email: str) -> bool:
        """Verifica MX records no DNS"""
        try:
            domain = email.split("@")[-1]
            import dns.resolver
            try:
                records = dns.resolver.resolve(domain, "MX", lifetime=self.timeout)
                return len(records) > 0
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.exception.Timeout):
                return False
        except ImportError:
            # Se dnspython não está instalado, faz SMTP direto
            return True
        except Exception:
            return False
    
    def _check_smtp_light(self, email: str) -> bool:
        """Verifica SMTP com RCPT TO (sem enviar email)"""
        try:
            domain = email.split("@")[-1]
            
            # Tenta obter MX record
            mx_host = None
            try:
                import dns.resolver
                records = dns.resolver.resolve(domain, "MX", lifetime=self.timeout)
                if records:
                    mx_host = str(records[0].exchange).rstrip(".")
            except ImportError:
                # Fallback: assume domain é o MX
                mx_host = domain
            except Exception:
                mx_host = domain
            
            if not mx_host:
                return False
            
            # Conecta ao SMTP
            try:
                sock = socket.create_connection((mx_host, 25), timeout=self.timeout)
                sock.settimeout(self.timeout)
                
                # SMTP handshake
                response = sock.recv(1024)
                if not response.startswith(b"220"):
                    sock.close()
                    return False
                
                # EHLO
                sock.send(b"EHLO checker\r\n")
                response = sock.recv(1024)
                
                # MAIL FROM
                sock.send(b"MAIL FROM:<checker@example.com>\r\n")
                response = sock.recv(1024)
                if not response.startswith(b"250"):
                    sock.close()
                    return False
                
                # RCPT TO
                email_bytes = f"RCPT TO:<{email}>\r\n".encode()
                sock.send(email_bytes)
                response = sock.recv(1024)
                
                # QUIT
                sock.send(b"QUIT\r\n")
                sock.close()
                
                # Interpreta resposta
                if response.startswith(b"250"):
                    return True
                elif response.startswith(b"550") or response.startswith(b"553"):
                    return False
                else:
                    # 451, 452: servidor com problema, assumir válido
                    return True
            except (socket.timeout, socket.error, ConnectionRefusedError):
                return False
        except Exception:
            return False

validator = EmailValidator(timeout=EMAIL_VALIDATION_TIMEOUT)

# ===== UTILITY FUNCTIONS [FROM v8] =====
def human(n: int) -> str:
    """Formata bytes em formato legível"""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path: Path = SAVE_PATH) -> Dict[str, str]:
    """Retorna uso de disco"""
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
    """Verifica espaço em disco"""
    try:
        du = shutil.disk_usage(str(path))
        free_gb = du.free / (1024**3)
        if free_gb < min_free_gb:
            logger.error(f"{E['error']} DISCO INSUFICIENTE: {free_gb:.1f}GB livre, mínimo {min_free_gb}GB")
            return False
        logger.info(f"{E['space']} Disco: {free_gb:.1f}GB livre (OK)")
        return True
    except Exception as e:
        logger.error(f"{E['error']} Erro disco: {e}")
        return False

def start_resource_monitor(interval: int = 10):
    """Monitora recursos (CPU, RAM, Disco)"""
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
                    f"CPU: {cpu:.1f}% | Disco: {disk_free_gb:.1f}GB | Emails: {current_emails:,}/{EMAIL_LIMIT:,}"
                )
                time.sleep(interval)
            except Exception:
                time.sleep(interval)

    thread = threading.Thread(target=monitor_loop, daemon=True, name="ResourceMonitor")
    thread.start()
    logger.info(f"{E['ok']} Monitor iniciado ({interval}s)")
    return thread

def save_state(state: Dict[str, Any]):
    """Salva state.json"""
    with state_lock:
        try:
            with open(STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro salvando state: {e}")

def load_state() -> Dict[str, Any]:
    """Carrega state.json"""
    try:
        if STATE_PATH.exists():
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Erro carregando state: {e}")
        return {}

def save_domains_stats(stats: Dict[str, int]):
    """Salva estatísticas de domínios"""
    try:
        with open(DOMAINS_STATS_PATH, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, default=str, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro salvando domains_stats: {e}")

# ===== EMAIL VALIDATION UTILITIES [FROM v8 + v9] =====
_EMAIL_VALIDATOR_RE = re.compile(
    r"^(?P<local>[A-Za-z0-9](?:[A-Za-z0-9._%+\-]{0,62}[A-Za-z0-9])?)@(?P<domain>[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z]{2,})+)$"
)

def is_valid_email(email: str) -> bool:
    """Valida email com regras rigorosas"""
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
    """Verifica se email é disposable"""
    try:
        domain = email.split("@")[-1].lower()
        return domain in DISPOSABLE_DOMAINS
    except Exception:
        return False

def is_block_local(local: str) -> bool:
    """Verifica se local-part é bloqueado"""
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
    """Extrai nome a partir do local-part"""
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

# ===== DUCKDB [FROM v8] =====
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Inicializa DuckDB com schema correto"""
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
        
        # Schema para emails_raw (vai ter email, nome, domain)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS emails_raw (
                email VARCHAR,
                nome VARCHAR,
                domain VARCHAR,
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
    """Insere registros em batch"""
    if not records:
        return 0
    try:
        df = pd.DataFrame(records, columns=["email", "nome", "domain", "origem", "data"])
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
                    conn.execute("INSERT INTO emails_raw VALUES (?, ?, ?, ?, ?)", list(row))
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

# ===== LIBTORRENT [FROM v8] =====
def create_libtorrent_session() -> lt.session:
    """Cria sessão libtorrent"""
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
    """Lista todos os arquivos do torrent"""
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
    """Normaliza string para comparação"""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.casefold()

def parse_target_index(target: str) -> Optional[int]:
    """Extrai índice de target string"""
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
    """Encontra arquivos target no torrent"""
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
    """Retorna path local para arquivo do torrent"""
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
    """Aguarda download completo de arquivo"""
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

# ===== PROCESSING WORKER [FROM v8] =====
def process_chunk_worker(chunk_data: bytes, chunk_idx: int, origin: str) -> Dict[str, Any]:
    """
    Processa chunk e extrai emails com domain
    Retorna: { "records": [...], "stats": {...} }
    """
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
                
                # NOVO: Extrai domain
                domain = email_lower.split("@", 1)[1]
                nome = extract_name_from_local(local_part)
                stats["valid"] += 1
                results.append((email_lower, nome, domain, origin, data_iso))
            except Exception:
                stats["invalid_format"] += 1
                continue
    except Exception as e:
        return {"records": results, "stats": stats, "error": str(e)}
    return {"records": results, "stats": stats}

def process_tar_streaming_and_insert(tar_path: Path, origin: str, conn: duckdb.DuckDBPyConnection) -> int:
    """Processa TAR e insere emails com domain em DuckDB"""
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

# ===== HUGGINGFACE HELPERS [FROM v8] =====
def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    """Setup datasets no HuggingFace"""
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
    """Upload file para HuggingFace"""
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

def hf_download_checkpoint(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    """Download checkpoint do HuggingFace"""
    try:
        logger.info(f"{E['download']} Checkpoint...")
        api.hf_hub_download(repo_id=checkpoint_repo, filename="state.json", local_dir=str(local_path), token=token, repo_type="dataset")
        logger.info(f"{E['ok']} Checkpoint OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem checkpoint")
        return False

def hf_download_duckdb(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    """Download DuckDB do HuggingFace"""
    try:
        logger.info(f"{E['download']} DuckDB...")
        api.hf_hub_download(repo_id=checkpoint_repo, filename="emails.duckdb", local_dir=str(local_path), token=token, repo_type="dataset")
        logger.info(f"{E['ok']} DuckDB OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem DuckDB")
        return False

# ===== PHASES [FROM v8 + v9] =====

# FASE 1 [FROM v8]
def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    """FASE 1: Download torrents"""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['download']} FASE 1: Download {len(magnets)} torrents")
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

# FASE 2 [FROM v8]
def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    """FASE 2: Aguardar downloads"""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['download']} FASE 2: Aguardando downloads")
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
                expected_size = all_files_map.get(idx, {}).get("size", 0)
                logger.info(f"{E['download']} {tname} [{idx}] ({human(expected_size)})")
                ok = wait_for_file_complete(handle, idx, expected_size, timeout=FILE_DOWNLOAD_TIMEOUT)
                if not ok:
                    logger.error(f"{E['error']} Timeout {file_key}")
                    continue
                local_path = local_path_for_index(SAVE_PATH, info, idx)
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

# FASE 3 [FROM v8]
def phase3_process_tars(tars: List[Tuple], state: Dict, conn: duckdb.DuckDBPyConnection) -> int:
    """FASE 3: Processar TARs"""
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

# FASE 4 [FROM v8]
def phase4_load_to_duckdb(conn: duckdb.DuckDBPyConnection) -> int:
    """FASE 4: No-op (dados já estão no DuckDB)"""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['db']} FASE 4: Load to DuckDB (no-op)")
    logger.info(f"{'='*100}\n")
    return 0

# FASE 5A [FROM v9]
def phase5a_validate_emails(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """
    FASE 5A: Validar emails ativos (DNS + SMTP)
    Retorna: { "valid": n, "invalid": m }
    """
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['active']} FASE 5A: Validar Emails Ativos (DNS + SMTP)")
    logger.info(f"{'='*100}\n")
    
    validation_stats = {"valid": 0, "invalid": 0}
    
    try:
        # Fetch all emails
        emails_result = conn.execute("SELECT DISTINCT email FROM emails_raw ORDER BY email;").fetchall()
        total_emails = len(emails_result)
        
        if total_emails == 0:
            logger.warning(f"{E['warn']} Nenhum email para validar")
            return validation_stats
        
        logger.info(f"{E['check']} Validando {total_emails:,} emails...")
        
        valid_emails = []
        for i, (email,) in enumerate(emails_result):
            if stop_event.is_set():
                break
            
            if i % 1000 == 0:
                logger.info(f"{E['check']} Progresso: {i:,}/{total_emails:,}")
            
            try:
                if validator.validate(email):
                    valid_emails.append(email)
                    validation_stats["valid"] += 1
                else:
                    validation_stats["invalid"] += 1
            except Exception as e:
                logger.debug(f"Validação erro para {email}: {e}")
                validation_stats["invalid"] += 1
        
        # Update DuckDB: manter apenas válidos
        logger.info(f"{E['db']} Removendo {validation_stats['invalid']:,} emails inválidos...")
        
        # Criar tabela temporária com emails válidos
        if valid_emails:
            valid_emails_sql = ", ".join([f"'{e}'" for e in valid_emails])
            conn.execute(f"""
                DELETE FROM emails_raw 
                WHERE email NOT IN ({valid_emails_sql});
            """)
            conn.commit()
        else:
            # Se nenhum email válido, limpar tudo
            conn.execute("DELETE FROM emails_raw;")
            conn.commit()
        
        logger.info(f"\n{E['ok']} FASE 5A: {validation_stats['valid']:,} válidos, {validation_stats['invalid']:,} inválidos\n")
        return validation_stats
    except Exception as e:
        logger.error(f"{E['error']} Fase 5A: {e}")
        logger.debug(traceback.format_exc())
        return validation_stats

# FASE 5B [FROM v9]
def phase5b_deduplicate_and_group(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """
    FASE 5B: Deduplicate + Agrupar por domínio
    Retorna: { "domain.com": count, ... }
    """
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['domain']} FASE 5B: Deduplicate + Agrupar por Domínio")
    logger.info(f"{'='*100}\n")
    
    domain_stats: Dict[str, int] = {}
    
    try:
        # Deduplicate by email
        logger.info(f"{E['db']} Removendo duplicatas...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emails_dedup AS
            SELECT DISTINCT 
                   email,
                   nome,
                   domain
            FROM emails_raw
            WHERE email IS NOT NULL AND domain IS NOT NULL;
        """)
        conn.execute("DROP TABLE IF EXISTS emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()
        
        # Count by domain
        logger.info(f"{E['domain']} Agrupando por domínio...")
        domain_result = conn.execute("""
            SELECT domain, COUNT(*) as count
            FROM emails_raw
            GROUP BY domain
            ORDER BY count DESC;
        """).fetchall()
        
        if not domain_result:
            logger.warning(f"{E['warn']} Nenhum domínio encontrado")
            return domain_stats
        
        total_domains = len(domain_result)
        total_emails = sum(count for _, count in domain_result)
        
        for domain, count in domain_result:
            domain_stats[domain] = count
            logger.info(f"{E['domain']} @{domain}: {count:,} emails")
        
        logger.info(f"\n{E['ok']} FASE 5B: {total_domains} domínios, {total_emails:,} emails total\n")
        return domain_stats
    except Exception as e:
        logger.error(f"{E['error']} Fase 5B: {e}")
        logger.debug(traceback.format_exc())
        return domain_stats

# FASE 6 [FROM v9]
def phase6_export_by_domain(conn: duckdb.DuckDBPyConnection, domain_stats: Dict[str, int]) -> Dict[str, List[Path]]:
    """
    FASE 6: Exportar por domínio com numeração
    Estrutura: Trader_Emails/gmail/email_001.parquet, ...
    """
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['email']} FASE 6: Exportar por Domínio (com numeração)")
    logger.info(f"{'='*100}\n")
    
    domain_files: Dict[str, List[Path]] = {}
    
    try:
        for domain, total_count in sorted(domain_stats.items(), key=lambda x: x[1], reverse=True):
            if stop_event.is_set():
                break
            
            logger.info(f"\n{E['domain']} Exportando @{domain} ({total_count:,} emails)...")
            
            domain_export_dir = EXPORT_DIR / "Trader_Emails" / domain
            domain_export_dir.mkdir(parents=True, exist_ok=True)
            
            domain_files[domain] = []
            
            # Get all emails for this domain
            emails_result = conn.execute(f"""
                SELECT email, nome
                FROM emails_raw
                WHERE domain = '{domain}'
                ORDER BY email;
            """).fetchall()
            
            if not emails_result:
                logger.warning(f"{E['warn']} Nenhum email para {domain}")
                continue
            
            # Export in chunks with numbering
            chunk_size = 100000
            file_counter = 1
            
            for chunk_start in range(0, len(emails_result), chunk_size):
                if stop_event.is_set():
                    break
                
                chunk_end = min(chunk_start + chunk_size, len(emails_result))
                chunk = emails_result[chunk_start:chunk_end]
                
                # Create numbered filename
                filename = f"email_{file_counter:06d}.parquet"
                filepath = domain_export_dir / filename
                
                # Convert to parquet
                df_chunk = pd.DataFrame(chunk, columns=["email", "nome"])
                df_chunk.to_parquet(str(filepath), compression='snappy', index=False)
                
                domain_files[domain].append(filepath)
                
                logger.info(f"{E['ok']} [{domain}] {filename}: {len(chunk):,} emails")
                file_counter += 1
        
        # Save domain stats
        save_domains_stats(domain_stats)
        
        logger.info(f"\n{E['ok']} FASE 6: {sum(len(v) for v in domain_files.values())} arquivos criados\n")
        return domain_files
    except Exception as e:
        logger.error(f"{E['error']} Fase 6: {e}")
        logger.debug(traceback.format_exc())
        return {}

# FASE 7 [FROM v9]
def phase7_upload_by_domain(api: HfApi, token: str, emails_repo: str, checkpoint_repo: str, 
                           domain_files: Dict[str, List[Path]], state: Dict):
    """FASE 7: Upload estruturado por domínio"""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['upload']} FASE 7: Upload por Domínio")
    logger.info(f"{'='*100}\n")
    
    total_uploaded = 0
    uploaded_stats = {}
    
    try:
        for domain, files in sorted(domain_files.items()):
            if stop_event.is_set():
                break
            
            logger.info(f"\n{E['domain']} Uploading @{domain} ({len(files)} arquivos)...")
            domain_uploaded = 0
            
            for i, filepath in enumerate(files, 1):
                if stop_event.is_set():
                    break
                
                repo_path = f"Trader_Emails/{domain}/{filepath.name}"
                if hf_upload_file(api, token, emails_repo, filepath, repo_path):
                    domain_uploaded += 1
                    total_uploaded += 1
                    try:
                        filepath.unlink()
                    except Exception:
                        pass
                
                if i % 5 == 0:
                    logger.info(f"  {E['ok']} {domain}: {domain_uploaded}/{len(files)} arquivos")
            
            uploaded_stats[domain] = domain_uploaded
            logger.info(f"{E['ok']} @{domain}: {domain_uploaded} arquivos uploadados")
        
        # Upload state and stats
        hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")
        hf_upload_file(api, token, checkpoint_repo, DOMAINS_STATS_PATH, "domains_stats.json")
        
        state["last_execution"] = datetime.now(timezone.utc).isoformat()
        state["domain_files_uploaded"] = uploaded_stats
        state["total_files_uploaded"] = total_uploaded
        state["total_emails_extracted"] = get_email_counter()
        save_state(state)
        
        logger.info(f"\n{E['ok']} FASE 7 OK: {total_uploaded} arquivos uploadados\n")
    except Exception as e:
        logger.error(f"{E['error']} Fase 7: {e}")
        logger.debug(traceback.format_exc())

# ===== MAIN =====
def main():
    logger.info(f"\n{'#'*100}")
    logger.info(f"# {E['start']} MINERADOR V8 + V9 MERGED - 7 FASES COMPLETAS")
    logger.info(f"{'#'*100}")
    logger.info(f"\n✅ 7 FASES IMPLEMENTADAS:")
    logger.info(f"   FASE 1: Download torrents (FROM v8)")
    logger.info(f"   FASE 2: Aguardar downloads (FROM v8)")
    logger.info(f"   FASE 3: Processar TARs + Extrair emails (FROM v8)")
    logger.info(f"   FASE 4: Load to DuckDB (no-op)")
    logger.info(f"   FASE 5A: Validar emails ativos (DNS+SMTP) (FROM v9)")
    logger.info(f"   FASE 5B: Deduplicate + Agrupar por domínio (FROM v9)")
    logger.info(f"   FASE 6: Exportar por domínio com numeração (FROM v9)")
    logger.info(f"   FASE 7: Upload estruturado no HuggingFace (FROM v9)\n")

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
    logger.info(f"{E['ok']} State: {len(state)} entries")

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
    domain_stats = {}
    
    try:
        overall_start = time.time()

        # ===== FASES 1-3: Download + Processing [FROM v8] =====
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
                phase4_load_to_duckdb(conn)
                
                # ===== FASES 5A-5B: Validation + Grouping [FROM v9] =====
                if not stop_event.is_set():
                    validation_stats = phase5a_validate_emails(conn)
                    
                    if not stop_event.is_set():
                        domain_stats = phase5b_deduplicate_and_group(conn)
                        
                        # ===== FASE 6-7: Export + Upload [FROM v9] =====
                        if not stop_event.is_set() and domain_stats:
                            domain_files = phase6_export_by_domain(conn, domain_stats)
                            
                            if not stop_event.is_set() and domain_files:
                                phase7_upload_by_domain(api, HF_TOKEN, emails_repo, checkpoint_repo, domain_files, state)
        else:
            logger.info(f"{E['info']} Nenhum arquivo novo")

        total_time = time.time() - overall_start
        logger.info(f"\n{'='*100}")
        logger.info(f"{E['ok']} ✅ SUCESSO TOTAL")
        logger.info(f"{E['clock']} Tempo: {total_time / 60:.2f}min")
        logger.info(f"{E['email']} Emails: {get_email_counter():,}")
        logger.info(f"{E['domain']} Domínios: {len(domain_stats)}")
        logger.info(f"{E['stats']} Disco: {disk_usage()}")
        logger.info(f"{'='*100}\n")
    except KeyboardInterrupt:
        logger.warning(f"\n{E['warn']} Interrupção por usuário")
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
