#!/usr/bin/env python3
"""
minerador_production.py \u2014 Escal\u00e1vel a bilh\u00f5es de emails com toler\u00e2ncia a falhas.

ARQUITETURA:
  FASE 1: Download 5 torrents simult\u00e2neos
  FASE 2: Checkpoint torrents no HF
  FASE 3: Processar com mmap + regex + ProcessPoolExecutor + STREAMING
  FASE 4: Gerar raw_chunk_*.parquet (streaming incremental)
  FASE 5: Filtrar dom\u00ednios descart\u00e1veis
  FASE 6: DuckDB com SELECT DISTINCT (deduplica\u00e7\u00e3o global)
  FASE 7: Gerar Trader_Emails_*.parquet (30M linhas/arquivo)
  FASE 8: Upload HF + atualizar checkpoint para pr\u00f3xima run

PERSIST\u00caNCIA: Tudo no Hugging Face \u2192 recupera\u00e7\u00e3o completa ap\u00f3s timeout

OTIMIZA\u00c7\u00d5ES DE MEM\u00d3RIA:
  - process_tar_with_mmap(): Streaming com ParquetWriter (mem\u00f3ria constante)
  - phase4_load_to_duckdb(): INSERT FROM read_parquet() nativo (sem DataFrame)
  - ALTERA\u00c7\u00c3O 1: CHUNK_SIZE = 256 MB (de 1 GB)
  - ALTERA\u00c7\u00c3O 2: MAX_WORKERS = min(6, cpu_count)
  - ALTERA\u00c7\u00c3O 3: MAX_INFLIGHT = min(8, cpu_count * 2)
  - ALTERA\u00c7\u00c3O 4: Logs de mem\u00f3ria detalhados (GB usado/livre)
  - ALTERA\u00c7\u00c3O 5: gc.collect() peri\u00f3dico ap\u00f3s processamento
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

import libtorrent as lt
from huggingface_hub import HfApi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from rich.logging import RichHandler
from rich.console import Console

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

# ALTERA\u00c7\u00c3O 1: REDUZIR CHUNK_SIZE DE 1GB PARA 256MB
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(256 * 1024 * 1024)))

MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(512 * 1024 * 1024)))
ROWS_PER_PARQUET_FILE = int(os.environ.get("ROWS_PER_PARQUET_FILE", "5000000"))

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
    "start": "\ud83d\ude80", "download": "\ud83d\udce5", "extract": "\ud83d\udce6", "stats": "\ud83d\udcca",
    "space": "\ud83d\udcc9", "email": "\ud83d\udce7", "upload": "\ud83d\udce4",
    "clean": "\ud83e\uddf9", "warn": "\u26a0\ufe0f", "error": "\u274c", "ok": "\u2705",
    "info": "\ud83d\uddff", "cpu": "\u2699\ufe0f", "db": "\ud83d\uddc4\ufe0f",
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
    espa\u00e7os duplicados ou invis\u00edveis e diferen\u00e7as de path.
    """
    if not isinstance(s, str):
        s = str(s)
    # Substituir qualquer whitespace (incluindo \n, \r, \t) por espa\u00e7o \u00fanico
    s = re.sub(r'\s+', ' ', s)
    # Normalizar barras de diret\u00f3rio para o padr\u00e3o UNIX
    s = s.replace('\\', '/')
    # Remover espa\u00e7os nas extremidades e converter para min\u00fasculas
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

# ===== DUCKDB =====
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB with optimal settings."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    
    conn.execute("PRAGMA threads=8;")
    conn.execute("PRAGMA memory_limit='12GB';")
    
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
        # Try modern libtorrent API (v2.x)
        session = lt.session()
        
        # Configure via settings_pack if available
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
            # Fallback for older libtorrent versions
            logger.info(f"{E['info']} Using fallback libtorrent configuration")
            pass
        
        logger.info(f"{E['cpu']} Libtorrent session created")
        return session
    except Exception as e:
        logger.exception(f"{E['error']} Failed to create libtorrent session")
        raise

def find_target_indices(torrent_info: lt.torrent_info, targets: List[str]) -> Tuple[List[int], List[str]]:
    """
    Find target file indices in torrent using um sistema de matching em 3 n\u00edveis.
    Normaliza paths para garantir toler\u00e2ncia a falhas na formata\u00e7\u00e3o visual.
    Nunca falha silenciosamente e exp\u00f5e o cat\u00e1logo de metadata completo em caso de falha.
    """
    n = torrent_info.num_files()
    files_storage = torrent_info.files()
    
    # 1. Cat\u00e1logo de ficheiros e pr\u00e9-computa\u00e7\u00e3o das vers\u00f5es normalizadas
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
        
        # N\u00cdVEL 1: Match Exato Normalizado
        for i, fdata in file_catalog.items():
            if fdata['norm'] == target_norm:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['ok']} N\u00edvel 1 (Match Exato): '{t}' -> '{fdata['raw']}'")
                break
                
        if matched: continue
            
        # N\u00cdVEL 2: Match por Nome Final do Ficheiro (Basename)
        for i, fdata in file_catalog.items():
            if fdata['basename'] == target_basename:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['ok']} N\u00edvel 2 (Basename): '{t}' -> '{fdata['raw']}'")
                break
                
        if matched: continue
            
        # N\u00cdVEL 3: Match Parcial (Substring Robusta)
        for i, fdata in file_catalog.items():
            # Verifica se o basename do target est\u00e1 contido no path real normalizado
            # Ou se o basename real est\u00e1 contido no path do target normalizado
            if target_basename in fdata['norm'] or fdata['basename'] in target_norm:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['warn']} N\u00edvel 3 (Match Parcial): '{t}' -> '{fdata['raw']}'")
                break
                
        if not matched:
            missing_targets.append(t)
            logger.error(f"{E['error']} Imposs\u00edvel encontrar correspond\u00eancia para o target: '{t}'")
            
    # Fallback rigoroso com logging se houver targets desaparecidos
    if missing_targets:
        logger.warning(f"{E['warn']} LISTA COMPLETA DE FICHEIROS DISPON\u00cdVEIS NO TORRENT DE METADATA ({torrent_info.name()}):")
        for i, fdata in file_catalog.items():
            logger.warning(f"  -> Index [{i}]: Raw='{fdata['raw']}' | Normalizado='{fdata['norm']}' | Size={human(fdata['size'])}")
            
    return sorted(list(found_indices)), missing_targets

def local_path_for_index_robust(save_path: Path, torrent_info: lt.torrent_info, index: int) -> Path | None:
    """
    Localiza robustamente o arquivo baixado no disco, tolerando m\u00faltiplas estruturas de libtorrent.
    """
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    basename = Path(file_path).name
    
    # N\u00cdVEL 1: Constru\u00e7\u00e3o padr\u00e3o (salvepath / torrent_name / file_path)
    candidate1 = save_path / torrent_name / file_path
    if candidate1.exists() and candidate1.is_file():
        logger.info(f"{E['ok']} [N\u00cdVEL 1] Arquivo localizado: {candidate1}")
        return candidate1
    
    # N\u00cdVEL 2: Sem duplica\u00e7\u00e3o (salvepath / file_path)
    candidate2 = save_path / file_path
    if candidate2.exists() and candidate2.is_file():
        logger.info(f"{E['ok']} [N\u00cdVEL 2] Arquivo localizado (sem duplica\u00e7\u00e3o): {candidate2}")
        return candidate2
    
    # N\u00cdVEL 3: Busca recursiva por basename no diret\u00f3rio do torrent
    torrent_dir = save_path / torrent_name
    if torrent_dir.exists() and torrent_dir.is_dir():
        for found_file in torrent_dir.rglob(basename):
            if found_file.is_file():
                logger.info(f"{E['ok']} [N\u00cdVEL 3] Arquivo localizado (busca recursiva): {found_file}")
                return found_file
    
    # N\u00cdVEL 4: Busca recursiva a partir do save_path (fallback m\u00e1ximo)
    for found_file in save_path.rglob(basename):
        if found_file.is_file():
            logger.info(f"{E['ok']} [N\u00cdVEL 4] Arquivo localizado (busca global): {found_file}")
            return found_file
    
    # FALHA: Registar diagn\u00f3stico completo
    logger.error(f"{E['error']} ========== DIAGN\u00d3STICO COMPLETO DE ARQUIVO PERDIDO ==========")
    logger.error(f"{E['error']} Torrent: {torrent_name}")
    logger.error(f"{E['error']} File Index: {index}")
    logger.error(f"{E['error']} File Path (raw): {file_path}")
    logger.error(f"{E['error']} File Basename: {basename}")
    logger.error(f"{E['error']} Save Path: {save_path}")
    logger.error(f"{E['error']} ")
    logger.error(f"{E['error']} Caminhos testados (N\u00c3O encontrados):")
    logger.error(f"{E['error']}   [1] {candidate1}")
    logger.error(f"{E['error']}   [2] {candidate2}")
    if torrent_dir.exists():
        logger.error(f"{E['error']}   [3] Busca recursiva em {torrent_dir}/")
    logger.error(f"{E['error']}   [4] Busca global em {save_path}/")
    logger.error(f"{E['error']} ")
    logger.error(f"{E['error']} Conte\u00fado do diret\u00f3rio Torrent ({torrent_dir}):")
    if torrent_dir.exists() and torrent_dir.is_dir():
        try:
            for item in list(torrent_dir.rglob("*"))[:50]:  # Limitar output
                rel_path = item.relative_to(save_path)
                if item.is_file():
                    size = item.stat().st_size
                    logger.error(f"{E['error']}     FILE: {rel_path} ({human(size)})")
                else:
                    logger.error(f"{E['error']}     DIR:  {rel_path}/")
        except Exception as e:
            logger.error(f"{E['error']}     [Erro ao listar: {str(e)}]")
    else:
        logger.error(f"{E['error']}     [Diret\u00f3rio N\u00c3O existe]")
    logger.error(f"{E['error']} ")
    logger.error(f"{E['error']} Conte\u00fado raiz de Save Path ({save_path}):")
    try:
        for item in list(save_path.iterdir())[:20]:  # Apenas n\u00edvel 1
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
            local_part = re.sub(r"[_.\-]+", " ", local_part).strip()
            nome = " ".join([p.capitalize() for p in local_part.split()]) if local_part else ""
            
            results.append((email, nome, origin, data_iso))
        except Exception as e:
            logger.exception(
                "\u274c WORKER FAILURE DETALHADO\n"
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
    Com otimiza\u00e7\u00f5es de mem\u00f3ria: CHUNK_SIZE 256MB, MAX_WORKERS 6, MAX_INFLIGHT 8.
    """
    # ALTERA\u00c7\u00c3O 2: LIMITAR WORKERS A M\u00c1XIMO 6 (conservador, mant\u00e9m paralelismo)
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
                
                # ALTERA\u00c7\u00c3O 5: LIBERAR MEM\u00d3RIA PERIODICAMENTE
                gc.collect()
                
                # STREAMING: Inicializa writer quando temos dados
                writer = None
                schema = None
                current_chunk_file = None
                row_count = 0
                chunk_batch_count = 0
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                
                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    # ALTERA\u00c7\u00c3O 3: LIMITAR MAX_INFLIGHT A 8 (evita explos\u00e3o de mem\u00f3ria)
                    MAX_INFLIGHT = min(8, (os.cpu_count() or 4) * 2)
                    inflight = set()
                    chunk_idx = 0
                    
                    def check_memory():
                        """Monitorar mem\u00f3ria com log detalhado."""
                        mem = psutil.virtual_memory()
                        # ALTERA\u00c7\u00c3O 4: LOG DETALHADO COM GB USADO/LIVRE
                        if mem.percent > 85:
                            logger.warning(
                                f"\u26a0\ufe0f RAM ALTA: "
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
                                    "\u274c WORKER FAILURE DETALHADO\n"
                                    f"chunk_idx={chunk_idx}\n"
                                    f"member={member.name}\n"
                                    f"error_type={type(e).__name__}\n"
                                    f"error_msg={str(e)}"
                                )
                                return None
                        return None

                    def yield_or_write
