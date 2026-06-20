#!/usr/bin/env python3
"""
minerador_production.py — Escalável a bilhões de emails com tolerância a falhas.

ARQUITETURA:
  FASE 1: Download 5 torrents simultâneos
  FASE 2: Checkpoint torrents no HF
  FASE 3: Processar com mmap + regex + ProcessPoolExecutor
  FASE 4: Gerar raw_chunk_*.parquet
  FASE 5: Filtrar domínios descartáveis
  FASE 6: DuckDB com SELECT DISTINCT (deduplicação global)
  FASE 7: Gerar Trader_Emails_*.parquet (30M linhas/arquivo)
  FASE 8: Upload HF + atualizar checkpoint para próxima run

PERSISTÊNCIA: Tudo no Hugging Face → recuperação completa após timeout
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
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(1 * 1024 * 1024 * 1024)))
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(512 * 1024 * 1024)))

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
    "info": "🗿", "cpu": "⚙️", "db": "🗄️",
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
    # Substituir qualquer whitespace (incluindo \n, \r, \t) por espaço único
    s = re.sub(r'\s+', ' ', s)
    # Normalizar barras de diretório para o padrão UNIX
    s = s.replace('\\', '/')
    # Remover espaços nas extremidades e converter para minúsculas
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
    Find target file indices in torrent using um sistema de matching em 3 níveis.
    Normaliza paths para garantir tolerância a falhas na formatação visual.
    Nunca falha silenciosamente e expõe o catálogo de metadata completo em caso de falha.
    """
    n = torrent_info.num_files()
    files_storage = torrent_info.files()
    
    # 1. Catálogo de ficheiros e pré-computação das versões normalizadas
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
            # Verifica se o basename do target está contido no path real normalizado
            # Ou se o basename real está contido no path do target normalizado
            if target_basename in fdata['norm'] or fdata['basename'] in target_norm:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['warn']} Nível 3 (Match Parcial): '{t}' -> '{fdata['raw']}'")
                break
                
        if not matched:
            missing_targets.append(t)
            logger.error(f"{E['error']} Impossível encontrar correspondência para o target: '{t}'")
            
    # Fallback rigoroso com logging se houver targets desaparecidos
    if missing_targets:
        logger.warning(f"{E['warn']} LISTA COMPLETA DE FICHEIROS DISPONÍVEIS NO TORRENT DE METADATA ({torrent_info.name()}):")
        for i, fdata in file_catalog.items():
            logger.warning(f"  -> Index [{i}]: Raw='{fdata['raw']}' | Normalizado='{fdata['norm']}' | Size={human(fdata['size'])}")
            
    return sorted(list(found_indices)), missing_targets

def local_path_for_index(save_path: Path, torrent_info: lt.torrent_info, index: int) -> Path:
    """Get local path for a torrent file index."""
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    return save_path / torrent_name / file_path

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
        except Exception:
            continue
    
    return results

def process_tar_with_mmap(tar_path: Path, origin: str) -> List[Path]:
    """Extract tar.gz with mmap + regex + ProcessPoolExecutor."""
    cpu_count = os.cpu_count() or 4
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
                
                all_records = []
                chunk_idx = 0
                
                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    futures = []
                    
                    while True:
                        chunk_data = fobj.read(CHUNK_SIZE)
                        if not chunk_data:
                            break
                        
                        if stop_event.is_set():
                            break
                        
                        future = executor.submit(process_chunk_worker, chunk_data, chunk_idx, member.name)
                        futures.append(future)
                        chunk_idx += 1
                    
                    for future in as_completed(futures):
                        try:
                            records = future.result()
                            all_records.extend(records)
                        except Exception as e:
                            logger.exception(f"{E['error']} Worker failed")
                
                # Save as parquet
                if all_records:
                    df = pd.DataFrame(all_records, columns=["email", "nome", "origem", "data"])
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    chunk_file = RAW_CHUNKS_DIR / f"raw_chunk_{len(chunk_files):06d}_{ts}.parquet"
                    
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, str(chunk_file), compression="snappy")
                    
                    chunk_files.append(chunk_file)
                    logger.info(f"{E['ok']} Chunk: {chunk_file.name} ({len(all_records):,})")
        
        # Clean tar
        try:
            tar_path.unlink()
        except Exception:
            pass
    
    except Exception as e:
        logger.exception(f"{E['error']} Tar processing failed")
    
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
            
            # Wait for metadata
            while not handle.has_metadata() and not stop_event.is_set():
                time.sleep(POLL_INTERVAL)
            
            if stop_event.is_set():
                raise KeyboardInterrupt()
            
            info = handle.get_torrent_info()
            found, missing = find_target_indices(info, targets)
            
            if missing:
                logger.error(f"{E['error']} Missing targets in {name} após busca inteligente.")
                raise RuntimeError(f"Targets not found in metadata")
            
            # Set priorities
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
                local_path = local_path_for_index(SAVE_PATH, info, idx)
                
                if not local_path.exists():
                    logger.error(f"{E['error']} File not found on disk: {local_path}")
                    continue
                
                all_files.append((tname, local_path, info))
                
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
            except Exception:
                logger.exception(f"{E['error']} Wait failed")
    
    logger.info(f"{E['ok']} PHASE 2 complete: {len(all_files)} files ready")
    return all_files

def phase3_process_tars(tars: List[Tuple], state: Dict) -> List[Path]:
    """PHASE 3: Process tars with mmap + regex + ProcessPoolExecutor."""
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
    """PHASE 4: Load chunks into DuckDB."""
    logger.info(f"{E['db']} PHASE 4: Loading {len(chunks)} chunks into DuckDB")
    
    total_inserted = 0
    loaded_chunks = state.get("loaded_chunks", [])
    
    for chunk_file in chunks:
        if stop_event.is_set():
            break
        
        if str(chunk_file) in loaded_chunks:
            logger.info(f"{E['ok']} Chunk already loaded: {chunk_file.name}")
            continue
        
        try:
            df = pd.read_parquet(chunk_file)
            records = [tuple(row) for row in df.itertuples(index=False, name=None)]
            inserted = batch_insert_duckdb(conn, records)
            total_inserted += inserted
            
            loaded_chunks.append(str(chunk_file))
            state["loaded_chunks"] = loaded_chunks
            save_state(state)
            
            logger.info(f"{E['db']} Loaded: {chunk_file.name} (+{inserted:,} records)")
        except Exception:
            logger.exception(f"{E['error']} Failed to load chunk")
    
    logger.info(f"{E['ok']} PHASE 4 complete: {total_inserted:,} total records inserted")
    return total_inserted

def phase5_deduplicate_duckdb(conn: duckdb.DuckDBPyConnection) -> int:
    """PHASE 5: Global deduplication using DuckDB SELECT DISTINCT."""
    logger.info(f"{E['db']} PHASE 5: Global deduplication (SELECT DISTINCT)")
    
    try:
        count_before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Records before dedup: {count_before:,}")
        
        # Use SELECT DISTINCT for deduplication
        conn.execute("CREATE TABLE IF NOT EXISTS emails_dedup AS SELECT DISTINCT * FROM emails_raw;")
        conn.execute("DROP TABLE IF EXISTS emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()
        
        count_after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        duplicates = count_before - count_after
        
        logger.info(f"{E['stats']} Records after dedup: {count_after:,}")
        logger.info(f"{E['email']} Duplicates removed: {duplicates:,}")
        
        return count_after
    except Exception:
        logger.exception(f"{E['error']} Deduplication failed")
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
        except Exception:
            logger.exception(f"{E['error']} Failed to export final file")
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
    
    # Update state
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    state["final_files_uploaded"] = len(final_files)
    save_state(state)
    
    logger.info(f"{E['ok']} PHASE 7 complete: Checkpoint saved to HF")

def main():
    """Main orchestration."""
    logger.info(f"{E['start']} Minerador Production v1")
    logger.info(f"{E['info']} SAVE_PATH: {SAVE_PATH}")
    logger.info(f"{E['info']} CPU cores: {os.cpu_count()}")
    logger.info(f"{E['stats']} Disk usage: {disk_usage(SAVE_PATH)}")
    
    # Verify HF token
    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN not set in environment")
        sys.exit(2)
    
    # Setup HF
    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.exception(f"{E['error']} HF setup failed")
        sys.exit(1)
    
    # Download checkpoint from HF
    logger.info(f"{E['download']} Downloading checkpoint from Hugging Face")
    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    hf_download_duckdb(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    
    state = load_state()
    logger.info(f"{E['ok']} State loaded with {len(state)} entries")
    
    # Initialize DuckDB and libtorrent
    conn = init_duckdb(DB_PATH)
    
    try:
        session = create_libtorrent_session()
    except Exception as e:
        logger.exception(f"{E['error']} Failed to initialize libtorrent")
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
                            # PHASE 7
                            phase7_upload_hf(api, HF_TOKEN, emails_repo, checkpoint_repo, final_files, DB_PATH, state)
        
        total_time = time.time() - overall_start
        logger.info(f"{E['stats']} Total runtime: {total_time / 60:.2f} minutes")
        logger.info(f"{E['ok']} Minerador Production completed successfully")
    
    except KeyboardInterrupt:
        logger.warning(f"{E['warn']} Graceful shutdown initiated")
    except Exception as e:
        logger.exception(f"{E['error']} Unexpected error during execution")
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
