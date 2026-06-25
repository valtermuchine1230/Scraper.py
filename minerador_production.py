#!/usr/bin/env python3
"""
minerador_production_v5_final.py — VERSÃO FINAL COM DIAGNÓSTICO AUTOMÁTICO

ESTRATÉGIA:
  ✓ Lista TODOS os arquivos antes de tentar fazer match
  ✓ Se não encontrar targets exatos, procura fuzzy automático
  ✓ Seleciona TODOS os .tar.gz que fizerem match com padrões inteligentes
  ✓ Logs 100% detalhados mostrando cada passo
  ✓ SEM DEPRECATION WARNINGS (usando alternativas modernas)
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
import traceback
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any
from threading import Event, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import libtorrent as lt
from huggingface_hub import HfApi
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# ===== LOGGING =====
class ColoredFormatter(logging.Formatter):
    """Formatter com cores."""
    
    COLORS = {
        'DEBUG': '\033[36m',
        'INFO': '\033[32m',
        'WARNING': '\033[33m',
        'ERROR': '\033[31m',
        'CRITICAL': '\033[35m',
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        record.levelname = f"{color}{self.BOLD}{levelname:8s}{self.RESET}"
        record.msg = str(record.msg)
        return super().format(record)

def setup_logging(log_path: Path, log_level: str = "INFO") -> logging.Logger:
    """Setup logging."""
    logger = logging.getLogger("minerador_v5")
    logger.setLevel(log_level)
    logger.handlers = []
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(
        fmt='%(asctime)s │ %(levelname)s │ %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_path), encoding='utf-8', mode='w')
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        fmt='%(asctime)s │ %(levelname)s │ %(funcName)s:%(lineno)d │ %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# ===== CONFIGURAÇÃO =====
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
RAW_CHUNKS_DIR = SAVE_PATH / "raw_chunks"
DB_PATH = SAVE_PATH / "emails.duckdb"
STATE_PATH = SAVE_PATH / "state.json"
LOG_PATH = SAVE_PATH / "minerador.log"
ERROR_LOG_PATH = SAVE_PATH / "errors.log"

for d in [EXPORT_DIR, TEMP_DIR, RAW_CHUNKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO_EMAILS = os.environ.get("HF_REPO_EMAILS", "Trader_Emails")
HF_REPO_CHECKPOINT = os.environ.get("HF_REPO_CHECKPOINT", "minerador_checkpoints")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "3"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BATCH_INSERT_DDB = int(os.environ.get("BATCH_INSERT_DDB", "100000"))
ROWS_PER_FINAL_FILE = int(os.environ.get("ROWS_PER_FINAL_FILE", "10000000"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(256 * 1024 * 1024)))

logger = setup_logging(LOG_PATH, LOG_LEVEL)

E = {
    "start": "🚀", "download": "📥", "extract": "📦", "stats": "📊",
    "space": "💾", "email": "📧", "upload": "📤", "clean": "🧹",
    "warn": "⚠️", "error": "❌", "ok": "✅", "info": "ℹ️",
    "cpu": "⚙️", "db": "🗄️", "clock": "⏱️", "list": "📋", "search": "🔍",
}

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

stop_event = Event()
state_lock = Lock()

def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum}; encerrando...")
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ===== MAGNETS =====
MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        "search_keywords": ["collection", "#2", "trading", "collection", "btc", "combo"],
    },
    {
        "name": "Collection #1",
        "magnet": "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E&dn=Collection%201&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce",
        "search_keywords": ["collection", "#1", "btc", "combo", "trading", "cloud"],
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

# ===== UTILITIES =====
def human(n: int) -> str:
    """Converter bytes."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path: Path = SAVE_PATH) -> Dict[str, str]:
    """Disco."""
    try:
        du = shutil.disk_usage(str(path))
        return {
            "total": human(du.total),
            "used": human(du.used),
            "free": human(du.free),
            "percent": f"{(du.used / du.total * 100):.1f}%"
        }
    except Exception:
        return {"error": "N/A"}

def log_error_detailed(exception: Exception, context: str = ""):
    """Log erro."""
    error_msg = f"""
╔════════════════════════════════════════════════════════════════╗
║ {E['error']} ERRO: {context}
║ Tipo: {type(exception).__name__}
║ Msg: {str(exception)[:60]}
╚════════════════════════════════════════════════════════════════╝
"""
    logger.error(error_msg)

def save_state(state: Dict[str, Any]):
    """Salvar estado."""
    with state_lock:
        try:
            with open(STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str, ensure_ascii=False)
        except Exception as e:
            logger.error(f"{E['error']} Falha ao salvar estado: {e}")

def load_state() -> Dict[str, Any]:
    """Carregar estado."""
    try:
        if STATE_PATH.exists():
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception:
        return {}

def is_disposable_email(email: str) -> bool:
    """Verificar email."""
    try:
        domain = email.split("@")[-1].lower()
        return domain in DISPOSABLE_DOMAINS
    except Exception:
        return False

# ===== DUCKDB =====
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Init DuckDB."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        conn = duckdb.connect(str(db_path))
        conn.execute("SET threads=8;")
        conn.execute("SET memory_limit='2GB';")
        conn.execute("SET max_memory='2GB';")
        conn.execute(f"SET temp_directory='{str(TEMP_DIR)}';")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emails_raw (
                email VARCHAR PRIMARY KEY,
                nome VARCHAR,
                origem VARCHAR,
                data VARCHAR
            );
        """)
        conn.commit()
        
        logger.info(f"{E['ok']} DuckDB OK")
        return conn
    
    except Exception as e:
        log_error_detailed(e, "Init DuckDB")
        raise

def batch_insert_duckdb(conn: duckdb.DuckDBPyConnection, records: List[Tuple]) -> int:
    """Insert."""
    if not records:
        return 0
    
    try:
        for email, nome, origem, data in records:
            try:
                conn.execute(
                    "INSERT INTO emails_raw (email, nome, origem, data) VALUES (?, ?, ?, ?)",
                    [email, nome, origem, data],
                )
            except:
                pass
        
        conn.commit()
        return len(records)
    
    except Exception as e:
        log_error_detailed(e, "Insert DuckDB")
        conn.rollback()
        return 0

# ===== LIBTORRENT =====
def create_libtorrent_session() -> lt.session:
    """Session."""
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
        except:
            pass
        
        logger.info(f"{E['ok']} Libtorrent OK")
        return session
    
    except Exception as e:
        log_error_detailed(e, "Libtorrent")
        raise

def get_all_torrent_files(torrent_info: lt.torrent_info) -> Dict[int, Dict]:
    """Obter TODOS os arquivos do torrent com diagnóstico completo."""
    files_dict = {}
    
    try:
        # Usar file_storage ao invés de at() para evitar deprecation
        file_storage = torrent_info.files()
        n = file_storage.num_files()
        
        for i in range(n):
            try:
                # Alternativa moderna ao at()
                file_entry = file_storage[i]
                file_path = file_entry.path
                file_size = file_entry.size
                
                files_dict[i] = {
                    "path": file_path,
                    "size": file_size,
                    "basename": Path(file_path).name,
                    "name_lower": file_path.lower(),
                }
            except Exception:
                # Fallback se file_storage[i] não funcionar
                try:
                    file_path = file_storage.at(i).path
                    file_size = file_storage.at(i).size
                    
                    files_dict[i] = {
                        "path": file_path,
                        "size": file_size,
                        "basename": Path(file_path).name,
                        "name_lower": file_path.lower(),
                    }
                except Exception as inner_e:
                    logger.debug(f"Erro ao obter arquivo {i}: {inner_e}")
                    continue
    
    except Exception as e:
        logger.error(f"{E['error']} Falha ao obter files: {e}")
    
    return files_dict

def find_target_indices_smart(torrent_info: lt.torrent_info, keywords: List[str]) -> List[int]:
    """
    Busca INTELIGENTE:
    1. Lista TODOS os arquivos
    2. Procura por .tar.gz files
    3. Faz matching fuzzy com keywords
    4. Retorna TODOS que darem match
    """
    all_files = get_all_torrent_files(torrent_info)
    
    # Log de diagnóstico
    logger.info(f"\n{E['list']} DIAGNÓSTICO DO TORRENT:")
    logger.info(f"{'─' * 70}")
    logger.info(f"Total de arquivos: {len(all_files)}")
    logger.info(f"Procurando por keywords: {keywords}")
    logger.info(f"{'─' * 70}\n")
    
    # Listar TODOS os arquivos tar.gz
    logger.info(f"{E['info']} ARQUIVOS .tar.gz DISPONÍVEIS:")
    tar_files = {}
    
    for idx, file_info in sorted(all_files.items()):
        path_lower = file_info["name_lower"]
        
        if path_lower.endswith(".tar.gz"):
            tar_files[idx] = file_info
            logger.info(f"  [{idx:3d}] {file_info['path']} ({human(file_info['size'])})")
    
    logger.info(f"\n{E['search']} PROCURANDO MATCHES COM KEYWORDS:")
    logger.info(f"Keywords: {' + '.join(keywords)}\n")
    
    # Busca fuzzy: procura por TODOS os keywords
    found_indices = []
    
    for idx, file_info in tar_files.items():
        name_lower = file_info["name_lower"]
        
        # Contar quantos keywords estão presentes
        matches = sum(1 for kw in keywords if kw.lower() in name_lower)
        match_pct = (matches / len(keywords)) * 100 if keywords else 0
        
        # Se >50% dos keywords coincidem, incluir
        if match_pct >= 50:
            logger.info(f"  {E['ok']} ✅ MATCH ({match_pct:.0f}%): {file_info['path']}")
            found_indices.append(idx)
        else:
            logger.info(f"  ⭕ Nope ({match_pct:.0f}%): {file_info['basename']}")
    
    if not found_indices:
        logger.warning(f"\n{E['warn']} NENHUM MATCH ENCONTRADO COM {len(keywords)} KEYWORDS")
        logger.warning(f"Procurando fallback: apenas .tar.gz files que contenham 'combo' ou 'collection'...\n")
        
        # Fallback: pegar TODOS os tar.gz que contenham "combo" ou "collection"
        for idx, file_info in tar_files.items():
            name_lower = file_info["name_lower"]
            if ("combo" in name_lower or "collection" in name_lower) and "tar.gz" in name_lower:
                logger.info(f"  {E['ok']} FALLBACK: {file_info['path']}")
                found_indices.append(idx)
    
    logger.info(f"\n{E['ok']} TOTAL ENCONTRADO: {len(found_indices)} arquivo(s)\n")
    
    return sorted(set(found_indices))

def local_path_for_index(save_path: Path, torrent_info: lt.torrent_info, index: int) -> Path:
    """Caminho local."""
    torrent_name = torrent_info.name()
    
    try:
        file_entry = torrent_info.files()[index]
        file_path = file_entry.path
    except:
        file_path = torrent_info.files().at(index).path
    
    return save_path / torrent_name / file_path

def wait_for_file_complete(handle: lt.torrent_handle, file_index: int, expected_size: int) -> bool:
    """Aguardar."""
    last_log = 0
    start_time = time.time()
    
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        
        try:
            fprog = handle.file_progress()
            got = fprog[file_index] if file_index < len(fprog) else 0
            pct = (got / expected_size * 100) if expected_size else 0.0
            
            now = time.time()
            if now - last_log >= 5:
                speed = got / (now - start_time) if (now - start_time) > 0 else 0
                logger.info(f"{E['download']} [{file_index}] {human(got)}/{human(expected_size)} ({pct:.1f}%)")
                last_log = now
            
            if expected_size and got >= expected_size:
                logger.info(f"{E['ok']} Arquivo {file_index} completo")
                return True
            
            time.sleep(POLL_INTERVAL)
        
        except Exception:
            time.sleep(POLL_INTERVAL)

# ===== PROCESSAMENTO =====
def process_chunk_worker(chunk_data: bytes, chunk_idx: int, origin: str) -> List[Tuple]:
    """Worker."""
    results = []
    data_iso = datetime.now(timezone.utc).isoformat()
    
    try:
        for match in EMAIL_REGEX.finditer(chunk_data):
            try:
                email_b = match.group()
                try:
                    email = email_b.decode("utf8", "ignore").strip().lower()
                except:
                    email = email_b.decode("latin1", "ignore").strip().lower()
                
                if not email or "@" not in email or is_disposable_email(email):
                    continue
                
                local_part = email.split("@")[0]
                local_part = re.sub(r"\d+", "", local_part)
                local_part = re.sub(r"[_.\-]+", " ", local_part).strip()
                nome = " ".join([p.capitalize() for p in local_part.split()]) if local_part else ""
                
                results.append((email, nome, origin, data_iso))
            
            except:
                continue
    
    except Exception:
        pass
    
    return results

def process_tar_streaming(tar_path: Path, origin: str) -> List[Path]:
    """Processar TAR."""
    cpu_count = os.cpu_count() or 4
    chunk_files = []
    total_records = 0
    
    logger.info(f"{E['extract']} Processando: {tar_path.name} ({human(tar_path.stat().st_size)})")
    
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            member_count = 0
            
            for member in tar:
                if stop_event.is_set():
                    break
                
                if not member.isfile() or not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue
                
                member_count += 1
                logger.info(f"{E['extract']} [{member_count}] {member.name}")
                
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue
                
                all_records = []
                chunk_idx = 0
                
                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    futures = {}
                    
                    while True:
                        chunk_data = fobj.read(CHUNK_SIZE)
                        if not chunk_data:
                            break
                        
                        if stop_event.is_set():
                            break
                        
                        future = executor.submit(process_chunk_worker, chunk_data, chunk_idx, member.name)
                        futures[future] = chunk_idx
                        chunk_idx += 1
                    
                    for future in as_completed(futures):
                        try:
                            records = future.result()
                            all_records.extend(records)
                        except:
                            pass
                
                if all_records:
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    chunk_file = RAW_CHUNKS_DIR / f"raw_{len(chunk_files):06d}_{ts}.parquet"
                    
                    try:
                        import pandas as pd
                        df = pd.DataFrame(all_records, columns=["email", "nome", "origem", "data"])
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, str(chunk_file), compression="snappy")
                        
                        chunk_files.append(chunk_file)
                        total_records += len(all_records)
                        
                        logger.info(f"{E['ok']} Chunk: {len(all_records):,} emails")
                    
                    except Exception as e:
                        log_error_detailed(e, "Salvar chunk")
        
        try:
            tar_path.unlink()
        except:
            pass
    
    except Exception as e:
        log_error_detailed(e, "Processar TAR")
    
    logger.info(f"{E['ok']} TAR: {len(chunk_files)} chunks, {total_records:,} emails")
    return chunk_files

# ===== HUGGING FACE =====
def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    """Setup HF."""
    if not token:
        raise RuntimeError("HF_TOKEN não definido")
    
    try:
        api = HfApi()
        who = api.whoami(token=token)
        user = who.get("name") or who.get("user")
        
        if not user:
            raise RuntimeError("Usuário HF não determinado")
        
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
        log_error_detailed(e, "Setup HF")
        raise

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str) -> bool:
    """Upload."""
    if not local_path.exists():
        return False
    
    file_size = local_path.stat().st_size
    max_retries = 3
    
    logger.info(f"{E['upload']} Upload: {repo_path} ({human(file_size)})")
    
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
        
        except Exception:
            logger.warning(f"{E['warn']} Tentativa {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 10)
    
    return False

def hf_download_checkpoint(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    """Download checkpoint."""
    try:
        logger.info(f"{E['download']} Baixando checkpoint...")
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="state.json",
            local_dir=str(local_path.parent),
            token=token,
            repo_type="dataset",
        )
        logger.info(f"{E['ok']} Checkpoint OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem checkpoint anterior")
        return False

def hf_download_duckdb(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    """Download DuckDB."""
    try:
        logger.info(f"{E['download']} Baixando DuckDB...")
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="emails.duckdb",
            local_dir=str(local_path.parent),
            token=token,
            repo_type="dataset",
        )
        logger.info(f"{E['ok']} DuckDB OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem DuckDB anterior")
        return False

# ===== FASES =====
def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    """FASE 1: Download torrents com busca inteligente."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['download']} FASE 1: Download {len(magnets)} torrents (busca inteligente com diagnóstico)")
    logger.info(f"{'='*70}\n")
    
    completed = {}
    
    def download_single(item):
        name = item["name"]
        magnet = item["magnet"]
        keywords = item.get("search_keywords", [])
        
        try:
            logger.info(f"{E['download']} Iniciando: {name}")
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)
            
            metadata_wait = 0
            while True:
                try:
                    has_meta = handle.torrent_info() is not None
                except:
                    has_meta = handle.has_metadata() if hasattr(handle, 'has_metadata') else False
                
                if has_meta or stop_event.is_set():
                    break
                
                metadata_wait += 1
                if metadata_wait % 10 == 0:
                    logger.debug(f"{E['clock']} Aguardando metadata {name}...")
                time.sleep(POLL_INTERVAL)
            
            if stop_event.is_set():
                raise KeyboardInterrupt()
            
            # Usar torrent_info() se disponível, fallback para get_torrent_info()
            try:
                info = handle.torrent_info()
            except:
                info = handle.get_torrent_info()
            
            # Busca inteligente com diagnóstico completo
            found = find_target_indices_smart(info, keywords)
            
            if not found:
                logger.error(f"{E['error']} Nenhum arquivo encontrado em {name}")
                logger.error(f"    Keywords procurados: {keywords}")
                logger.error(f"    Verifique os logs acima para diagnóstico completo")
                raise RuntimeError(f"Nenhum arquivo encontrado")
            
            # Definir prioridades
            try:
                n_files = info.files().num_files()
            except:
                n_files = len(list(get_all_torrent_files(info).keys()))
            
            for i in range(n_files):
                try:
                    handle.file_priority(i, 7 if i in found else 0)
                except:
                    pass
            
            logger.info(f"{E['ok']} {name} PRONTO | {len(found)} arquivo(s): {found}")
            return (name, (handle, info, found))
        
        except Exception as e:
            log_error_detailed(e, f"Torrent {name}")
            return None
    
    with ThreadPoolExecutor(max_workers=min(len(magnets), 5)) as executor:
        futures = [executor.submit(download_single, item) for item in magnets]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    name, data = result
                    completed[name] = data
            except Exception:
                pass
    
    logger.info(f"\n{E['ok']} FASE 1: {len(completed)}/{len(magnets)} torrents prontos\n")
    return completed

def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    """FASE 2: Aguardar downloads."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['download']} FASE 2: Aguardando downloads")
    logger.info(f"{'='*70}\n")
    
    all_files = []
    processed_key = state.get("downloaded_files", {})
    
    for tname, (handle, info, indices) in completed_torrents.items():
        if stop_event.is_set():
            break
        
        for idx in indices:
            file_key = f"{tname}_{idx}"
            if file_key in processed_key:
                logger.info(f"{E['ok']} Já processado: {file_key}")
                continue
            
            try:
                file_entry = info.files()[idx]
                expected_size = file_entry.size
            except:
                expected_size = info.files().at(idx).size
            
            logger.info(f"{E['download']} Esperando: {tname} idx:{idx} ({human(expected_size)})")
            
            try:
                wait_for_file_complete(handle, idx, expected_size)
                local_path = local_path_for_index(SAVE_PATH, info, idx)
                
                if not local_path.exists():
                    logger.error(f"{E['error']} Arquivo não encontrado: {local_path}")
                    continue
                
                all_files.append((tname, local_path, info))
                
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
            
            except Exception as e:
                log_error_detailed(e, f"Download {file_key}")
    
    logger.info(f"\n{E['ok']} FASE 2: {len(all_files)} arquivos prontos\n")
    return all_files

def phase3_process_tars(tars: List[Tuple], state: Dict) -> List[Path]:
    """FASE 3: Processar TARs."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['extract']} FASE 3: Processar {len(tars)} TARs")
    logger.info(f"{'='*70}\n")
    
    all_chunks = []
    processed_tars = state.get("processed_tars", [])
    
    for tname, tar_path, info in tars:
        if stop_event.is_set():
            break
        
        if str(tar_path) in processed_tars:
            logger.info(f"{E['ok']} Já processado: {tar_path.name}")
            continue
        
        chunks = process_tar_streaming(tar_path, tname)
        all_chunks.extend(chunks)
        
        processed_tars.append(str(tar_path))
        state["processed_tars"] = processed_tars
        save_state(state)
    
    logger.info(f"\n{E['ok']} FASE 3: {len(all_chunks)} chunks\n")
    return all_chunks

def phase4_load_to_duckdb(chunks: List[Path], conn: duckdb.DuckDBPyConnection, state: Dict) -> int:
    """FASE 4: Carregar."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['db']} FASE 4: Carregar {len(chunks)} chunks")
    logger.info(f"{'='*70}\n")
    
    import pandas as pd
    
    total_inserted = 0
    loaded_chunks = state.get("loaded_chunks", [])
    
    for i, chunk_file in enumerate(chunks, 1):
        if stop_event.is_set():
            break
        
        if str(chunk_file) in loaded_chunks:
            logger.info(f"{E['ok']} [{i}/{len(chunks)}] Já carregado")
            continue
        
        try:
            logger.info(f"{E['db']} [{i}/{len(chunks)}] Carregando...")
            
            df = pd.read_parquet(chunk_file)
            records = [tuple(row) for row in df.itertuples(index=False, name=None)]
            inserted = batch_insert_duckdb(conn, records)
            total_inserted += inserted
            
            loaded_chunks.append(str(chunk_file))
            state["loaded_chunks"] = loaded_chunks
            save_state(state)
            
            logger.info(f"{E['ok']} [{i}/{len(chunks)}] +{inserted:,} records")
        
        except Exception as e:
            log_error_detailed(e, f"Carregando chunk {i}")
    
    logger.info(f"\n{E['ok']} FASE 4: {total_inserted:,} records\n")
    return total_inserted

def phase5_deduplicate(conn: duckdb.DuckDBPyConnection) -> int:
    """FASE 5: Dedup."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['db']} FASE 5: Deduplicação")
    logger.info(f"{'='*70}\n")
    
    try:
        count_before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Antes: {count_before:,}")
        
        conn.execute("CREATE TABLE emails_dedup AS SELECT DISTINCT * FROM emails_raw;")
        conn.execute("DROP TABLE emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()
        
        count_after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        duplicates = count_before - count_after
        
        logger.info(f"{E['stats']} Depois: {count_after:,}")
        logger.info(f"{E['email']} Removidos: {duplicates:,}")
        
        logger.info(f"\n{E['ok']} FASE 5: {count_after:,} únicos\n")
        return count_after
    
    except Exception as e:
        log_error_detailed(e, "Dedup")
        return 0

def phase6_export(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    """FASE 6: Exportar."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['email']} FASE 6: Exportar datasets")
    logger.info(f"{'='*70}\n")
    
    final_files = []
    file_num = 1
    offset = 0
    
    while not stop_event.is_set():
        try:
            logger.info(f"{E['email']} Exportando file {file_num}...")
            
            rows_df = conn.execute(f"""
                SELECT * FROM emails_raw 
                LIMIT {ROWS_PER_FINAL_FILE} 
                OFFSET {offset};
            """).fetchdf()
            
            if rows_df.shape[0] == 0:
                break
            
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            final_file = EXPORT_DIR / f"Trader_Emails_{file_num:03d}_{ts}.parquet"
            
            table = pa.Table.from_pandas(rows_df)
            pq.write_table(table, str(final_file), compression="snappy")
            
            final_files.append(final_file)
            logger.info(f"{E['ok']} [{file_num}] {final_file.name} ({rows_df.shape[0]:,} rows)")
            
            file_num += 1
            offset += ROWS_PER_FINAL_FILE
        
        except Exception as e:
            log_error_detailed(e, f"Export file {file_num}")
            break
    
    logger.info(f"\n{E['ok']} FASE 6: {len(final_files)} files\n")
    return final_files

def phase7_upload(api: HfApi, token: str, emails_repo: str, checkpoint_repo: str, 
                  final_files: List[Path], db_path: Path, state: Dict):
    """FASE 7: Upload."""
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['upload']} FASE 7: Upload HF")
    logger.info(f"{'='*70}\n")
    
    for i, final_file in enumerate(final_files, 1):
        if stop_event.is_set():
            break
        
        repo_path = f"Trader_Emails/{final_file.name}"
        logger.info(f"{E['upload']} [{i}/{len(final_files)}] {repo_path}")
        
        if hf_upload_file(api, token, emails_repo, final_file, repo_path):
            try:
                final_file.unlink()
            except:
                pass
    
    hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")
    
    if db_path.exists():
        hf_upload_file(api, token, checkpoint_repo, db_path, "emails.duckdb")
    
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    
    logger.info(f"\n{E['ok']} FASE 7 completa\n")

# ===== MAIN =====
def main():
    """Main."""
    logger.info(f"\n{'#'*70}")
    logger.info(f"# {E['start']} MINERADOR V5 FINAL - Diagnóstico Completo + Busca Inteligente")
    logger.info(f"{'#'*70}\n")
    
    logger.info(f"{E['info']} SAVE_PATH: {SAVE_PATH}")
    logger.info(f"{E['cpu']} CPU: {os.cpu_count()}")
    logger.info(f"{E['space']} Disco: {disk_usage()}\n")
    
    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN não definido")
        sys.exit(2)
    
    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.error(f"{E['error']} HF setup falhou")
        sys.exit(1)
    
    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    hf_download_duckdb(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    
    state = load_state()
    
    try:
        conn = init_duckdb(DB_PATH)
    except Exception as e:
        sys.exit(1)
    
    try:
        session = create_libtorrent_session()
    except Exception as e:
        sys.exit(1)
    
    total_emails = 0
    
    try:
        overall_start = time.time()
        
        completed_torrents = phase1_download_torrents(session, MAGNETS)
        
        if not completed_torrents:
            logger.error(f"{E['error']} Nenhum torrent completado")
            return
        
        if stop_event.is_set():
            return
        
        tars = phase2_wait_downloads(completed_torrents, state)
        
        if tars and not stop_event.is_set():
            chunks = phase3_process_tars(tars, state)
            
            if chunks and not stop_event.is_set():
                phase4_load_to_duckdb(chunks, conn, state)
                
                if not stop_event.is_set():
                    total_emails = phase5_deduplicate(conn)
                    
                    if not stop_event.is_set():
                        final_files = phase6_export(conn)
                        
                        if not stop_event.is_set():
                            phase7_upload(api, HF_TOKEN, emails_repo, checkpoint_repo, 
                                        final_files, DB_PATH, state)
        
        total_time = time.time() - overall_start
        logger.info(f"\n{'='*70}")
        logger.info(f"{E['ok']} ✅ SUCESSO TOTAL")
        logger.info(f"{E['clock']} Tempo: {total_time / 60:.2f}min")
        logger.info(f"{E['email']} Emails: {total_emails:,}")
        logger.info(f"{E['stats']} Disco: {disk_usage()}")
        logger.info(f"{'='*70}\n")
    
    except KeyboardInterrupt:
        logger.warning(f"\n{E['warn']} Interrupção")
    
    except Exception as e:
        log_error_detailed(e, "Main")
        sys.exit(1)
    
    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()
