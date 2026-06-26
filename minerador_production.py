#!/usr/bin/env python3
"""
minerador_production_v5_fixed.py — Versão fixa e completa.

Correções aplicadas:
- Matching robusto: index lookup + path exact + basename + fuzzy fallback
- Timeout/watchdog em downloads para evitar travamento
- Robustez ao ler torrent_info e arquivos (suporte a diferentes versões libtorrent)
- Supressão de DeprecationWarning do libtorrent para limpar logs
- Correções no HF helpers (download local_dir e retorno correto)
- Mantive arquitetura, logs e fases originais
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
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any
from threading import Event, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# libtorrent em algumas versões emite DeprecationWarning — suprimimos apenas essas warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="libtorrent")

import libtorrent as lt
from huggingface_hub import HfApi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

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
    logger = logging.getLogger("minerador_v5")
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

# ===== CONFIG =====
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
FILE_DOWNLOAD_TIMEOUT = int(os.environ.get("FILE_DOWNLOAD_TIMEOUT", str(7200)))  # seconds

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
}

EMAIL_REGEX = re.compile(rb"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", re.IGNORECASE)

stop_event = Event()
state_lock = Lock()

def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum}; graceful shutdown")
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ===== MAGNET LINKS =====
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
            "Collection 1/Collection #1_BTC combos.tar.gz",
            "Collection 1/Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection 1/Collection #1_OLD CLOUD_BTC combos.tar.gz",
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

# ===== UTILITIES =====
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

def is_disposable_email(email: str) -> bool:
    try:
        domain = email.split("@")[-1].lower()
        return domain in DISPOSABLE_DOMAINS
    except Exception:
        return False

# ===== DUCKDB =====
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = duckdb.connect(str(db_path))
        conn.execute("SET threads=8;")
        conn.execute("SET memory_limit='2GB';")
        conn.execute("SET max_memory='2GB';")
        conn.execute(f"SET temp_directory='{str(TEMP_DIR)}';")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS emails_raw (
                email VARCHAR PRIMARY KEY,
                nome VARCHAR,
                origem VARCHAR,
                data VARCHAR
            );
            """
        )
        conn.commit()
        logger.info(f"{E['ok']} DuckDB inicializado")
        return conn
    except Exception as e:
        logger.error(f"{E['error']} DuckDB init falhou: {e}")
        raise

def batch_insert_duckdb(conn: duckdb.DuckDBPyConnection, records: List[Tuple]) -> int:
    if not records:
        return 0
    try:
        for email, nome, origem, data in records:
            try:
                conn.execute(
                    "INSERT INTO emails_raw (email, nome, origem, data) VALUES (?, ?, ?, ?)",
                    [email, nome, origem, data],
                )
            except Exception:
                # presumimos duplicata ou problema; ignorar
                pass
        conn.commit()
        return len(records)
    except Exception as e:
        logger.error(f"{E['error']} Insert falhou: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

# ===== LIBTORRENT =====
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
        except AttributeError:
            # fallback para versões sem settings_pack
            pass
        logger.info(f"{E['ok']} Libtorrent inicializado")
        return session
    except Exception as e:
        logger.error(f"{E['error']} Libtorrent init falhou: {e}")
        raise

def list_all_torrent_files(torrent_info) -> Dict[int, Dict]:
    """Lista todos os arquivos do torrent com índices e detalhes robustamente."""
    files_map: Dict[int, Dict] = {}
    # tentar várias formas de obter o número de arquivos
    try:
        n = getattr(torrent_info, "num_files")()  # se existir
    except Exception:
        try:
            # files() pode ser indexável
            n = len(torrent_info.files())
        except Exception:
            logger.error(f"{E['error']} Não consegui obter número de arquivos")
            return files_map

    for i in range(n):
        try:
            # Tentar várias APIs para file_entry
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
                    # última tentativa: se files() retorna lista-like
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

# Helpers de normalização e parse
def normalize_str(s: str) -> str:
    if s is None:
        return ""
    # remover caracteres invisíveis, normalização unicode, collapse espaços, casefold
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.casefold()

def parse_target_index(target: str):
    """Tenta extrair um índice do target (ex: '[ 76]', '│ [ 76] ...', 'index:76', '76')."""
    if target is None:
        return None
    t = str(target).strip()
    # procurar padrões [ 123 ]
    m = re.search(r"\[\s*(\d+)\s*\]", t)
    if m:
        return int(m.group(1))
    # procurar 'index:123' ou 'idx:123'
    m = re.search(r"\b(?:index|idx)\s*[:=]\s*(\d+)\b", t, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    # se string for somente dígitos
    if re.fullmatch(r"\d+", t):
        return int(t)
    return None

def find_targets_exact(torrent_info, targets: List[str]) -> Tuple[List[int], Dict[int, Dict]]:
    """
    Busca robusta por targets:
    1) Se target indica índice, usa índice.
    2) Path exact (normalizado).
    3) Basename exact (normalizado).
    4) Fuzzy fallback (difflib) sobre paths e basenames.
    """
    files_map = list_all_torrent_files(torrent_info)
    if not files_map:
        logger.error(f"{E['error']} Nenhum arquivo foi lido do torrent!")
        return [], {}

    # Log todos os arquivos do torrent
    logger.info(f"\n{E['list']} ═══ TODOS OS ARQUIVOS NO TORRENT ({len(files_map)} total) ═══")
    for idx in sorted(files_map.keys()):
        info = files_map[idx]
        logger.info(f" [{idx:3d}] {info['path']:<80s} | {human(info['size']):>12s}")
    logger.info(f"{'═' * 100}\n")

    # Preparar mapas normalizados
    normalized_path_map = {idx: normalize_str(info["path"]) for idx, info in files_map.items()}
    normalized_basename_map = {idx: normalize_str(info["basename"]) for idx, info in files_map.items()}
    all_paths = list(normalized_path_map.values())
    all_basenames = list(normalized_basename_map.values())

    found_indices = []
    for target in targets:
        t_raw = str(target)
        logger.info(f"{E['info']} BUSCANDO TARGET: '{t_raw}'")
        # 1) tentar extrair índice
        idx_hint = parse_target_index(t_raw)
        if idx_hint is not None:
            if idx_hint in files_map:
                logger.info(f" {E['ok']} ✅ ENCONTRADO por índice [{idx_hint}] -> {files_map[idx_hint]['path']}")
                found_indices.append(idx_hint)
                continue
            else:
                logger.warning(f" {E['warn']} Índice {idx_hint} fora do intervalo")
        # 2) normalizar target
        t_normalized = normalize_str(t_raw)
        # remover prefixos de listagem tipo "│ [ 0] " se ainda existirem
        t_normalized = re.sub(r"^\W*\[\s*\d+\s*\]\s*", "", t_normalized).strip()
        # 3) buscar por path exato (normalizado)
        matched = False
        for idx, norm_path in normalized_path_map.items():
            if norm_path == t_normalized:
                logger.info(f" {E['ok']} ✅ ENCONTRADO (path) no índice [{idx}]")
                logger.info(f" Path: {files_map[idx]['path']}")
                logger.info(f" Size: {human(files_map[idx]['size'])}")
                found_indices.append(idx)
                matched = True
                break
        if matched:
            continue
        # 4) buscar por basename
        target_basename = normalize_str(Path(t_raw).name)
        if target_basename:
            for idx, norm_base in normalized_basename_map.items():
                if norm_base == target_basename:
                    logger.info(f" {E['ok']} ✅ ENCONTRADO (basename) no índice [{idx}]")
                    logger.info(f" Path: {files_map[idx]['path']}")
                    logger.info(f" Size: {human(files_map[idx]['size'])}")
                    found_indices.append(idx)
                    matched = True
                    break
            if matched:
                continue
        # 5) fuzzy fallback (cauteloso)
        # tentamos encontrar close matches nos paths primeiro, depois basenames
        close = difflib.get_close_matches(t_normalized, all_paths, n=1, cutoff=0.82)
        if close:
            chosen = close[0]
            # achar índice
            idx_chosen = [i for i, p in normalized_path_map.items() if p == chosen]
            if idx_chosen:
                idxc = idx_chosen[0]
                logger.info(f" {E['ok']} ✅ ENCONTRADO (fuzzy path) no índice [{idxc}] -> {files_map[idxc]['path']}")
                found_indices.append(idxc)
                continue
        close_base = difflib.get_close_matches(target_basename, all_basenames, n=1, cutoff=0.82)
        if close_base:
            chosen = close_base[0]
            idx_chosen = [i for i, b in normalized_basename_map.items() if b == chosen]
            if idx_chosen:
                idxc = idx_chosen[0]
                logger.info(f" {E['ok']} ✅ ENCONTRADO (fuzzy basename) no índice [{idxc}] -> {files_map[idxc]['path']}")
                found_indices.append(idxc)
                continue
        # não encontrado
        logger.warning(f" {E['warn']} ❌ NÃO ENCONTRADO: '{t_raw}'")

    found_indices = sorted(set(found_indices))
    logger.info(f"\n{E['list']} RESUMO: {len(found_indices)} target(s) encontrado(s) nos índices: {found_indices}\n")
    return found_indices, files_map

def local_path_for_index(save_path: Path, torrent_info, index: int) -> Path:
    try:
        torrent_name = torrent_info.name()
    except Exception:
        try:
            torrent_name = getattr(torrent_info, "name", lambda: "unknown")()
        except Exception:
            torrent_name = "unknown"
    file_path = None
    try:
        fe = torrent_info.files().at(index)
        file_path = fe.path
    except Exception:
        try:
            fe = torrent_info.files()[index]
            file_path = fe.path
        except Exception:
            # fallback: build using index mapping if possible not available here; return None
            logger.debug(f"{E['warn']} local_path_for_index: não consegui obter file path via API para index {index}")
            return None
    return save_path / torrent_name / file_path

def wait_for_file_complete(handle: lt.torrent_handle, file_index: int, expected_size: int, timeout: int = FILE_DOWNLOAD_TIMEOUT) -> bool:
    """Aguarda até que o arquivo alcance expected_size ou timeout."""
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
                # fallback se fprog não indexável
                try:
                    got = int(fprog[file_index])
                except Exception:
                    got = 0
            pct = (got / expected_size * 100) if expected_size else 0.0
            now = time.time()
            if now - last_log >= 5:
                logger.info(f"{E['download']} File[{file_index}]: {human(got)}/{human(expected_size)} ({pct:.1f}%)")
                last_log = now
            if expected_size and got >= expected_size:
                logger.info(f"{E['ok']} Arquivo {file_index} completo")
                return True
            if (now - start_time) > timeout:
                logger.error(f"{E['error']} Timeout aguardando arquivo {file_index} (esperado {human(expected_size)})")
                return False
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.debug(f"Erro monitorando: {e}")
            time.sleep(POLL_INTERVAL)

# ===== PROCESSAMENTO =====
def process_chunk_worker(chunk_data: bytes, chunk_idx: int, origin: str) -> List[Tuple]:
    results = []
    data_iso = datetime.now(timezone.utc).isoformat()
    try:
        for match in EMAIL_REGEX.finditer(chunk_data):
            try:
                email_b = match.group()
                try:
                    email = email_b.decode("utf8", "ignore").strip().lower()
                except Exception:
                    email = email_b.decode("latin1", "ignore").strip().lower()
                if not email or "@" not in email or is_disposable_email(email):
                    continue
                local_part = email.split("@")[0]
                local_part = re.sub(r"\d+", "", local_part)
                local_part = re.sub(r"[_.\-]+", " ", local_part).strip()
                nome = " ".join([p.capitalize() for p in local_part.split()]) if local_part else ""
                results.append((email, nome, origin, data_iso))
            except Exception:
                continue
    except Exception as e:
        logger.error(f"{E['error']} Worker error: {e}")
    return results

def process_tar_streaming(tar_path: Path, origin: str) -> List[Path]:
    cpu_count = os.cpu_count() or 4
    chunk_files: List[Path] = []
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
                logger.info(f"{E['extract']} [{member_count}] {member.name} ({human(member.size)})")
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
                            if records:
                                all_records.extend(records)
                        except Exception as e:
                            logger.error(f"{E['error']} Worker: {e}")
                if all_records:
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    chunk_file = RAW_CHUNKS_DIR / f"raw_{len(chunk_files):06d}_{ts}.parquet"
                    try:
                        df = pd.DataFrame(all_records, columns=["email", "nome", "origem", "data"])
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, str(chunk_file), compression="snappy")
                        chunk_files.append(chunk_file)
                        total_records += len(all_records)
                        logger.info(f"{E['ok']} Chunk: {chunk_file.name} ({len(all_records):,})")
                    except Exception as e:
                        logger.error(f"{E['error']} Chunk save: {e}")
        # tentar apagar tar (limpeza)
        try:
            tar_path.unlink()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"{E['error']} TAR process: {e}")
    logger.info(f"{E['ok']} TAR: {len(chunk_files)} chunks, {total_records:,}")
    return chunk_files

# ===== HUGGING FACE =====
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
                else:
                    logger.warning(f"{E['warn']} Create repo: {str(e)[:200]}")
        return api, emails_repo, checkpoint_repo
    except Exception as e:
        logger.error(f"{E['error']} HF setup: {e}")
        raise

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str) -> bool:
    if not local_path.exists():
        logger.warning(f"{E['warn']} File not found for upload: {local_path}")
        return False
    max_retries = 3
    logger.info(f"{E['upload']} Upload: {repo_path}")
    for attempt in range(max_retries):
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
            )
            logger.info(f"{E['ok']} Upload OK: {repo_path}")
            return True
        except Exception as e:
            logger.warning(f"{E['warn']} Tentativa {attempt + 1}/{max_retries} falhou: {e}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 10)
    logger.error(f"{E['error']} Upload failed after {max_retries} attempts: {repo_path}")
    return False

def hf_download_checkpoint(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    try:
        logger.info(f"{E['download']} Baixando checkpoint...")
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="state.json",
            local_dir=str(local_path),
            token=token,
            repo_type="dataset",
        )
        logger.info(f"{E['ok']} Checkpoint OK")
        return True
    except Exception:
        logger.info(f"{E['info']} Sem checkpoint anterior")
        return False

def hf_download_duckdb(api: HfApi, token: str, checkpoint_repo: str, local_path: Path) -> bool:
    try:
        logger.info(f"{E['download']} Baixando DuckDB...")
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="emails.duckdb",
            local_dir=str(local_path),
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
    """FASE 1: Download torrents com listagem COMPLETA de arquivos."""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['download']} FASE 1: Download {len(magnets)} torrents (com debug completo)")
    logger.info(f"{'='*100}\n")
    completed: Dict[str, Tuple] = {}

    def download_single(item):
        name = item["name"]
        magnet = item["magnet"]
        targets = item.get("targets", [])
        try:
            logger.info(f"{E['download']} Iniciando: {name}")
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)
            # Espera metadata
            metadata_wait = 0
            max_wait = 600
            while metadata_wait < max_wait and not stop_event.is_set():
                try:
                    # algumas versões têm has_metadata; envolvemos em try/except
                    has_metadata = False
                    try:
                        has_metadata = handle.has_metadata()
                    except Exception:
                        # fallback: tentar torrent_info
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
                    logger.debug(f"{E['clock']} Aguardando metadata {name}... ({metadata_wait}s)")
                time.sleep(1)
            if metadata_wait >= max_wait:
                logger.error(f"{E['error']} Timeout aguardando metadata")
                return None
            if stop_event.is_set():
                raise KeyboardInterrupt()
            # Obter info do torrent (tentar métodos distintos)
            info = None
            try:
                info = handle.torrent_info()
            except Exception:
                try:
                    info = handle.get_torrent_info()
                except Exception as e:
                    logger.error(f"{E['error']} Não consegui obter torrent_info: {e}")
                    return None
            # BUSCA EXATA (robusta)
            found, all_files = find_targets_exact(info, targets)
            if not found:
                logger.error(f"\n{E['error']} ❌ NENHUM TARGET ENCONTRADO em {name}!")
                logger.error(f" Targets procurados:")
                for t in targets:
                    logger.error(f" - {t}")
                logger.error(f"\n Arquivos disponíveis:")
                for idx in sorted(all_files.keys()):
                    f = all_files[idx]
                    logger.error(f" [{idx:3d}] {f['path']}")
                logger.error(f"\n 💡 DICA: Atualize os 'targets' em MAGNETS com os nomes exatos acima!")
                return None
            # Definir prioridades
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
                    # pode falhar em algumas versões — ignora
                    pass
            logger.info(f"{E['ok']} {name} pronto | {len(found)} arquivo(s): {found}")
            return (name, (handle, info, found, all_files))
        except Exception as e:
            logger.error(f"{E['error']} Torrent {name}: {e}")
            logger.debug(traceback.format_exc())
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
                logger.error(f"{E['error']} Future error: {e}")
                logger.debug(traceback.format_exc())
    logger.info(f"\n{E['ok']} FASE 1: {len(completed)}/{len(magnets)} torrents prontos\n")
    return completed

def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    """FASE 2: Aguardar downloads."""
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
                logger.info(f"{E['download']} Esperando: {tname} [idx:{idx}] ({human(expected_size)})")
                ok = wait_for_file_complete(handle, idx, expected_size, timeout=FILE_DOWNLOAD_TIMEOUT)
                if not ok:
                    logger.error(f"{E['error']} Timeout ou erro no download do arquivo {file_key}")
                    continue
                local_path = local_path_for_index(SAVE_PATH, info, idx)
                if local_path is None:
                    logger.error(f"{E['error']} Local path é None para {file_key}")
                    continue
                if not local_path.exists():
                    # Em alguns cenários libtorrent coloca arquivos em subfolders incomuns; tentamos procurar por matching basename
                    basename = Path(all_files_map[idx]["path"]).name
                    logger.warning(f"{E['warn']} Arquivo não encontrado no caminho esperado: {local_path}. Tentando procurar por basename {basename} em {SAVE_PATH}...")
                    # procurar recursivamente por basename no SAVE_PATH/torrent_name
                    torrent_name = getattr(info, "name", lambda: None)()
                    fallback_root = SAVE_PATH / torrent_name if torrent_name else SAVE_PATH
                    found_paths = list(fallback_root.rglob(basename)) if fallback_root.exists() else []
                    if found_paths:
                        local_path = found_paths[0]
                        logger.info(f"{E['ok']} Fallback encontrado: {local_path}")
                    else:
                        logger.error(f"{E['error']} Arquivo não existe: {local_path}")
                        continue
                all_files_ready.append((tname, local_path, info))
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"{E['error']} Download {file_key}: {e}")
                logger.debug(traceback.format_exc())
    logger.info(f"\n{E['ok']} FASE 2: {len(all_files_ready)} arquivos prontos\n")
    return all_files_ready

def phase3_process_tars(tars: List[Tuple], state: Dict) -> List[Path]:
    """FASE 3: Processar TARs."""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['extract']} FASE 3: Processar {len(tars)} TARs")
    logger.info(f"{'='*100}\n")
    all_chunks: List[Path] = []
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
    """FASE 4: Carregar em DuckDB."""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['db']} FASE 4: Carregar {len(chunks)} chunks")
    logger.info(f"{'='*100}\n")
    total_inserted = 0
    loaded_chunks = state.get("loaded_chunks", [])
    for i, chunk_file in enumerate(chunks, 1):
        if stop_event.is_set():
            break
        if str(chunk_file) in loaded_chunks:
            logger.info(f"{E['ok']} [{i}/{len(chunks)}] Já carregado")
            continue
        try:
            logger.info(f"{E['db']} [{i}/{len(chunks)}] Carregando... {chunk_file.name}")
            df = pd.read_parquet(chunk_file)
            records = [tuple(row) for row in df.itertuples(index=False, name=None)]
            inserted = batch_insert_duckdb(conn, records)
            total_inserted += inserted
            loaded_chunks.append(str(chunk_file))
            state["loaded_chunks"] = loaded_chunks
            save_state(state)
            logger.info(f"{E['ok']} [{i}/{len(chunks)}] +{inserted:,}")
        except Exception as e:
            logger.error(f"{E['error']} Load chunk: {e}")
            logger.debug(traceback.format_exc())
    logger.info(f"\n{E['ok']} FASE 4: {total_inserted:,} total\n")
    return total_inserted

def phase5_deduplicate(conn: duckdb.DuckDBPyConnection) -> int:
    """FASE 5: Deduplicate."""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['db']} FASE 5: Deduplicação")
    logger.info(f"{'='*100}\n")
    try:
        count_before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Antes: {count_before:,}")
        conn.execute("CREATE TABLE emails_dedup AS SELECT DISTINCT * FROM emails_raw;")
        conn.execute("DROP TABLE emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()
        count_after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Depois: {count_after:,}")
        logger.info(f"{E['email']} Removidos: {count_before - count_after:,}")
        logger.info(f"\n{E['ok']} FASE 5 completa\n")
        return count_after
    except Exception as e:
        logger.error(f"{E['error']} Dedup: {e}")
        logger.debug(traceback.format_exc())
        return 0

def phase6_export(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    """FASE 6: Exportar."""
    logger.info(f"\n{'='*100}")
    logger.info(f"{E['email']} FASE 6: Exportar")
    logger.info(f"{'='*100}\n")
    final_files: List[Path] = []
    file_num = 1
    offset = 0
    while not stop_event.is_set():
        try:
            rows_df = conn.execute(
                f""" SELECT * FROM emails_raw LIMIT {ROWS_PER_FINAL_FILE} OFFSET {offset}; """
            ).fetchdf()
            if rows_df.shape[0] == 0:
                break
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            final_file = EXPORT_DIR / f"Trader_Emails_{file_num:03d}_{ts}.parquet"
            table = pa.Table.from_pandas(rows_df)
            pq.write_table(table, str(final_file), compression="snappy")
            final_files.append(final_file)
            logger.info(f"{E['ok']} [{file_num}] {rows_df.shape[0]:,} rows -> {final_file.name}")
            file_num += 1
            offset += ROWS_PER_FINAL_FILE
        except Exception as e:
            logger.error(f"{E['error']} Export: {e}")
            logger.debug(traceback.format_exc())
            break
    logger.info(f"\n{E['ok']} FASE 6: {len(final_files)} files\n")
    return final_files

def phase7_upload(api: HfApi, token: str, emails_repo: str, checkpoint_repo: str, final_files: List[Path], db_path: Path, state: Dict):
    """FASE 7: Upload."""
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
    # checkpoint
    hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")
    if db_path.exists():
        hf_upload_file(api, token, checkpoint_repo, db_path, "emails.duckdb")
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    state["final_files_uploaded"] = len(final_files)
    save_state(state)
    logger.info(f"\n{E['ok']} FASE 7 completa\n")

# ===== MAIN =====
def main():
    logger.info(f"\n{'#'*100}")
    logger.info(f"# {E['start']} MINERADOR V5 - VERSÃO FINAL COMPLETA (Debug Total)")
    logger.info(f"{'#'*100}\n")
    logger.info(f"{E['info']} SAVE_PATH: {SAVE_PATH}")
    logger.info(f"{E['cpu']} CPU: {os.cpu_count()}")
    logger.info(f"{E['space']} Disco: {disk_usage()}\n")

    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN não definido")
        sys.exit(2)

    # Setup HF
    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.error(f"{E['error']} HF setup falhou: {e}")
        sys.exit(1)

    # Download checkpoint from HF
    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    hf_download_duckdb(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)

    state = load_state()
    logger.info(f"{E['ok']} State loaded with {len(state)} entries")

    # Initialize DuckDB and libtorrent
    try:
        conn = init_duckdb(DB_PATH)
    except Exception as e:
        logger.error(f"{E['error']} DuckDB init fatal: {e}")
        sys.exit(1)

    try:
        session = create_libtorrent_session()
    except Exception as e:
        logger.error(f"{E['error']} Failed to initialize libtorrent: {e}")
        sys.exit(1)

    total_emails = 0
    try:
        overall_start = time.time()
        # PHASE 1
        completed_torrents = phase1_download_torrents(session, MAGNETS)
        if not completed_torrents:
            logger.error(f"{E['error']} Nenhum torrent completado")
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
                    total_emails = phase5_deduplicate(conn)
                    if not stop_event.is_set():
                        # PHASE 6
                        final_files = phase6_export(conn)
                        if not stop_event.is_set():
                            # PHASE 7
                            phase7_upload(api, HF_TOKEN, emails_repo, checkpoint_repo, final_files, DB_PATH, state)
        total_time = time.time() - overall_start
        logger.info(f"\n{'='*100}")
        logger.info(f"{E['ok']} ✅ SUCESSO COMPLETO")
        logger.info(f"{E['clock']} Tempo: {total_time / 60:.2f}min")
        logger.info(f"{E['email']} Emails: {total_emails:,}")
        logger.info(f"{E['stats']} Disco Final: {disk_usage()}")
        logger.info(f"{'='*100}\n")
    except KeyboardInterrupt:
        logger.warning(f"\n{E['warn']} Interrupção do usuário")
    except Exception as e:
        logger.error(f"{E['error']} Erro: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
