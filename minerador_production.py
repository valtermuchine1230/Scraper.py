#!/usr/bin/env python3
"""
minerador_production_v2.py — Escalável a bilhões de emails (100% Disco, Logs Bonitos, Erros Detalhados)

ARQUITETURA OTIMIZADA:
  ✓ ZERO uso de RAM para dados grandes (tudo em disco)
  ✓ Processamento em streaming com chunks pequenos
  ✓ DuckDB com cache em disco (não RAM)
  ✓ Logs estruturados e cores
  ✓ Erros com stack trace completo + dicas de resolução

PERSISTÊNCIA: Hugging Face → recuperação completa após timeout
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
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any, Set, Optional
from threading import Event, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import libtorrent as lt
from huggingface_hub import HfApi
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# ===== LOGGING COLORIDO =====
class ColoredFormatter(logging.Formatter):
    """Formatter com cores para diferentes níveis de log."""
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.RESET)
        
        # Formatar mensagem com cores
        record.levelname = f"{color}{self.BOLD}{levelname:8s}{self.RESET}"
        record.msg = str(record.msg)
        
        return super().format(record)

def setup_logging(log_path: Path, log_level: str = "INFO") -> logging.Logger:
    """Setup logging com console + arquivo."""
    logger = logging.getLogger("minerador_v2")
    logger.setLevel(log_level)
    
    # Limpar handlers antigos
    logger.handlers = []
    
    # Console handler (colorido)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(
        fmt='%(asctime)s │ %(levelname)s │ %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler (completo)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_path), encoding='utf-8')
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
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(512 * 1024 * 1024)))

# Setup logging
logger = setup_logging(LOG_PATH, LOG_LEVEL)

# ===== EMOJIS E SÍMBOLOS =====
E = {
    "start": "🚀", "download": "📥", "extract": "📦", "stats": "📊",
    "space": "💾", "email": "📧", "upload": "📤", "clean": "🧹",
    "warn": "⚠️", "error": "❌", "ok": "✅", "info": "ℹ️",
    "cpu": "⚙️", "db": "🗄️", "clock": "⏱️", "arrow": "→",
}

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

stop_event = Event()
state_lock = Lock()

def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum} recebido; encerrando gracefully...")
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
            "Collection #1/Collection #1_BTC combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_BTC combos.tar.gz",
        ],
    },
]

# DISPOSABLE DOMAINS
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
    """Converter bytes em formato legível."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path: Path = SAVE_PATH) -> Dict[str, str]:
    """Obter info de disco."""
    try:
        du = shutil.disk_usage(str(path))
        return {
            "total": human(du.total),
            "used": human(du.used),
            "free": human(du.free),
            "percent": f"{(du.used / du.total * 100):.1f}%"
        }
    except Exception as e:
        logger.error(f"{E['error']} Erro ao obter uso de disco: {e}")
        return {"error": str(e)}

def log_error_detailed(exception: Exception, context: str = "", suggestions: List[str] = None):
    """Log detalhado de erros com contexto e sugestões."""
    error_msg = f"""
╔════════════════════════════════════════════════════════════════╗
║ {E['error']} ERRO DETALHADO
╠════════════════════════════════════════════════════════════════╣
║ Contexto: {context}
║ Tipo: {type(exception).__name__}
║ Mensagem: {str(exception)}
╠════════════════════════════════════════════════════════════════╣
║ Stack Trace:
║ {chr(10).join(['║ ' + line for line in traceback.format_exc().split(chr(10))])}
╠════════════════════════════════════════════════════════════════╣
║ Sugestões:
"""
    
    if suggestions:
        for i, suggestion in enumerate(suggestions, 1):
            error_msg += f"║ {i}. {suggestion}\n"
    else:
        error_msg += "║ • Verifique os logs para mais detalhes\n"
        error_msg += "║ • Tente reiniciar o processo\n"
    
    error_msg += "╚════════════════════════════════════════════════════════════════╝\n"
    
    logger.error(error_msg)
    
    # Salvar em arquivo de erros
    try:
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"\n{datetime.now().isoformat()}\n{error_msg}\n")
    except Exception:
        pass

def save_state(state: Dict[str, Any]):
    """Salvar estado (thread-safe)."""
    with state_lock:
        try:
            with open(STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str, ensure_ascii=False)
            logger.debug(f"{E['ok']} Estado salvo: {len(state)} entradas")
        except Exception as e:
            log_error_detailed(e, "Salvando estado", [
                f"Verifique permissões de escrita em {STATE_PATH}",
                "Certifique-se de ter espaço em disco suficiente"
            ])

def load_state() -> Dict[str, Any]:
    """Carregar estado."""
    try:
        if STATE_PATH.exists():
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        log_error_detailed(e, "Carregando estado", [
            "Arquivo pode estar corrompido, iniciando com estado limpo",
            f"Backup em: {STATE_PATH}.backup"
        ])
        return {}

def is_disposable_email(email: str) -> bool:
    """Verificar se email é de domínio descartável."""
    try:
        domain = email.split("@")[-1].lower()
        return domain in DISPOSABLE_DOMAINS
    except Exception:
        return False

# ===== DUCKDB (OTIMIZADO PARA DISCO) =====
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Inicializar DuckDB com settings otimizados para DISCO."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        conn = duckdb.connect(str(db_path))
        
        # ✓ SETTINGS OTIMIZADOS PARA DISCO (não RAM)
        conn.execute("SET threads=8;")
        conn.execute("SET memory_limit='2GB';")
        conn.execute("SET max_memory='2GB';")
        # REMOVIDO: buffer_pool_size (não existe em todas versões do DuckDB)
        conn.execute(f"SET temp_directory='{str(TEMP_DIR)}';")
        
        logger.info(f"{E['db']} DuckDB: Temporary dir = {TEMP_DIR}")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emails_raw (
                email VARCHAR PRIMARY KEY,
                nome VARCHAR,
                origem VARCHAR,
                data VARCHAR
            );
        """)
        conn.commit()
        
        logger.info(f"{E['ok']} DuckDB inicializado: {db_path}")
        return conn
    
    except Exception as e:
        log_error_detailed(e, "Inicializando DuckDB", [
            f"Verifique se {db_path.parent} existe e é gravável",
            "Tente criar manualmente: mkdir -p " + str(db_path.parent),
            "DuckDB requer acesso a disco, não apenas RAM"
        ])
        raise

def batch_insert_duckdb_streaming(conn: duckdb.DuckDBPyConnection, 
                                   records: List[Tuple]) -> int:
    """
    Inserir dados em batch direto (otimizado para DuckDB).
    """
    if not records:
        return 0
    
    try:
        # Usar INSERT com VALUES lista para melhor performance
        for email, nome, origem, data in records:
            try:
                conn.execute(
                    "INSERT INTO emails_raw (email, nome, origem, data) VALUES (?, ?, ?, ?)",
                    [email, nome, origem, data],
                )
            except duckdb.CatalogException:
                # Duplicate, skip
                pass
        
        conn.commit()
        return len(records)
    
    except Exception as e:
        log_error_detailed(e, "Inserindo batch DuckDB", [
            f"Verifique espaço em disco disponível",
            "Tente deletar e reprocessar o chunk",
            "Ou reduza BATCH_INSERT_DDB"
        ])
        conn.rollback()
        return 0

# ===== LIBTORRENT =====
def create_libtorrent_session() -> lt.session:
    """Criar session libtorrent otimizada."""
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
            logger.info(f"{E['cpu']} Libtorrent configurado com {cpu_count} cores")
        
        except AttributeError:
            logger.info(f"{E['info']} Usando configuração padrão libtorrent")
        
        return session
    
    except Exception as e:
        log_error_detailed(e, "Criando session libtorrent", [
            "Instale libtorrent: pip install python-libtorrent",
            "Ou no Ubuntu: sudo apt install python3-libtorrent",
            "Verifique se a porta está disponível (6881-6889)"
        ])
        raise

def find_target_indices(torrent_info: lt.torrent_info, 
                       targets: List[str]) -> Tuple[List[int], List[str]]:
    """Encontrar índices de arquivos alvo no torrent."""
    n = torrent_info.num_files()
    idx_to_path = {i: torrent_info.files().at(i).path for i in range(n)}
    
    found = []
    missing = []
    
    for t in targets:
        matched = False
        for i, p in idx_to_path.items():
            if p == t or p.lower() == t.lower():
                found.append(i)
                matched = True
                logger.debug(f"{E['ok']} Alvo encontrado: {t} (index {i})")
                break
        if not matched:
            missing.append(t)
            logger.warning(f"{E['warn']} Alvo não encontrado: {t}")
    
    return sorted(set(found)), missing

def local_path_for_index(save_path: Path, 
                         torrent_info: lt.torrent_info, 
                         index: int) -> Path:
    """Caminho local para um arquivo do torrent."""
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    return save_path / torrent_name / file_path

def wait_for_file_complete(handle: lt.torrent_handle, 
                          file_index: int, 
                          expected_size: int) -> bool:
    """Aguardar arquivo completar download."""
    last_log = 0
    start_time = time.time()
    
    while True:
        if stop_event.is_set():
            logger.warning(f"{E['warn']} Download cancelado pelo usuário")
            raise KeyboardInterrupt()
        
        try:
            fprog = handle.file_progress()
            got = fprog[file_index] if file_index < len(fprog) else 0
            pct = (got / expected_size * 100) if expected_size else 0.0
            
            now = time.time()
            if now - last_log >= 5:
                speed = got / (now - start_time) if (now - start_time) > 0 else 0
                eta = (expected_size - got) / speed if speed > 0 else 0
                logger.info(
                    f"{E['download']} File[{file_index}]: {human(got)}/{human(expected_size)} "
                    f"({pct:.1f}%) | Vel: {human(speed)}/s | ETA: {eta/60:.1f}min"
                )
                last_log = now
            
            if expected_size and got >= expected_size:
                logger.info(f"{E['ok']} Arquivo {file_index} completo ({human(expected_size)})")
                return True
            
            time.sleep(POLL_INTERVAL)
        
        except Exception as e:
            logger.error(f"{E['error']} Erro monitorando download: {e}")
            time.sleep(POLL_INTERVAL)

# ===== PROCESSAMENTO =====
def process_chunk_worker(chunk_data: bytes, 
                        chunk_idx: int, 
                        origin: str) -> List[Tuple]:
    """
    Worker process: extrair emails de chunk usando regex em BYTES.
    ZERO conversão intermediária em RAM.
    """
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
                
                # Extrair nome do email
                local_part = email.split("@")[0]
                local_part = re.sub(r"\d+", "", local_part)
                local_part = re.sub(r"[_.\-]+", " ", local_part).strip()
                nome = " ".join([p.capitalize() for p in local_part.split()]) if local_part else ""
                
                results.append((email, nome, origin, data_iso))
            
            except Exception:
                continue
    
    except Exception as e:
        logger.error(f"{E['error']} Erro no worker {chunk_idx}: {e}")
    
    return results

def process_tar_with_disk_streaming(tar_path: Path, origin: str) -> List[Path]:
    """
    Extrair TAR.GZ em STREAMING com chunks pequenos (zero RAM).
    Salvar chunks em Parquet imediatamente.
    """
    cpu_count = os.cpu_count() or 4
    chunk_files = []
    total_records = 0
    
    logger.info(f"{E['extract']} {E['arrow']} Processando: {tar_path.name} ({human(tar_path.stat().st_size)})")
    
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            member_count = 0
            
            for member in tar:
                if stop_event.is_set():
                    logger.warning(f"{E['warn']} Extração cancelada")
                    break
                
                if not member.isfile() or not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue
                
                member_count += 1
                logger.info(f"{E['extract']} {E['arrow']} [{member_count}] {member.name} ({human(member.size)})")
                
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue
                
                all_records = []
                chunk_idx = 0
                bytes_processed = 0
                
                # Processar em chunks PEQUENOS (256MB)
                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    futures = {}
                    
                    # Ler e submeter chunks
                    while True:
                        chunk_data = fobj.read(CHUNK_SIZE)
                        if not chunk_data:
                            break
                        
                        if stop_event.is_set():
                            break
                        
                        future = executor.submit(
                            process_chunk_worker, 
                            chunk_data, 
                            chunk_idx, 
                            member.name
                        )
                        futures[future] = chunk_idx
                        bytes_processed += len(chunk_data)
                        chunk_idx += 1
                        
                        logger.debug(
                            f"{E['cpu']} Chunk {chunk_idx}: {human(bytes_processed)}/{human(member.size)} processados"
                        )
                    
                    # Coletar resultados
                    for future in as_completed(futures):
                        try:
                            records = future.result()
                            all_records.extend(records)
                        except Exception as e:
                            worker_idx = futures[future]
                            logger.error(f"{E['error']} Worker {worker_idx} falhou: {e}")
                
                # Salvar como Parquet (DISK, não RAM)
                if all_records:
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    chunk_file = RAW_CHUNKS_DIR / f"raw_{len(chunk_files):06d}_{ts}.parquet"
                    
                    try:
                        # Criar dataframe e salvar
                        import pandas as pd
                        df = pd.DataFrame(all_records, columns=["email", "nome", "origem", "data"])
                        
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, str(chunk_file), compression="snappy")
                        
                        chunk_files.append(chunk_file)
                        total_records += len(all_records)
                        
                        logger.info(f"{E['ok']} Chunk salvo: {chunk_file.name} ({len(all_records):,} emails)")
                    
                    except Exception as e:
                        log_error_detailed(e, f"Salvando chunk Parquet {chunk_file}", [
                            f"Verifique espaço em disco: {disk_usage()}",
                            "Arquivo pode estar muito grande"
                        ])
        
        # Deletar arquivo TAR original
        try:
            tar_path.unlink()
            logger.info(f"{E['clean']} TAR deletado: {tar_path.name}")
        except Exception as e:
            logger.warning(f"{E['warn']} Não foi possível deletar TAR: {e}")
    
    except Exception as e:
        log_error_detailed(e, f"Processando TAR {tar_path}", [
            "Arquivo TAR pode estar corrompido",
            f"Tente extrair manualmente: tar -tzf {tar_path}",
            "Verifique espaço em disco e permissões"
        ])
    
    logger.info(f"{E['ok']} TAR processado: {len(chunk_files)} chunks, {total_records:,} emails")
    return chunk_files

# ===== HUGGING FACE =====
def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    """Setup/verificar datasets no Hugging Face."""
    if not token:
        raise RuntimeError("HF_TOKEN não definido nas variáveis de ambiente")
    
    try:
        api = HfApi()
        who = api.whoami(token=token)
        user = who.get("name") or who.get("user")
        
        if not user:
            raise RuntimeError("Não foi possível determinar usuário HF")
        
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
                    logger.warning(f"{E['warn']} Criar repo: {str(e)[:100]}")
        
        return api, emails_repo, checkpoint_repo
    
    except Exception as e:
        log_error_detailed(e, "Setup Hugging Face", [
            "Verifique se HF_TOKEN está correto",
            "Gere token em: https://huggingface.co/settings/tokens",
            "Certifique-se de que tem conta HF Pro/Enterprise"
        ])
        raise

def hf_upload_file(api: HfApi, token: str, repo_id: str, 
                   local_path: Path, repo_path: str) -> bool:
    """Upload arquivo para HF com retry."""
    if not local_path.exists():
        logger.warning(f"{E['warn']} Arquivo não existe: {local_path}")
        return False
    
    file_size = local_path.stat().st_size
    max_retries = 3
    
    logger.info(f"{E['upload']} Iniciando upload: {repo_path} ({human(file_size)})")
    
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
            logger.warning(f"{E['warn']} Upload tentativa {attempt + 1}/{max_retries} falhou")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                logger.info(f"{E['clock']} Aguardando {wait_time}s antes de retry...")
                time.sleep(wait_time)
    
    log_error_detailed(
        Exception(f"Upload falhou após {max_retries} tentativas"),
        f"Upload HF: {repo_path}",
        [
            "Arquivo pode estar muito grande",
            "Verifique quota em Hugging Face",
            "Tente fazer upload manual via interface web"
        ]
    )
    return False

def hf_download_checkpoint(api: HfApi, token: str, 
                          checkpoint_repo: str, local_path: Path) -> bool:
    """Download checkpoint do HF se existir."""
    try:
        logger.info(f"{E['download']} Tentando baixar checkpoint de {checkpoint_repo}...")
        
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="state.json",
            local_dir=str(local_path.parent),
            token=token,
            repo_type="dataset",
        )
        
        logger.info(f"{E['ok']} Checkpoint baixado com sucesso")
        return True
    
    except Exception as e:
        logger.info(f"{E['info']} Sem checkpoint anterior (iniciando fresh): {str(e)[:60]}")
        return False

def hf_download_duckdb(api: HfApi, token: str, 
                       checkpoint_repo: str, local_path: Path) -> bool:
    """Download database DuckDB do HF."""
    try:
        logger.info(f"{E['download']} Tentando baixar DuckDB database...")
        
        api.hf_hub_download(
            repo_id=checkpoint_repo,
            filename="emails.duckdb",
            local_dir=str(local_path.parent),
            token=token,
            repo_type="dataset",
        )
        
        logger.info(f"{E['ok']} DuckDB baixado com sucesso")
        return True
    
    except Exception as e:
        logger.info(f"{E['info']} Sem DuckDB anterior: {str(e)[:60]}")
        return False

# ===== FASES PRINCIPAIS =====
def phase1_download_torrents(session: lt.session, magnets: List[Dict]) -> Dict[str, Tuple]:
    """
    FASE 1: Download de torrents (simultâneo).
    RETORNA: dict {name: (handle, info, indices)}
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['download']} FASE 1: Download {len(magnets)} torrents simultâneos")
    logger.info(f"{'='*70}\n")
    
    completed = {}
    
    def download_single(item):
        name = item["name"]
        magnet = item["magnet"]
        targets = item.get("targets", [])
        
        try:
            logger.info(f"{E['download']} Iniciando: {name}")
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)
            
            # Aguardar metadata
            metadata_wait = 0
            while not handle.has_metadata() and not stop_event.is_set():
                metadata_wait += 1
                if metadata_wait % 10 == 0:
                    logger.debug(f"{E['clock']} Aguardando metadata de {name}... ({metadata_wait}s)")
                time.sleep(POLL_INTERVAL)
            
            if stop_event.is_set():
                raise KeyboardInterrupt()
            
            info = handle.get_torrent_info()
            found, missing = find_target_indices(info, targets)
            
            if missing:
                logger.error(f"{E['error']} Alvo(s) não encontrado(s) em {name}: {missing}")
                raise RuntimeError(f"Targets ausentes no metadata")
            
            # Definir prioridades
            nfiles = info.num_files()
            for i in range(nfiles):
                handle.file_priority(i, 7 if i in found else 0)
            
            logger.info(f"{E['ok']} {name} pronto | Alvo(s): {found}")
            return (name, (handle, info, found))
        
        except Exception as e:
            log_error_detailed(e, f"Download torrent {name}", [
                "Verifique conectividade com trackers",
                "Magnet link pode estar inválido",
                "Tente nova execução depois"
            ])
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
    
    logger.info(f"\n{E['ok']} FASE 1 concluída: {len(completed)}/{len(magnets)} torrents prontos\n")
    return completed

def phase2_wait_downloads(completed_torrents: Dict, state: Dict) -> List[Tuple]:
    """
    FASE 2: Aguardar conclusão dos downloads.
    RETORNA: list [(name, local_path, info), ...]
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['download']} FASE 2: Aguardando conclusão dos downloads")
    logger.info(f"{'='*70}\n")
    
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
                logger.info(f"{E['ok']} Pulando (já processado): {file_key}")
                continue
            
            expected_size = info.files().at(idx).size
            logger.info(f"{E['download']} Esperando: {tname} [idx:{idx}] ({human(expected_size)})")
            
            try:
                wait_for_file_complete(handle, idx, expected_size)
                local_path = local_path_for_index(SAVE_PATH, info, idx)
                
                if not local_path.exists():
                    logger.error(f"{E['error']} Arquivo não encontrado no disco: {local_path}")
                    continue
                
                all_files.append((tname, local_path, info))
                
                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
            
            except Exception as e:
                log_error_detailed(e, f"Aguardando download {file_key}", [
                    "Conexão pode ter sido perdida",
                    "Tente retomar a execução",
                    "Se persistir, considere reiniciar torrent"
                ])
    
    logger.info(f"\n{E['ok']} FASE 2 concluída: {len(all_files)} arquivos prontos\n")
    return all_files

def phase3_process_tars(tars: List[Tuple], state: Dict) -> List[Path]:
    """
    FASE 3: Processar TAR.GZ em STREAMING (zero RAM para dados).
    RETORNA: list [Path(chunk1.parquet), Path(chunk2.parquet), ...]
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['extract']} FASE 3: Processar {len(tars)} arquivos TAR")
    logger.info(f"{'='*70}\n")
    
    all_chunks = []
    processed_tars = state.get("processed_tars", [])
    
    for tname, tar_path, info in tars:
        if stop_event.is_set():
            break
        
        if str(tar_path) in processed_tars:
            logger.info(f"{E['ok']} Pulando (já processado): {tar_path.name}")
            continue
        
        chunks = process_tar_with_disk_streaming(tar_path, tname)
        all_chunks.extend(chunks)
        
        processed_tars.append(str(tar_path))
        state["processed_tars"] = processed_tars
        save_state(state)
    
    logger.info(f"\n{E['ok']} FASE 3 concluída: {len(all_chunks)} chunks gerados\n")
    return all_chunks

def phase4_load_to_duckdb(chunks: List[Path], 
                          conn: duckdb.DuckDBPyConnection, 
                          state: Dict) -> int:
    """
    FASE 4: Carregar chunks em DuckDB (disco, não RAM).
    RETORNA: total de registros inseridos
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['db']} FASE 4: Carregar {len(chunks)} chunks em DuckDB")
    logger.info(f"{'='*70}\n")
    
    import pandas as pd
    
    total_inserted = 0
    loaded_chunks = state.get("loaded_chunks", [])
    
    for i, chunk_file in enumerate(chunks, 1):
        if stop_event.is_set():
            break
        
        if str(chunk_file) in loaded_chunks:
            logger.info(f"{E['ok']} [{i}/{len(chunks)}] Chunk já carregado: {chunk_file.name}")
            continue
        
        try:
            logger.info(f"{E['db']} [{i}/{len(chunks)}] Carregando: {chunk_file.name}...")
            
            df = pd.read_parquet(chunk_file)
            records = [tuple(row) for row in df.itertuples(index=False, name=None)]
            inserted = batch_insert_duckdb_streaming(conn, records)
            total_inserted += inserted
            
            loaded_chunks.append(str(chunk_file))
            state["loaded_chunks"] = loaded_chunks
            save_state(state)
            
            logger.info(f"{E['ok']} [{i}/{len(chunks)}] Carregado: +{inserted:,} records")
        
        except Exception as e:
            log_error_detailed(e, f"Carregando chunk {chunk_file}", [
                "Arquivo Parquet pode estar corrompido",
                f"Tente deletar: rm {chunk_file}",
                "O chunk será regenerado na próxima execução"
            ])
    
    logger.info(f"\n{E['ok']} FASE 4 concluída: {total_inserted:,} records inseridos\n")
    return total_inserted

def phase5_deduplicate_duckdb(conn: duckdb.DuckDBPyConnection) -> int:
    """
    FASE 5: Deduplicação global (SELECT DISTINCT em disco).
    RETORNA: total de emails únicos após dedup
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['db']} FASE 5: Deduplicação global (SELECT DISTINCT)")
    logger.info(f"{'='*70}\n")
    
    try:
        count_before = conn.execute(
            "SELECT COUNT(*) FROM emails_raw;"
        ).fetchone()[0]
        
        logger.info(f"{E['stats']} Records antes de dedup: {count_before:,}")
        
        # Deduplicação via SELECT DISTINCT
        logger.info(f"{E['db']} Executando SELECT DISTINCT (em disco)...")
        conn.execute("""
            CREATE TABLE emails_dedup AS 
            SELECT DISTINCT * FROM emails_raw;
        """)
        
        conn.execute("DROP TABLE emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()
        
        count_after = conn.execute(
            "SELECT COUNT(*) FROM emails_raw;"
        ).fetchone()[0]
        
        duplicates = count_before - count_after
        dedup_pct = (duplicates / count_before * 100) if count_before > 0 else 0
        
        logger.info(f"{E['stats']} Records após dedup: {count_after:,}")
        logger.info(f"{E['email']} Duplicatas removidas: {duplicates:,} ({dedup_pct:.1f}%)")
        
        logger.info(f"\n{E['ok']} FASE 5 concluída: {count_after:,} emails únicos\n")
        return count_after
    
    except Exception as e:
        log_error_detailed(e, "Deduplicação DuckDB", [
            "Verifique espaço em disco (precisa de 2x do tamanho original)",
            "Reduzir BATCH_INSERT_DDB se RAM estiver limitada",
            "Tente manualmente: DELETE FROM emails_raw WHERE rowid NOT IN (SELECT MIN(rowid) FROM emails_raw GROUP BY email)"
        ])
        return 0

def phase6_export_final_files(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    """
    FASE 6: Exportar datasets finais (10M linhas cada em Parquet).
    RETORNA: list [Trader_Emails_001.parquet, ...]
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['email']} FASE 6: Gerar datasets finais ({ROWS_PER_FINAL_FILE:,} rows cada)")
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
                logger.info(f"{E['ok']} Todos os registros exportados")
                break
            
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            final_file = EXPORT_DIR / f"Trader_Emails_{file_num:03d}_{ts}.parquet"
            
            # Exportar para Parquet (comprimido)
            table = pa.Table.from_pandas(rows_df)
            pq.write_table(table, str(final_file), compression="snappy")
            
            final_files.append(final_file)
            file_size = final_file.stat().st_size
            
            logger.info(f"{E['ok']} [{file_num}] Exportado: {final_file.name} ({human(file_size)}) "
                       f"[{rows_df.shape[0]:,} rows]")
            
            file_num += 1
            offset += ROWS_PER_FINAL_FILE
        
        except Exception as e:
            log_error_detailed(e, f"Exportando arquivo final {file_num}", [
                "Verifique espaço em disco suficiente",
                f"Espaço disponível: {disk_usage()['free']}",
                "Reduza ROWS_PER_FINAL_FILE se necessário"
            ])
            break
    
    logger.info(f"\n{E['ok']} FASE 6 concluída: {len(final_files)} datasets finais\n")
    return final_files

def phase7_upload_hf(api: HfApi, token: str, emails_repo: str, 
                     checkpoint_repo: str, final_files: List[Path], 
                     db_path: Path, state: Dict):
    """
    FASE 7: Upload para HF + atualizar checkpoint.
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"{E['upload']} FASE 7: Upload Hugging Face + Checkpoint")
    logger.info(f"{'='*70}\n")
    
    # Upload final datasets
    for i, final_file in enumerate(final_files, 1):
        if stop_event.is_set():
            break
        
        repo_path = f"Trader_Emails/{final_file.name}"
        logger.info(f"{E['upload']} [{i}/{len(final_files)}] Uploading: {repo_path}")
        
        if hf_upload_file(api, token, emails_repo, final_file, repo_path):
            try:
                final_file.unlink()
                logger.info(f"{E['clean']} Arquivo local deletado: {final_file.name}")
            except Exception:
                pass
    
    # Upload checkpoint
    logger.info(f"{E['upload']} Uploading checkpoint...")
    hf_upload_file(api, token, checkpoint_repo, STATE_PATH, "state.json")
    
    if db_path.exists():
        logger.info(f"{E['upload']} Uploading DuckDB database...")
        hf_upload_file(api, token, checkpoint_repo, db_path, "emails.duckdb")
    
    state["last_execution"] = datetime.now(timezone.utc).isoformat()
    state["final_files_uploaded"] = len(final_files)
    save_state(state)
    
    logger.info(f"\n{E['ok']} FASE 7 concluída: Checkpoint salvo em HF\n")

# ===== MAIN =====
def main():
    """Orquestração principal."""
    logger.info(f"\n{'#'*70}")
    logger.info(f"# {E['start']} MINERADOR PRODUCTION V2 - Otimizado para Disco")
    logger.info(f"{'#'*70}\n")
    
    logger.info(f"{E['info']} SAVE_PATH: {SAVE_PATH}")
    logger.info(f"{E['cpu']} CPU cores: {os.cpu_count()}")
    logger.info(f"{E['space']} Uso de disco: {disk_usage()}")
    logger.info(f"{E['db']} DuckDB temp: {TEMP_DIR}")
    logger.info(f"{E['info']} Logs: {LOG_PATH}")
    logger.info(f"{E['error']} Erros detalhados: {ERROR_LOG_PATH}\n")
    
    # Verificar HF_TOKEN
    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN não definido")
        sys.exit(2)
    
    # Setup HF
    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.error(f"{E['error']} Setup HF falhou")
        sys.exit(1)
    
    # Download checkpoint
    logger.info(f"{E['download']} Baixando checkpoint anterior...")
    hf_download_checkpoint(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    hf_download_duckdb(api, HF_TOKEN, checkpoint_repo, SAVE_PATH)
    
    state = load_state()
    logger.info(f"{E['ok']} Estado carregado: {len(state)} entradas\n")
    
    # Inicializar DuckDB
    try:
        conn = init_duckdb(DB_PATH)
    except Exception as e:
        logger.error(f"{E['error']} DuckDB init falhou")
        sys.exit(1)
    
    # Inicializar libtorrent
    try:
        session = create_libtorrent_session()
    except Exception as e:
        logger.error(f"{E['error']} Libtorrent init falhou")
        sys.exit(1)
    
    total_emails = 0
    
    try:
        overall_start = time.time()
        
        # FASES
        completed_torrents = phase1_download_torrents(session, MAGNETS)
        
        if not completed_torrents:
            logger.error(f"{E['error']} Nenhum torrent completado com sucesso")
            return
        
        if stop_event.is_set():
            logger.warning(f"{E['warn']} Interrupção durante FASE 1")
            return
        
        tars = phase2_wait_downloads(completed_torrents, state)
        
        if tars and not stop_event.is_set():
            chunks = phase3_process_tars(tars, state)
            
            if chunks and not stop_event.is_set():
                phase4_load_to_duckdb(chunks, conn, state)
                
                if not stop_event.is_set():
                    total_emails = phase5_deduplicate_duckdb(conn)
                    
                    if not stop_event.is_set():
                        final_files = phase6_export_final_files(conn)
                        
                        if not stop_event.is_set():
                            phase7_upload_hf(api, HF_TOKEN, emails_repo, checkpoint_repo, 
                                           final_files, DB_PATH, state)
        
        total_time = time.time() - overall_start
        logger.info(f"\n{'='*70}")
        logger.info(f"{E['ok']} Minerador concluído com SUCESSO")
        logger.info(f"{E['clock']} Tempo total: {total_time / 60:.2f} minutos")
        logger.info(f"{E['email']} Emails únicos: {total_emails:,}")
        logger.info(f"{E['stats']} Disco final: {disk_usage()}")
        logger.info(f"{'='*70}\n")
    
    except KeyboardInterrupt:
        logger.warning(f"\n{E['warn']} Encerrando gracefully...")
    
    except Exception as e:
        log_error_detailed(e, "Execução principal", [
            "Verifique os logs detalhados para mais informações",
            f"Arquivo de erros: {ERROR_LOG_PATH}",
            "Tente retomar a execução (tem checkpoint)"
        ])
        sys.exit(1)
    
    finally:
        try:
            conn.close()
            logger.info(f"{E['ok']} Conexão DuckDB fechada")
        except Exception:
            pass

if __name__ == "__main__":
    main()
