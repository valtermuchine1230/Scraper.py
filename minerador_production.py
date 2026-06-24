#!/usr/bin/env python3
"""
minerador_production.py — Escalável a bilhões de emails com tolerância a falhas.

INSTALAÇÃO DE DEPENDÊNCIAS:
    pip install -r requirements.txt
    pip install mmh3

ARQUITETURA:
  FASE 1: Download 5 torrents simultâneos
  FASE 2: Checkpoint torrents no HF
  FASE 3: Processar com mmap + regex + ProcessPoolExecutor + STREAMING
  FASE 4: Gerar raw_chunk_*.parquet (streaming incremental)
  FASE 5: Filtrar domínios descartáveis
  FASE 6: DuckDB com SELECT DISTINCT (deduplicação global)
  FASE 7: Gerar Trader_Emails_*.parquet (30M linhas/arquivo)
  FASE 8: Upload HF + atualizar checkpoint para próxima run

PERSISTÊNCIA TOTAL:
  - Bloom Filter → Valter3B/bloom_filter (bloom_filter.bin + bloom_meta.json + bloom_count.txt)
  - Checkpoint   → Valter3B/minerador_checkpoints (state.json + emails.duckdb + processed_chunks.json + torrent_state.json)
  - Upload: a cada X minutos, após cada chunk, antes de encerrar, em SIGTERM, SIGINT, finally
  - Download: na inicialização, com verificação de integridade
  - Nunca reinicia do zero se existir checkpoint remoto

BLOOM FILTER (DEDUPLICAÇÃO EM DISCO — MMAP PURE PYTHON):
  - Implementação: BloomFilterDisk (mmap nativo + mmh3)
  - Capacidade: 1.500.000.000 entradas | Taxa de erro: 0.1%
  - RAM usada pelo BF: ~200-500 MB geridos dinamicamente pelo SO
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
import mmap
import math
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
    from huggingface_hub.utils import RepositoryNotFoundError
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
    import mmh3
except ImportError:
    print("❌ ERROR: mmh3 not installed")
    print("   Executar: pip install mmh3")
    sys.exit(1)

try:
    from rich.logging import RichHandler
    from rich.console import Console
except ImportError:
    print("❌ ERROR: rich not installed")
    print("   Executar: pip install 'rich>=13.0.0'")
    sys.exit(1)


# =====================================================================
# 🔀 BLOOM FILTER DISK NATIVO (100% Python + OS mmap)
# =====================================================================
class BloomFilterDisk:
    """
    Bloom Filter implementado nativamente com mmap e mmh3.
    Altamente estável para GitHub Actions e perfeitamente seguro para a RAM.
    """
    def __init__(self, capacity: int, error_rate: float, filename: str):
        self.capacity   = capacity
        self.error_rate = error_rate
        self.filename   = filename

        self.num_bits   = -int((capacity * math.log(error_rate)) / (math.log(2) ** 2))
        self.num_hashes = int((self.num_bits / capacity) * math.log(2))
        self.num_bytes  = (self.num_bits + 7) // 8
        # 8 bytes de cabeçalho para persistir contagem exata
        self.file_size  = 8 + self.num_bytes

        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                f.seek(self.file_size - 1)
                f.write(b"\0")

        self.file = open(filename, "r+b")
        self.mmap = mmap.mmap(self.file.fileno(), self.file_size, access=mmap.ACCESS_WRITE)

    def _get_hashes(self, item: str) -> list:
        h1, h2 = mmh3.hash64(item.encode("utf-8"))
        h1 &= 0xFFFFFFFFFFFFFFFF
        h2 &= 0xFFFFFFFFFFFFFFFF
        return [(h1 + i * h2) % self.num_bits for i in range(self.num_hashes)]

    def add(self, item: str):
        for bit_idx in self._get_hashes(item):
            byte_idx   = 8 + (bit_idx // 8)
            bit_offset = bit_idx % 8
            b = self.mmap[byte_idx]
            self.mmap[byte_idx] = b | (1 << bit_offset)
        current = int.from_bytes(self.mmap[0:8], byteorder="little")
        self.mmap[0:8] = (current + 1).to_bytes(8, byteorder="little")

    def __contains__(self, item: str) -> bool:
        for bit_idx in self._get_hashes(item):
            byte_idx   = 8 + (bit_idx // 8)
            bit_offset = bit_idx % 8
            if not (self.mmap[byte_idx] & (1 << bit_offset)):
                return False
        return True

    def __len__(self) -> int:
        return int.from_bytes(self.mmap[0:8], byteorder="little")

    def flush(self):
        """Força o sync do SO para disco."""
        self.mmap.flush()

    def close(self):
        """Força o sync e fecha o ficheiro com segurança."""
        self.mmap.flush()
        self.mmap.close()
        self.file.close()


# =====================================================================
# ⚙️  CONFIGURAÇÃO
# =====================================================================
SAVE_PATH     = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR      = SAVE_PATH / "exports"
TEMP_DIR        = SAVE_PATH / "temp"
RAW_CHUNKS_DIR  = SAVE_PATH / "raw_chunks"

DB_PATH                = SAVE_PATH / "emails.duckdb"
STATE_PATH             = SAVE_PATH / "state.json"
LOG_PATH               = SAVE_PATH / "minerador.log"
PROCESSED_CHUNKS_PATH  = SAVE_PATH / "processed_chunks.json"
TORRENT_STATE_PATH     = SAVE_PATH / "torrent_state.json"

# Bloom filter — 3 ficheiros obrigatórios no HF
BLOOM_FILTER_PATH = SAVE_PATH / "bloom_filter.bin"
BLOOM_META_PATH   = SAVE_PATH / "bloom_meta.json"
BLOOM_COUNT_PATH  = SAVE_PATH / "bloom_count.txt"

for d in [EXPORT_DIR, TEMP_DIR, RAW_CHUNKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

HF_TOKEN              = os.environ.get("HF_TOKEN")
HF_REPO_EMAILS        = os.environ.get("HF_REPO_EMAILS",        "Trader_Emails")
HF_REPO_CHECKPOINT    = os.environ.get("HF_REPO_CHECKPOINT",    "minerador_checkpoints")
HF_REPO_BLOOM_SUFFIX  = os.environ.get("HF_REPO_BLOOM_SUFFIX",  "bloom_filter")

# Upload periódico (minutos)
CHECKPOINT_INTERVAL_MIN = int(os.environ.get("CHECKPOINT_INTERVAL_MIN", "15"))

POLL_INTERVAL       = int(os.environ.get("POLL_INTERVAL",     "3"))
LOG_LEVEL           = os.environ.get("LOG_LEVEL",             "INFO")
BATCH_INSERT_DDB    = int(os.environ.get("BATCH_INSERT_DDB",  "500000"))
ROWS_PER_FINAL_FILE = int(os.environ.get("ROWS_PER_FINAL_FILE","30000000"))
CHUNK_SIZE          = int(os.environ.get("CHUNK_SIZE",         str(64 * 1024 * 1024)))
MIN_FREE_BYTES      = int(os.environ.get("MIN_FREE_BYTES",     str(512 * 1024 * 1024)))
ROWS_PER_PARQUET    = int(os.environ.get("ROWS_PER_PARQUET",   "5000000"))

BLOOM_CAPACITY   = 1_500_000_000
BLOOM_ERROR_RATE = 0.001

# Variáveis globais dinâmicas
HF_REPO_BLOOM:   Optional[str]           = None
bloom_filter:    Optional[BloomFilterDisk] = None
bloom_lock  = Lock()
stop_event  = Event()
state_lock  = Lock()

# Referências globais para uso em signal handlers e threads periódicas
_g_api:              Optional[HfApi] = None
_g_token:            Optional[str]   = None
_g_checkpoint_repo:  Optional[str]   = None
_g_periodic_timer:   Optional[threading.Timer] = None

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

# =====================================================================
# 🎨 EMOJIS
# =====================================================================
E = {
    "start":      "▶️",
    "download":   "📥",
    "extract":    "⏬",
    "stats":      "📙",
    "space":      "🔽",
    "email":      "📧",
    "upload":     "📨",
    "clean":      "♻️",
    "warn":       "⚠️",
    "error":      "❌",
    "ok":         "✅",
    "info":       "🔈",
    "cpu":        "⏯",
    "db":         "💳",
    "bloom":      "🔀",
    "skip":       "🚫",
    "checkpoint": "📩",
    "signal":     "❕",
    "integrity":  "❇️",
    "loop":       "➿",
}

EMAIL_REGEX = re.compile(rb"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", re.IGNORECASE)

# =====================================================================
# 🌐 MAGNETS
# =====================================================================
MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": (
            "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD"
            "&dn=Collection%20%232-%235%20%26%20Antipublic"
            "&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce"
            "&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce"
            "&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce"
            "&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce"
            "&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce"
            "&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce"
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

# =====================================================================
# 🚫 DISPOSABLE DOMAINS
# =====================================================================
DISPOSABLE_DOMAINS = {
    "tempmail.com","temp-mail.org","10minutemail.com","throwaway.email",
    "guerrillamail.com","mailinator.com","yopmail.com","maildrop.cc",
    "trashmail.com","fakeinbox.com","mailnesia.com","tempmail.email",
    "sharklasers.com","spam4.me","spamgourmet.com","tempmail.us",
    "mytrashmail.com","mailnesia.net","temporary-mail.net",
    "grr.la","temp-mail.io","tempmail24.com","maildisposable.com",
    "temp-mail.info","minute-mail.com","trash-mail.com",
    "10minutemailbox.com","tempmail.it","fakeemail.net",
    "mailbox.ga","oneclickmail.com","temp.email","trashmail.ws",
    "temp.mail","speedymail.org","emailondeck.com","schrott.email",
    "mail1.eu","tempmail.pro","temp-mailbox.com","mailtest.in",
    "gmail.com","googlemail.com","yahoo.com","ymail.com",
    "hotmail.com","outlook.com","live.com","msn.com",
    "aol.com","mail.com","inbox.com","fastmail.com",
    "protonmail.com","tutanota.com","zoho.com","mail.ru",
    "rambler.ru","yandex.com","yandex.ru","mail.ua",
    "ukr.net","qq.com","163.com","126.com",
    "sina.com","sohu.com","foxmail.com","tom.com",
    "vip.qq.com","vip.sina.com","163.net","126.net",
}


# =====================================================================
# ❕ SIGNAL HANDLERS
# =====================================================================
def handle_signal(signum, frame):
    """Trata SIGINT e SIGTERM: seta stop_event, checkpoint é salvo no finally."""
    logger.warning(f"{E['signal']} Signal {signum} recebido — a encerrar com segurança...")
    stop_event.set()


signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# =====================================================================
# 🔈 UTILITIES
# =====================================================================
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


# =====================================================================
# 📩 GESTÃO DE ESTADO
# =====================================================================
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
    """Salva estado dos chunks processados em ficheiro separado para o HF."""
    data = {
        "loaded_chunks":  state.get("loaded_chunks",  []),
        "processed_tars": state.get("processed_tars", []),
        "last_updated":   datetime.now(timezone.utc).isoformat(),
    }
    with open(PROCESSED_CHUNKS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def save_torrent_state(state: Dict[str, Any]):
    """Salva estado dos torrents em ficheiro separado para o HF."""
    data = {
        "downloaded_files": state.get("downloaded_files", {}),
        "last_updated":     datetime.now(timezone.utc).isoformat(),
    }
    with open(TORRENT_STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


def merge_checkpoint_into_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Funde os ficheiros de checkpoint descarregados no estado principal."""
    if PROCESSED_CHUNKS_PATH.exists():
        try:
            with open(PROCESSED_CHUNKS_PATH) as f:
                c = json.load(f)
            state.setdefault("loaded_chunks",  c.get("loaded_chunks",  []))
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

    return state


# =====================================================================
# ❇️  VERIFICAÇÃO DE INTEGRIDADE
# =====================================================================
def verify_file_integrity(path: Path, min_size: int = 10) -> bool:
    """
    Verifica se um ficheiro existe e tem tamanho mínimo aceitável.
    Retorna True se OK, False se inválido.
    """
    if not path.exists():
        logger.warning(f"{E['integrity']} Ficheiro não encontrado: {path.name}")
        return False
    size = path.stat().st_size
    if size < min_size:
        logger.warning(
            f"{E['integrity']} Ficheiro suspeito (muito pequeno): "
            f"{path.name} ({size} bytes < mínimo {min_size} bytes)"
        )
        return False
    logger.info(f"{E['integrity']} Integridade OK: {path.name} ({human(size)})")
    return True


# =====================================================================
# 🔀 BLOOM FILTER — Metadados e persistência
# =====================================================================
def save_bloom_meta():
    """Salva os metadados do Bloom Filter (bloom_meta.json e bloom_count.txt)."""
    global bloom_filter
    if bloom_filter is None:
        return
    meta = {
        "capacity":   bloom_filter.capacity,
        "error_rate": bloom_filter.error_rate,
        "num_bits":   bloom_filter.num_bits,
        "num_hashes": bloom_filter.num_hashes,
        "num_bytes":  bloom_filter.num_bytes,
        "file_size":  bloom_filter.file_size,
        "saved_at":   datetime.now(timezone.utc).isoformat(),
    }
    with open(BLOOM_META_PATH, "w") as f:
        json.dump(meta, f, indent=2)
    with open(BLOOM_COUNT_PATH, "w") as f:
        f.write(str(len(bloom_filter)))


# =====================================================================
# 📥 DOWNLOAD DE FICHEIROS DO HUGGING FACE
# =====================================================================
def _hf_download_single(
    api:       HfApi,
    token:     str,
    repo_id:   str,
    filename:  str,
    local_dir: Path,
    repo_type: str = "dataset",
) -> Optional[Path]:
    """
    Descarrega um único ficheiro do HF.
    Retorna o Path local se OK, None se não existir ou falhar.
    """
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
        # hf_hub_download pode colocar noutro caminho; garantir localização correcta
        downloaded = Path(local_file)
        if downloaded != local_path and downloaded.exists():
            shutil.copy2(downloaded, local_path)

        if not verify_file_integrity(local_path):
            return None

        return local_path
    except Exception:
        return None


def load_checkpoint_from_hf(api: HfApi, token: str, checkpoint_repo: str) -> Dict[str, bool]:
    """
    Descarrega todos os ficheiros de checkpoint do HF na inicialização.
    Escreve nos logs os ficheiros recuperados.

    Ficheiros obrigatórios:
      - state.json
      - emails.duckdb
      - processed_chunks.json
      - torrent_state.json
    """
    logger.info(f"{E['download']} A recuperar checkpoint do HF ({checkpoint_repo})...")

    files = [
        ("state.json",            STATE_PATH),
        ("emails.duckdb",         DB_PATH),
        ("processed_chunks.json", PROCESSED_CHUNKS_PATH),
        ("torrent_state.json",    TORRENT_STATE_PATH),
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
            logger.info(f"{E['info']} {filename} não encontrado no HF — será criado novo")

    if any_ok:
        logger.info("📥 Checkpoint recuperado do HF")

        # Log chunks recuperados
        if PROCESSED_CHUNKS_PATH.exists():
            try:
                with open(PROCESSED_CHUNKS_PATH) as f:
                    c = json.load(f)
                n_chunks = len(c.get("loaded_chunks", []))
                logger.info(f"📥 Recuperados {n_chunks} chunks processados")
            except Exception:
                pass
    else:
        logger.info(f"{E['info']} Nenhum checkpoint remoto encontrado — início do zero")

    return results


def load_bloom_from_hf(api: HfApi, token: str) -> bool:
    """
    Descarrega os ficheiros do Bloom Filter do HF na inicialização.

    Ficheiros obrigatórios:
      - bloom_filter.bin
      - bloom_meta.json
      - bloom_count.txt
    """
    global HF_REPO_BLOOM
    if not HF_REPO_BLOOM:
        return False

    logger.info(f"{E['bloom']} A recuperar Bloom Filter do HF ({HF_REPO_BLOOM})...")

    files = [
        ("bloom_filter.bin", BLOOM_FILTER_PATH),
        ("bloom_meta.json",  BLOOM_META_PATH),
        ("bloom_count.txt",  BLOOM_COUNT_PATH),
    ]

    bloom_ok = False
    for filename, local_path in files:
        result = _hf_download_single(api, token, HF_REPO_BLOOM, filename, SAVE_PATH)
        if result:
            if filename == "bloom_filter.bin":
                bloom_ok = True
            logger.info(f"{E['ok']} Descarregado: {filename} ({human(result.stat().st_size)})")
        else:
            logger.info(f"{E['info']} {filename} não encontrado no HF")

    if bloom_ok:
        logger.info("📥 Bloom Filter recuperado do HF")
        # Log emails conhecidos
        if BLOOM_COUNT_PATH.exists():
            try:
                count = int(BLOOM_COUNT_PATH.read_text().strip())
                logger.info(f"📥 Recuperados {count:,} emails já conhecidos")
            except Exception:
                pass
    else:
        logger.info(f"{E['info']} Bloom Filter não encontrado no HF — será criado novo")

    return bloom_ok


# =====================================================================
# 📨 UPLOAD PARA O HUGGING FACE
# =====================================================================
def _hf_upload_file(
    api:       HfApi,
    token:     str,
    repo_id:   str,
    local_path: Path,
    repo_path:  str,
    max_retries: int = 3,
) -> bool:
    """Upload de um único ficheiro para o HF com retry e verificação."""
    if not local_path.exists():
        logger.warning(f"{E['warn']} Ficheiro não encontrado para upload: {local_path}")
        return False

    size = local_path.stat().st_size
    if size == 0:
        logger.warning(f"{E['warn']} Ficheiro vazio, upload ignorado: {local_path.name}")
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
                f"{E['upload']} Upload OK: {repo_path} ({human(size)}) "
                f"→ {repo_id}"
            )
            return True
        except Exception as e:
            logger.warning(
                f"{E['warn']} Upload tentativa {attempt}/{max_retries} falhou: "
                f"{repo_path} — {str(e)[:100]}"
            )
            if attempt < max_retries:
                time.sleep(5 * attempt)

    logger.error(f"{E['error']} Upload falhou após {max_retries} tentativas: {repo_path}")
    return False


def upload_bloom_to_hf(api: HfApi, token: str) -> bool:
    """
    Faz upload dos 3 ficheiros do Bloom Filter para o HF:
      - bloom_filter.bin
      - bloom_meta.json
      - bloom_count.txt

    Retorna True se todos foram enviados com sucesso.
    """
    global bloom_filter, HF_REPO_BLOOM

    if bloom_filter is None or not HF_REPO_BLOOM:
        logger.warning(f"{E['warn']} upload_bloom_to_hf: bloom_filter ou HF_REPO_BLOOM não configurados")
        return False

    if not BLOOM_FILTER_PATH.exists():
        logger.warning(f"{E['warn']} bloom_filter.bin não encontrado em disco — upload impossível")
        return False

    # Flush mmap → garantir que disco tem dados actuais
    bloom_filter.flush()

    # Gerar metadados actualizados
    save_bloom_meta()

    bf_count = len(bloom_filter)
    bf_size  = BLOOM_FILTER_PATH.stat().st_size
    logger.info(
        f"{E['bloom']} A enviar Bloom Filter para HF "
        f"({bf_count:,} entradas | {bf_size/(1024**2):.1f} MB)..."
    )

    files = [
        (BLOOM_FILTER_PATH, "bloom_filter.bin"),
        (BLOOM_META_PATH,   "bloom_meta.json"),
        (BLOOM_COUNT_PATH,  "bloom_count.txt"),
    ]

    all_ok = True
    for local_path, repo_path in files:
        if not local_path.exists():
            continue
        ok = _hf_upload_file(api, token, HF_REPO_BLOOM, local_path, repo_path)
        if not ok:
            all_ok = False

    if all_ok:
        logger.info("✅ Bloom Filter enviado para HF")
    else:
        logger.warning(f"{E['warn']} Upload do Bloom Filter incompleto")

    return all_ok


def upload_checkpoint_to_hf(api: HfApi, token: str, checkpoint_repo: str) -> bool:
    """
    Faz upload dos 4 ficheiros de checkpoint para o HF:
      - state.json
      - emails.duckdb
      - processed_chunks.json
      - torrent_state.json

    Retorna True se todos os ficheiros existentes foram enviados com sucesso.
    """
    logger.info(f"{E['checkpoint']} A enviar checkpoint para HF ({checkpoint_repo})...")

    files = [
        (STATE_PATH,            "state.json"),
        (DB_PATH,               "emails.duckdb"),
        (PROCESSED_CHUNKS_PATH, "processed_chunks.json"),
        (TORRENT_STATE_PATH,    "torrent_state.json"),
    ]

    all_ok   = True
    uploaded = 0

    for local_path, repo_path in files:
        if not local_path.exists():
            logger.info(f"{E['info']} Ausente, a ignorar: {repo_path}")
            continue
        ok = _hf_upload_file(api, token, checkpoint_repo, local_path, repo_path)
        if ok:
            uploaded += 1
        else:
            all_ok = False

    if all_ok:
        logger.info("✅ Checkpoint enviado para HF")
        logger.info("✅ Estado persistido com sucesso")
    else:
        logger.warning(f"{E['warn']} Checkpoint parcialmente enviado ({uploaded} ficheiros)")

    return all_ok


def save_full_checkpoint(api: HfApi, token: str, checkpoint_repo: str):
    """
    Salva checkpoint completo: Bloom Filter + todos os ficheiros de estado.
    Chamado após cada chunk, periodicamente, em SIGTERM/SIGINT e no finally.
    """
    try:
        bloom_ok      = upload_bloom_to_hf(api, token)
        checkpoint_ok = upload_checkpoint_to_hf(api, token, checkpoint_repo)
        if bloom_ok and checkpoint_ok:
            logger.info("✅ Estado persistido com sucesso")
    except Exception as e:
        logger.exception(f"{E['error']} save_full_checkpoint falhou: {e}")


# =====================================================================
# ⏯  CHECKPOINT PERIÓDICO (background thread)
# =====================================================================
def _periodic_checkpoint_worker():
    """Função do timer periódico — faz checkpoint e reagenda."""
    global _g_periodic_timer

    if stop_event.is_set():
        return

    logger.info(f"{E['loop']} Checkpoint periódico automático ({CHECKPOINT_INTERVAL_MIN} min)...")
    if _g_api and _g_token and _g_checkpoint_repo:
        try:
            save_full_checkpoint(_g_api, _g_token, _g_checkpoint_repo)
        except Exception as e:
            logger.error(f"{E['error']} Checkpoint periódico falhou: {e}")

    # Reagendar se ainda não foi pedido stop
    if not stop_event.is_set():
        _g_periodic_timer = threading.Timer(
            CHECKPOINT_INTERVAL_MIN * 60,
            _periodic_checkpoint_worker,
        )
        _g_periodic_timer.daemon = True
        _g_periodic_timer.start()


def start_periodic_checkpoint(api: HfApi, token: str, checkpoint_repo: str):
    """Inicia o timer periódico de checkpoint em background."""
    global _g_api, _g_token, _g_checkpoint_repo, _g_periodic_timer

    _g_api             = api
    _g_token           = token
    _g_checkpoint_repo = checkpoint_repo

    _g_periodic_timer = threading.Timer(
        CHECKPOINT_INTERVAL_MIN * 60,
        _periodic_checkpoint_worker,
    )
    _g_periodic_timer.daemon = True
    _g_periodic_timer.start()
    logger.info(
        f"{E['ok']} Checkpoint periódico iniciado: a cada {CHECKPOINT_INTERVAL_MIN} minutos"
    )


def stop_periodic_checkpoint():
    """Para o timer periódico de checkpoint."""
    global _g_periodic_timer
    if _g_periodic_timer is not None:
        _g_periodic_timer.cancel()
        _g_periodic_timer = None
        logger.info(f"{E['ok']} Timer de checkpoint periódico cancelado")


# =====================================================================
# 🔀 BLOOM FILTER — Inicialização e carregamento completo
# =====================================================================
def init_bloom_filter(api: HfApi, token: str) -> BloomFilterDisk:
    """
    Inicializa o Bloom Filter na inicialização do sistema:
      1. Garante que o repositório HF existe
      2. Descarrega bloom_filter.bin do HF se existir
      3. Abre (ou cria) via mmap em disco
    Nunca carrega tudo para RAM — usa mmap nativo do SO.
    """
    global bloom_filter, HF_REPO_BLOOM

    if not HF_REPO_BLOOM:
        raise RuntimeError("HF_REPO_BLOOM não configurado — chamar hf_setup_datasets primeiro")

    # Garantir que o repositório HF existe
    try:
        api.create_repo(
            repo_id=HF_REPO_BLOOM,
            token=token,
            repo_type="dataset",
            private=True,
        )
        logger.info(f"{E['bloom']} Repositório HF criado: {HF_REPO_BLOOM}")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "409" in msg:
            logger.info(f"{E['bloom']} Repositório HF já existe: {HF_REPO_BLOOM}")
        else:
            logger.warning(f"{E['warn']} create_repo Bloom Filter: {str(e)[:120]}")

    # Descarregar bloom_filter.bin do HF (já feito em load_bloom_from_hf)
    # Se o ficheiro local existir, foi carregado do HF; senão será criado novo
    if BLOOM_FILTER_PATH.exists():
        bf_size = BLOOM_FILTER_PATH.stat().st_size
        logger.info(
            f"{E['bloom']} bloom_filter.bin encontrado em disco "
            f"({human(bf_size)}) — a abrir via mmap..."
        )
    else:
        logger.info(
            f"{E['bloom']} bloom_filter.bin não encontrado — "
            f"a criar novo (capacidade={BLOOM_CAPACITY:,}, erro={BLOOM_ERROR_RATE})"
        )

    bloom_filter = BloomFilterDisk(
        capacity=BLOOM_CAPACITY,
        error_rate=BLOOM_ERROR_RATE,
        filename=str(BLOOM_FILTER_PATH),
    )

    count = len(bloom_filter)
    size  = BLOOM_FILTER_PATH.stat().st_size if BLOOM_FILTER_PATH.exists() else 0
    logger.info(
        f"{E['bloom']} Bloom Filter mmap pronto: "
        f"~{count:,} entradas | {human(size)} em disco | ~200 MB RAM"
    )

    return bloom_filter


# =====================================================================
# 💳 DUCKDB
# =====================================================================
def init_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute("PRAGMA threads=8;")
    conn.execute("PRAGMA memory_limit='8GB';")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS emails_raw (
            email   VARCHAR PRIMARY KEY,
            nome    VARCHAR,
            origem  VARCHAR,
            data    VARCHAR
        );
    """)
    conn.commit()
    return conn


def batch_insert_duckdb(conn: duckdb.DuckDBPyConnection, records: List[Tuple]) -> int:
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
                pass
        conn.commit()
        return len(records)
    except Exception:
        logger.exception(f"{E['error']} DuckDB insert failed")
        conn.rollback()
        return 0


# =====================================================================
# ⏯  LIBTORRENT
# =====================================================================
def create_libtorrent_session() -> lt.session:
    try:
        session = lt.session()
        try:
            settings = lt.settings_pack()
            cpu_count = os.cpu_count() or 4
            settings.set_int("connections_limit",        min(cpu_count * 100,  800))
            settings.set_int("connections_limit_global", min(cpu_count * 500, 4000))
            settings.set_int("active_limit",             min(cpu_count * 50,   200))
            settings.set_int("request_queue_size", 1024)
            settings.set_int("cache_size", 4096)
            settings.set_bool("enable_dht", True)
            settings.set_bool("enable_lsd", True)
            settings.set_bool("enable_pex", True)
            settings.set_int("upload_rate_limit",   0)
            settings.set_int("download_rate_limit", 0)
            session.apply_settings(settings)
        except AttributeError:
            logger.info(f"{E['info']} Usando configuração libtorrent fallback")
        logger.info(f"{E['cpu']} Sessão libtorrent criada")
        return session
    except Exception:
        logger.exception(f"{E['error']} Falha ao criar sessão libtorrent")
        raise


def find_target_indices(
    torrent_info: lt.torrent_info,
    targets: List[str],
) -> Tuple[List[int], List[str]]:
    n = torrent_info.num_files()
    files_storage = torrent_info.files()

    file_catalog = {}
    for i in range(n):
        raw_path = files_storage.at(i).path
        norm_path = normalize_string_robust(raw_path)
        basename  = norm_path.split("/")[-1]
        file_catalog[i] = {
            "raw":      raw_path,
            "norm":     norm_path,
            "basename": basename,
            "size":     files_storage.at(i).size,
        }

    found_indices = set()
    missing_targets = []

    for t in targets:
        target_norm     = normalize_string_robust(t)
        target_basename = target_norm.split("/")[-1]
        matched = False

        for i, fdata in file_catalog.items():
            if fdata["norm"] == target_norm:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['ok']} Nível 1 (Exact): '{t}' → '{fdata['raw']}'")
                break

        if matched:
            continue

        for i, fdata in file_catalog.items():
            if fdata["basename"] == target_basename:
                found_indices.add(i)
                matched = True
                logger.info(f"{E['ok']} Nível 2 (Basename): '{t}' → '{fdata['raw']}'")
                break

        if matched:
            continue

        for i, fdata in file_catalog.items():
            if target_basename in fdata["norm"] or fdata["basename"] in target_norm:
                found_indices.add(i)
                matched = True
                logger.warning(f"{E['warn']} Nível 3 (Partial): '{t}' → '{fdata['raw']}'")
                break

        if not matched:
            missing_targets.append(t)
            logger.error(f"{E['error']} Sem correspondência para target: '{t}'")

    if missing_targets:
        logger.warning(
            f"{E['warn']} Ficheiros disponíveis no torrent ({torrent_info.name()}):"
        )
        for i, fdata in file_catalog.items():
            logger.warning(
                f"  → [{i}] '{fdata['raw']}' | "
                f"norm='{fdata['norm']}' | {human(fdata['size'])}"
            )

    return sorted(list(found_indices)), missing_targets


def local_path_for_index_robust(
    save_path:    Path,
    torrent_info: lt.torrent_info,
    index:        int,
) -> Optional[Path]:
    torrent_name = torrent_info.name()
    file_path    = torrent_info.files().at(index).path
    basename     = Path(file_path).name

    candidate1 = save_path / torrent_name / file_path
    if candidate1.exists() and candidate1.is_file():
        logger.info(f"{E['ok']} [L1] Encontrado: {candidate1}")
        return candidate1

    candidate2 = save_path / file_path
    if candidate2.exists() and candidate2.is_file():
        logger.info(f"{E['ok']} [L2] Encontrado: {candidate2}")
        return candidate2

    torrent_dir = save_path / torrent_name
    if torrent_dir.exists():
        for found in torrent_dir.rglob(basename):
            if found.is_file():
                logger.info(f"{E['ok']} [L3] Encontrado recursivo: {found}")
                return found

    for found in save_path.rglob(basename):
        if found.is_file():
            logger.info(f"{E['ok']} [L4] Encontrado global: {found}")
            return found

    logger.error(f"{E['error']} Ficheiro não encontrado em disco após busca exaustiva")
    logger.error(f"{E['error']} Torrent={torrent_name} | index={index} | file={file_path}")
    return None


def wait_for_file_complete(
    handle:        lt.torrent_handle,
    file_index:    int,
    expected_size: int,
) -> bool:
    last_log = 0
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()

        fprog = handle.file_progress()
        got   = fprog[file_index] if file_index < len(fprog) else 0
        pct   = (got / expected_size * 100) if expected_size else 0.0

        now = time.time()
        if now - last_log >= 5:
            logger.info(
                f"{E['download']} File[{file_index}]: "
                f"{got:,}/{expected_size:,} ({pct:.1f}%)"
            )
            last_log = now

        if expected_size and got >= expected_size:
            logger.info(f"{E['ok']} File {file_index} completo")
            return True

        time.sleep(POLL_INTERVAL)


# =====================================================================
# ⏬ PROCESSAMENTO
# =====================================================================
def process_chunk_worker(
    chunk_data: bytes,
    chunk_idx:  int,
    origin:     str,
) -> List[Tuple]:
    results  = []
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

            local_part = email.split("@")[0]
            local_part = re.sub(r"\d+",    "",  local_part)
            local_part = re.sub(r"[_.\\-]+", " ", local_part).strip()
            nome = " ".join(p.capitalize() for p in local_part.split()) if local_part else ""

            results.append((email, nome, origin, data_iso))
        except Exception as e:
            logger.exception(
                f"{E['error']} Worker chunk_idx={chunk_idx} origin={origin}: "
                f"{type(e).__name__}: {e}"
            )
            continue

    return results


def process_tar_with_mmap(
    tar_path: Path,
    origin:   str,
    api:      HfApi,
    token:    str,
    checkpoint_repo: str,
    state:    Dict[str, Any],
) -> List[Path]:
    """
    Extrai tar.gz com mmap + regex + ProcessPoolExecutor + STREAMING.
    Faz checkpoint após cada member processado.
    """
    cpu_count  = min(2, os.cpu_count() or 2)
    chunk_files: List[Path] = []

    logger.info(f"{E['extract']} Processando: {tar_path.name}")

    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar:
                if stop_event.is_set():
                    break

                if not member.isfile() or not (
                    member.name.endswith(".txt") or member.name.endswith(".csv")
                ):
                    continue

                logger.info(f"{E['extract']} Member: {member.name}")
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue

                gc.collect()

                writer: Optional[pq.ParquetWriter] = None
                schema               = None
                current_chunk_file   = None
                row_count            = 0
                chunk_batch_count    = 0
                bloom_skipped        = 0

                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    MAX_INFLIGHT = 4
                    inflight: set = set()
                    chunk_idx = 0

                    def check_memory():
                        mem = psutil.virtual_memory()
                        if mem.percent > 85:
                            logger.warning(
                                f"{E['warn']} RAM ALTA: {mem.percent}% | "
                                f"usada={mem.used/1024**3:.2f}GB | "
                                f"livre={mem.available/1024**3:.2f}GB"
                            )
                            time.sleep(2)

                    def drain_futures(inf):
                        done = {f for f in inf if f.done()}
                        records_out = None
                        for f in done:
                            inf.discard(f)
                            try:
                                r = f.result()
                                if r:
                                    records_out = r
                            except Exception as ex:
                                logger.exception(
                                    f"{E['error']} Worker falhou: "
                                    f"{type(ex).__name__}: {ex}"
                                )
                        return records_out

                    def yield_or_write(records):
                        nonlocal writer, schema, current_chunk_file
                        nonlocal row_count, chunk_batch_count, bloom_skipped

                        if not records:
                            return

                        safe_records = [
                            r for r in records
                            if isinstance(r, tuple) and len(r) == 4
                        ]
                        if not safe_records:
                            return

                        # ── 🔀 BLOOM FILTER ──────────────────────────────────
                        if bloom_filter is not None:
                            filtered = []
                            with bloom_lock:
                                for r in safe_records:
                                    email = r[0]
                                    if email not in bloom_filter:
                                        bloom_filter.add(email)
                                        filtered.append(r)
                                    else:
                                        bloom_skipped += 1
                            safe_records = filtered
                        # ─────────────────────────────────────────────────────

                        if not safe_records:
                            return

                        if writer is None:
                            current_chunk_file = (
                                RAW_CHUNKS_DIR /
                                f"raw_chunk_{len(chunk_files):06d}_{ts}.parquet"
                            )
                            schema = pa.schema([
                                pa.field("email",  pa.string()),
                                pa.field("nome",   pa.string()),
                                pa.field("origem", pa.string()),
                                pa.field("data",   pa.string()),
                            ])
                            writer = pq.ParquetWriter(
                                str(current_chunk_file), schema, compression="snappy"
                            )

                        table = pa.Table.from_arrays(
                            [
                                [r[0] for r in safe_records],
                                [r[1] for r in safe_records],
                                [r[2] for r in safe_records],
                                [r[3] for r in safe_records],
                            ],
                            names=["email", "nome", "origem", "data"],
                        )
                        count = len(safe_records)
                        writer.write_table(table)

                        del table
                        del safe_records
                        gc.collect()

                        row_count         += count
                        chunk_batch_count += 1

                        if chunk_batch_count % 10 == 0:
                            logger.info(
                                f"{E['email']} {member.name}: "
                                f"{row_count:,} emails | "
                                f"{E['skip']} {bloom_skipped:,} duplicatas"
                            )

                    while True:
                        chunk_data = fobj.read(CHUNK_SIZE)
                        if not chunk_data:
                            break
                        if stop_event.is_set():
                            break

                        check_memory()

                        while len(inflight) >= MAX_INFLIGHT:
                            records = drain_futures(inflight)
                            if records:
                                yield_or_write(records)

                        future = executor.submit(
                            process_chunk_worker, chunk_data, chunk_idx, member.name
                        )
                        inflight.add(future)
                        chunk_idx += 1

                    # Flush final
                    for f in list(inflight):
                        try:
                            r = f.result()
                            if r:
                                yield_or_write(r)
                        except Exception as ex:
                            logger.exception(
                                f"{E['error']} Worker flush falhou: "
                                f"{type(ex).__name__}: {ex}"
                            )

                if writer is not None:
                    writer.close()
                    chunk_files.append(current_chunk_file)
                    logger.info(
                        f"{E['ok']} Chunk finalizado: {current_chunk_file.name} "
                        f"({row_count:,} registos) | "
                        f"{E['skip']} {bloom_skipped:,} duplicatas pelo Bloom Filter"
                    )

                    # ── Checkpoint após cada chunk ─────────────────────────
                    state["processed_tars"] = state.get("processed_tars", [])
                    save_processed_chunks(state)
                    save_torrent_state(state)
                    save_full_checkpoint(api, token, checkpoint_repo)
                    # ──────────────────────────────────────────────────────

                if writer:
                    del writer
                if schema:
                    del schema
                gc.collect()

        try:
            tar_path.unlink()
        except Exception:
            pass

    except Exception as e:
        logger.exception(
            f"{E['error']} process_tar_with_mmap: "
            f"{type(e).__name__}: {e}"
        )

    return chunk_files


# =====================================================================
# 📨 HUGGING FACE — SETUP
# =====================================================================
def hf_setup_datasets(token: str) -> Tuple[HfApi, str, str]:
    """Setup / verificação dos datasets no Hugging Face."""
    global HF_REPO_BLOOM

    if not token:
        raise RuntimeError("HF_TOKEN não definido")

    api  = HfApi()
    who  = api.whoami(token=token)
    user = who.get("name") or who.get("user")

    if not user:
        raise RuntimeError("Não foi possível determinar o utilizador HF")

    emails_repo     = f"{user}/{HF_REPO_EMAILS}"
    checkpoint_repo = f"{user}/{HF_REPO_CHECKPOINT}"
    HF_REPO_BLOOM   = f"{user}/{HF_REPO_BLOOM_SUFFIX}"

    for repo_id in [emails_repo, checkpoint_repo, HF_REPO_BLOOM]:
        try:
            api.create_repo(
                repo_id=repo_id,
                token=token,
                repo_type="dataset",
                private=True,
            )
            logger.info(f"{E['ok']} Dataset criado: {repo_id}")
        except Exception as e:
            msg = str(e).lower()
            if "already exists" in msg or "409" in msg:
                logger.info(f"{E['ok']} Dataset já existe: {repo_id}")
            else:
                logger.warning(f"{E['warn']} create_repo {repo_id}: {str(e)[:100]}")

    return api, emails_repo, checkpoint_repo


# =====================================================================
# ▶️  FASES PRINCIPAIS
# =====================================================================
def phase1_download_torrents(
    session: lt.session,
    magnets: List[Dict],
) -> Dict[str, Tuple]:
    logger.info(
        f"{E['download']} FASE 1: Download de {len(magnets)} torrents em simultâneo"
    )

    completed = {}

    def download_single(item):
        name    = item["name"]
        magnet  = item["magnet"]
        targets = item.get("targets", [])
        try:
            logger.info(f"{E['download']} A iniciar: {name}")
            params = lt.parse_magnet_uri(magnet)
            params.save_path = str(SAVE_PATH)
            handle = session.add_torrent(params)

            while not handle.has_metadata() and not stop_event.is_set():
                time.sleep(POLL_INTERVAL)

            if stop_event.is_set():
                raise KeyboardInterrupt()

            info        = handle.get_torrent_info()
            found, miss = find_target_indices(info, targets)

            if miss:
                raise RuntimeError(f"Targets não encontrados: {miss}")

            nfiles = info.num_files()
            for i in range(nfiles):
                handle.file_priority(i, 7 if i in found else 0)

            logger.info(f"{E['ok']} {name} pronto | targets: {found}")
            return (name, (handle, info, found))
        except Exception:
            logger.exception(f"{E['error']} Torrent {name} falhou")
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

    logger.info(
        f"{E['ok']} FASE 1 concluída: {len(completed)}/{len(magnets)} torrents prontos"
    )
    return completed


def phase2_wait_downloads(
    completed_torrents: Dict,
    state:              Dict,
    api:                HfApi,
    token:              str,
    checkpoint_repo:    str,
) -> List[Tuple]:
    logger.info(f"{E['download']} FASE 2: A aguardar conclusão de todos os ficheiros")

    all_files    = []
    processed_key = state.get("downloaded_files", {})

    for tname, (handle, info, indices) in completed_torrents.items():
        if stop_event.is_set():
            break

        for idx in indices:
            if stop_event.is_set():
                break

            file_key = f"{tname}_{idx}"
            if file_key in processed_key:
                logger.info(f"{E['skip']} Ignorado (já processado): {file_key}")
                continue

            expected_size = info.files().at(idx).size
            logger.info(
                f"{E['download']} A aguardar: {tname} index {idx} "
                f"({human(expected_size)})"
            )

            try:
                wait_for_file_complete(handle, idx, expected_size)
                local_path = local_path_for_index_robust(SAVE_PATH, info, idx)

                if local_path is None:
                    logger.error(f"{E['error']} Ficheiro não encontrado em disco (index {idx})")
                    continue

                all_files.append((tname, local_path, info))

                processed_key[file_key] = True
                state["downloaded_files"] = processed_key
                save_state(state)
                save_torrent_state(state)

                # Checkpoint após cada ficheiro descarregado
                save_full_checkpoint(api, token, checkpoint_repo)

            except Exception as e:
                logger.exception(
                    f"{E['error']} phase2 {tname} index {idx}: "
                    f"{type(e).__name__}: {e}"
                )

    logger.info(f"{E['ok']} FASE 2 concluída: {len(all_files)} ficheiros prontos")
    return all_files


def phase3_process_tars(
    tars:            List[Tuple],
    state:           Dict,
    api:             HfApi,
    token:           str,
    checkpoint_repo: str,
) -> List[Path]:
    logger.info(f"{E['extract']} FASE 3: A processar {len(tars)} ficheiros tar")

    all_chunks    = []
    processed_tars = state.get("processed_tars", [])

    for tname, tar_path, info in tars:
        if stop_event.is_set():
            break

        if str(tar_path) in processed_tars:
            logger.info(f"{E['skip']} Ignorado (já processado): {tar_path.name}")
            continue

        chunks = process_tar_with_mmap(
            tar_path,
            tname,
            api,
            token,
            checkpoint_repo,
            state,
        )
        all_chunks.extend(chunks)

        processed_tars.append(str(tar_path))
        state["processed_tars"] = processed_tars
        save_state(state)
        save_processed_chunks(state)

        # Checkpoint após cada tar
        save_full_checkpoint(api, token, checkpoint_repo)

    logger.info(
        f"{E['ok']} FASE 3 concluída: {len(all_chunks)} raw chunks gerados"
    )
    return all_chunks


def phase4_load_to_duckdb(
    chunks:          List[Path],
    conn:            duckdb.DuckDBPyConnection,
    state:           Dict,
    api:             HfApi,
    token:           str,
    checkpoint_repo: str,
) -> int:
    logger.info(
        f"{E['db']} FASE 4: A carregar {len(chunks)} chunks para DuckDB"
    )

    total_inserted = 0
    loaded_chunks  = state.get("loaded_chunks", [])

    for chunk_file in chunks:
        if stop_event.is_set():
            break

        if str(chunk_file) in loaded_chunks:
            logger.info(f"{E['skip']} Chunk já carregado: {chunk_file.name}")
            continue

        try:
            result = conn.execute(f"""
                INSERT INTO emails_raw
                SELECT * FROM read_parquet('{chunk_file}')
                ON CONFLICT(email) DO NOTHING;
            """)
            conn.commit()
            inserted = result.rowcount if hasattr(result, "rowcount") else 0
            total_inserted += inserted

            loaded_chunks.append(str(chunk_file))
            state["loaded_chunks"] = loaded_chunks
            save_state(state)
            save_processed_chunks(state)

            # Checkpoint após cada chunk carregado no DuckDB
            save_full_checkpoint(api, token, checkpoint_repo)

            logger.info(f"{E['db']} Carregado: {chunk_file.name} (+{inserted:,})")

        except Exception:
            # Fallback: pandas
            logger.warning(
                f"{E['warn']} read_parquet nativo falhou, a usar pandas..."
            )
            try:
                df      = pd.read_parquet(chunk_file)
                records = [tuple(row) for row in df.itertuples(index=False, name=None)]
                inserted = batch_insert_duckdb(conn, records)
                total_inserted += inserted

                loaded_chunks.append(str(chunk_file))
                state["loaded_chunks"] = loaded_chunks
                save_state(state)
                save_processed_chunks(state)
                save_full_checkpoint(api, token, checkpoint_repo)

                logger.info(
                    f"{E['db']} Carregado (fallback): {chunk_file.name} (+{inserted:,})"
                )
            except Exception as ex:
                logger.exception(
                    f"{E['error']} phase4 {chunk_file.name}: "
                    f"{type(ex).__name__}: {ex}"
                )

    logger.info(
        f"{E['ok']} FASE 4 concluída: {total_inserted:,} registos inseridos"
    )
    return total_inserted


def phase5_deduplicate_duckdb(conn: duckdb.DuckDBPyConnection) -> int:
    logger.info(f"{E['clean']} FASE 5: Deduplicação global (SELECT DISTINCT)")

    try:
        count_before = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        logger.info(f"{E['stats']} Registos antes de dedup: {count_before:,}")

        conn.execute(
            "CREATE TABLE IF NOT EXISTS emails_dedup AS SELECT DISTINCT * FROM emails_raw;"
        )
        conn.execute("DROP TABLE IF EXISTS emails_raw;")
        conn.execute("ALTER TABLE emails_dedup RENAME TO emails_raw;")
        conn.commit()

        count_after = conn.execute("SELECT COUNT(*) FROM emails_raw;").fetchone()[0]
        duplicates  = count_before - count_after

        logger.info(f"{E['stats']} Registos após dedup: {count_after:,}")
        logger.info(f"{E['clean']} Duplicatas removidas: {duplicates:,}")

        return count_after
    except Exception as e:
        logger.exception(f"{E['error']} phase5: {type(e).__name__}: {e}")
        return 0


def phase6_export_final_files(conn: duckdb.DuckDBPyConnection) -> List[Path]:
    logger.info(
        f"{E['email']} FASE 6: A gerar datasets finais (30M linhas por ficheiro)"
    )

    final_files = []
    file_num    = 1
    offset      = 0

    while not stop_event.is_set():
        try:
            rows_df = conn.execute(
                f"SELECT * FROM emails_raw LIMIT {ROWS_PER_FINAL_FILE} OFFSET {offset};"
            ).fetchdf()

            if rows_df.shape[0] == 0:
                break

            ts         = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            final_file = EXPORT_DIR / f"Trader_Emails_{file_num:03d}_{ts}.parquet"

            table = pa.Table.from_pandas(rows_df)
            pq.write_table(table, str(final_file), compression="snappy")

            final_files.append(final_file)
            logger.info(
                f"{E['ok']} Gerado: {final_file.name} ({rows_df.shape[0]:,} linhas)"
            )

            file_num += 1
            offset   += ROWS_PER_FINAL_FILE
        except Exception as e:
            logger.exception(f"{E['error']} phase6 file_{file_num}: {type(e).__name__}: {e}")
            break

    logger.info(
        f"{E['ok']} FASE 6 concluída: {len(final_files)} datasets finais gerados"
    )
    return final_files


def phase7_upload_hf(
    api:             HfApi,
    token:           str,
    emails_repo:     str,
    checkpoint_repo: str,
    final_files:     List[Path],
    db_path:         Path,
    state:           Dict,
):
    logger.info(f"{E['upload']} FASE 7: Upload para Hugging Face")

    for final_file in final_files:
        if stop_event.is_set():
            break
        repo_path = f"Trader_Emails/{final_file.name}"
        if _hf_upload_file(api, token, emails_repo, final_file, repo_path):
            try:
                final_file.unlink()
            except Exception:
                pass

    # Upload checkpoint e Bloom Filter finais
    state["last_execution"]      = datetime.now(timezone.utc).isoformat()
    state["final_files_uploaded"] = len(final_files)
    save_state(state)
    save_processed_chunks(state)
    save_torrent_state(state)

    save_full_checkpoint(api, token, checkpoint_repo)

    logger.info(f"{E['ok']} FASE 7 concluída: checkpoint salvo no HF")


# =====================================================================
# ▶️  MAIN
# =====================================================================
def main():
    global bloom_filter, HF_REPO_BLOOM

    logger.info(f"{E['start']} Minerador Production v3 (BLOOM FILTER DISCO/MMAP NATIVO)")
    logger.info(f"{E['info']} SAVE_PATH:    {SAVE_PATH}")
    logger.info(f"{E['info']} CPU cores:    {os.cpu_count()}")
    logger.info(f"{E['info']} Disk usage:   {disk_usage(SAVE_PATH)}")
    logger.info(f"{E['cpu']} ═══ OTIMIZAÇÕES DE MEMÓRIA ═══")
    logger.info(f"{E['cpu']} memory_limit = 8GB")
    logger.info(f"{E['cpu']} CHUNK_SIZE   = {human(CHUNK_SIZE)}")
    logger.info(f"{E['cpu']} MAX_WORKERS  = min(2, {os.cpu_count() or 2})")
    logger.info(f"{E['cpu']} MAX_INFLIGHT = 4")
    logger.info(f"{E['bloom']} ═══ BLOOM FILTER (MMAP NATIVO) ═══")
    logger.info(f"{E['bloom']} Capacidade  = {BLOOM_CAPACITY:,}")
    logger.info(f"{E['bloom']} Taxa erro   = {BLOOM_ERROR_RATE * 100:.1f}%")
    logger.info(f"{E['loop']} Checkpoint  = a cada {CHECKPOINT_INTERVAL_MIN} minutos")
    logger.info(f"{E['cpu']} ═══════════════════════════════")

    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN não definido")
        sys.exit(2)

    # ── Setup HF ─────────────────────────────────────────────────────
    try:
        api, emails_repo, checkpoint_repo = hf_setup_datasets(HF_TOKEN)
    except Exception as e:
        logger.exception(f"{E['error']} HF setup falhou: {e}")
        sys.exit(1)

    logger.info(f"{E['bloom']} HF_REPO_BLOOM = {HF_REPO_BLOOM}")

    # ── Download checkpoint do HF na inicialização ───────────────────
    load_checkpoint_from_hf(api, HF_TOKEN, checkpoint_repo)

    # ── Download Bloom Filter do HF na inicialização ─────────────────
    load_bloom_from_hf(api, HF_TOKEN)

    # ── Inicializar Bloom Filter (abre / cria via mmap) ──────────────
    bloom_filter = init_bloom_filter(api, HF_TOKEN)

    # Log emails já conhecidos
    n_known = len(bloom_filter)
    if n_known > 0:
        logger.info(f"📥 Recuperados {n_known:,} emails já conhecidos")

    # ── Carregar estado e fundir checkpoints ─────────────────────────
    state = load_state()
    state = merge_checkpoint_into_state(state)

    n_chunks = len(state.get("loaded_chunks", []))
    if n_chunks > 0:
        logger.info(f"📥 Recuperados {n_chunks} chunks processados")

    logger.info(f"{E['ok']} Estado carregado: {len(state)} entradas")

    # ── Inicializar DuckDB ────────────────────────────────────────────
    conn = init_duckdb(DB_PATH)

    # ── Inicializar libtorrent ────────────────────────────────────────
    try:
        session = create_libtorrent_session()
    except Exception as e:
        logger.exception(f"{E['error']} libtorrent falhou: {e}")
        sys.exit(1)

    # ── Iniciar checkpoint periódico ─────────────────────────────────
    start_periodic_checkpoint(api, HF_TOKEN, checkpoint_repo)

    try:
        overall_start = time.time()

        # FASE 1
        completed_torrents = phase1_download_torrents(session, MAGNETS)
        if not completed_torrents:
            logger.error(f"{E['error']} Nenhum torrent concluído com sucesso")
            return

        if stop_event.is_set():
            logger.warning(f"{E['warn']} Parado durante FASE 1")
            return

        # FASE 2
        tars = phase2_wait_downloads(
            completed_torrents, state, api, HF_TOKEN, checkpoint_repo
        )

        if tars and not stop_event.is_set():
            # FASE 3
            chunks = phase3_process_tars(
                tars, state, api, HF_TOKEN, checkpoint_repo
            )

            if chunks and not stop_event.is_set():
                # FASE 4
                phase4_load_to_duckdb(
                    chunks, conn, state, api, HF_TOKEN, checkpoint_repo
                )

                if not stop_event.is_set():
                    # FASE 5
                    total_emails = phase5_deduplicate_duckdb(conn)

                    if not stop_event.is_set():
                        # FASE 6
                        final_files = phase6_export_final_files(conn)

                        if not stop_event.is_set():
                            # FASE 7
                            phase7_upload_hf(
                                api, HF_TOKEN, emails_repo, checkpoint_repo,
                                final_files, DB_PATH, state,
                            )

        total_time = time.time() - overall_start
        logger.info(f"{E['stats']} Tempo total: {total_time / 60:.2f} minutos")
        logger.info(f"{E['ok']} Minerador Production concluído com sucesso")

    except KeyboardInterrupt:
        logger.warning(f"{E['signal']} Shutdown gracioso iniciado por interrupção")
    except Exception as e:
        logger.exception(
            f"{E['error']} Erro fatal na orquestração principal: "
            f"{type(e).__name__}: {e}"
        )
        sys.exit(1)
    finally:
        # ── Parar timer periódico ─────────────────────────────────────
        stop_periodic_checkpoint()

        # ── Garantir persistência final antes de encerrar ─────────────
        logger.info(
            f"{E['checkpoint']} A assegurar persistência final antes do encerramento..."
        )
        try:
            # Actualizar ficheiros de estado auxiliares
            state_current = load_state()
            save_processed_chunks(state_current)
            save_torrent_state(state_current)

            # Upload completo: Bloom Filter + checkpoint
            save_full_checkpoint(api, HF_TOKEN, checkpoint_repo)
        except Exception as e:
            logger.error(
                f"{E['error']} Falha no checkpoint final: {type(e).__name__}: {e}"
            )

        # ── Fechar Bloom Filter mmap ──────────────────────────────────
        if bloom_filter is not None:
            try:
                bloom_filter.close()
                logger.info(f"{E['bloom']} Bloom Filter mmap fechado correctamente")
            except Exception as e:
                logger.error(f"{E['error']} Erro ao fechar Bloom Filter: {e}")

        # ── Fechar DuckDB ─────────────────────────────────────────────
        try:
            conn.close()
            logger.info(f"{E['db']} DuckDB fechado correctamente")
        except Exception:
            pass

        logger.info(f"{E['ok']} Encerramento completo")


if __name__ == "__main__":
    main()
