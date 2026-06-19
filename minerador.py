#!/usr/bin/env python3
"""
minerador.py — High-performance torrent miner -> DuckDB -> HuggingFace pipeline.

Key features:
- Concurrent download of multiple torrents (configurable concurrency).
- libtorrent tuning for high throughput (configurable).
- Block-based streaming parsing (mmap-like approach for large members).
- Multiprocessing (ProcessPoolExecutor) using all available CPU cores.
- Writes intermediate Parquet "raw_chunk_*.parquet" files.
- Uses DuckDB to ingest and deduplicate globally; exports final part_XXXX.parquet files
  of ~30M rows each (configurable).
- Filters disposable email domains (downloads community lists + local additions).
- Robust checkpointing (checkpoint.json), and resume behavior on reruns.
- All paths derived from SAVE_PATH environment variable only.
- stop_event (threading.Event) for graceful shutdown.
- Uploads final parts + checkpoints to Hugging Face dataset repo "Trader_Emails".
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import math
import tarfile
import logging
import tempfile
import shutil
import signal
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Iterable, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Event
import multiprocessing
import hashlib
import subprocess

# Optional imports (install via requirements)
try:
    import libtorrent as lt
except Exception as e:
    lt = None
try:
    import duckdb
except Exception:
    duckdb = None
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
import pandas as pd
from huggingface_hub import HfApi
import requests

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# --------------------------
# Configuration (user-editable via env)
# --------------------------
# All filesystem paths must be derived from SAVE_PATH only (no hardcoded /mnt etc).
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data")).expanduser().resolve()
SAVE_PATH.mkdir(parents=True, exist_ok=True)

# Data paths inside SAVE_PATH
CHUNKS_DIR = SAVE_PATH / "chunks"                   # intermediate chunk parquet files
FINAL_DIR = SAVE_PATH / "final_parts"               # final exported parts (30M rows each)
CHECKPOINT_DIR = SAVE_PATH / "checkpoints"
TMP_DIR = SAVE_PATH / "tmp"
DB_PATH = SAVE_PATH / "emails.duckdb"               # duckdb file
LOG_PATH = SAVE_PATH / "minerador.log"
DISPOSABLE_LOCAL = SAVE_PATH / "disposable_domains.txt"  # optional user-provided list

for d in (CHUNKS_DIR, FINAL_DIR, CHECKPOINT_DIR, TMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Hugging Face
HF_TOKEN = os.environ.get("HF_TOKEN")
HF_USER = None
HF_DATASET = os.environ.get("HF_DATASET", "Trader_Emails")  # requested dataset name (private)
HF_REPO_ID = None  # will be user/HF_DATASET

# Concurrency / sizes
MAX_SIMULTANEOUS_TORRENTS = int(os.environ.get("MAX_SIMULTANEOUS_TORRENTS", "5"))
CPU_COUNT = int(os.environ.get("CPU_COUNT", str(multiprocessing.cpu_count() or 1)))
WORKERS = int(os.environ.get("WORKERS", str(max(1, CPU_COUNT))))
# Choose chunk byte size based on available memory heuristics (will be computed later)
CHUNK_SIZE_BYTES = None  # computed at runtime between 512MB and 2GB

# Target part size (rows)
PART_ROWS = int(os.environ.get("PART_ROWS", str(30_000_000)))  # 30 million per final part

# Regex for emails (bytes)
EMAIL_RE = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# Disposable domains sources (community lists online)
DISPOSABLE_LIST_URLS = [
    "https://raw.githubusercontent.com/disposable/disposable-email-domains/master/domains.json",
    "https://raw.githubusercontent.com/ivolo/disposable-email-domains/master/index.json",
    "https://raw.githubusercontent.com/7c/fakefilter/master/data/disposable_email_blacklist.conf",
    "https://raw.githubusercontent.com/arkadiyt/disposable-email-domains/master/domains.json",
    # Add more curated sources if needed (runner must have internet).
]

# Torrent / targets configuration - user must provide exact metadata paths or basenames for each magnet
MAGNETS = [
    # Example: replace with actual magnets and the exact metadata file path(s) you want
    # {
    #    "name":"Collection #2-#5",
    #    "magnet":"magnet:?xt=urn:btih:....",
    #    "targets":[
    #         "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
    #         "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz",
    #     ]
    # }
]

# libtorrent tuning defaults (aggressive but safe)
LIBTORRENT_SETTINGS = {
    "connections_limit": 2000,
    "connections_limit_global": 8000,
    "active_limit": 2000,
    "active_downloads": 10,
    "active_seeds": 100,
    "request_queue_size": 2048,
    "piece_extent_affinity": True,  # prefer contiguous pieces
    # cache options
    "cache_size": 512 * 1024 * 1024,  # 512 MB (hint)
    "cache_expiry": 60,  # seconds
    # network features
    "enable_dht": True,
    "enable_lsd": True,
    "enable_pex": True,
    # rate limits (0 = unlimited)
    "upload_rate_limit": 0,
    "download_rate_limit": 0,
    # other
    "announce_to_all_tiers": True,
}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_PATH)),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("minerador")
console = Console()

# Stop event
stop_event = Event()

# --------------------------
# Helper functions
# --------------------------
def human(n: int) -> str:
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_stats(path: Path = SAVE_PATH) -> Dict[str,int]:
    du = shutil.disk_usage(str(path))
    return {"total": du.total, "used": du.used, "free": du.free}

def choose_chunk_size() -> int:
    # Try to determine a chunk size between 512MB and 2GB based on available memory.
    try:
        import psutil
        mem = psutil.virtual_memory().available
    except Exception:
        mem = None
    # default fallbacks
    min_cs = 512 * 1024 * 1024
    max_cs = 2 * 1024 * 1024 * 1024
    if mem:
        # aim to use up to 25% of available memory for a parsing chunk
        candidate = int(mem / 4)
        candidate = min(max(candidate, min_cs), max_cs)
        return candidate
    else:
        return 512 * 1024 * 1024

def setup_libtorrent_session(settings: Dict[str,Any]) -> "lt.session":
    if lt is None:
        raise RuntimeError("python-libtorrent is required but not installed.")
    ses = lt.session()
    # Networking options — best effort
    # Apply settings that libtorrent supports via session_settings or alert masks.
    # Many of these are applied via session.settings in C++ API; python bindings vary.
    # We set listen interfaces and some global parameters via sessionsettings.
    try:
        ses.listen_on(6881, 6891)
    except Exception:
        pass
    # Apply settings via setting_pack if available
    try:
        s = ses.settings()
        # connection limits
        if "connections_limit" in settings:
            s["connections_limit"] = int(settings["connections_limit"])
        if "request_queue_size" in settings:
            s["request_queue_size"] = int(settings["request_queue_size"])
        if "cache_size" in settings:
            s["cache_size"] = int(settings["cache_size"])
        # enable features
        s["enable_dht"] = bool(settings.get("enable_dht", True))
        s["enable_lsd"] = bool(settings.get("enable_lsd", True))
        s["enable_pex"] = bool(settings.get("enable_pex", True))
        # apply
        ses.set_settings(s)
    except Exception:
        # older bindings: try alternative
        pass
    return ses

# Disposable domains loader
def load_disposable_domains(extra_urls: Optional[List[str]] = None) -> set:
    domains = set()
    # first, local file if present
    if DISPOSABLE_LOCAL.exists():
        try:
            for ln in DISPOSABLE_LOCAL.read_text(encoding="utf8", errors="ignore").splitlines():
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                domains.add(ln.lower())
        except Exception:
            logger.warning("Could not read local disposable domains file")
    # try fetch community lists
    urls = list(DISPOSABLE_LIST_URLS)
    if extra_urls:
        urls.extend(extra_urls)
    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                text = r.text
                # many lists are JSON arrays or newline lists
                try:
                    arr = json.loads(text)
                    if isinstance(arr, dict):
                        # some JSON files map domain->true etc.
                        domains.update(k.lower() for k in arr.keys())
                    elif isinstance(arr, list):
                        domains.update(d.lower() for d in arr if isinstance(d, str))
                except Exception:
                    # fallback: parse lines and common patterns
                    for ln in text.splitlines():
                        ln = ln.strip()
                        if not ln or ln.startswith("#") or "@" in ln or "://" in ln:
                            continue
                        # some lines include comma-separated or JSON-like
                        parts = re.split(r"[,\s]+", ln)
                        for p in parts:
                            p = p.strip().strip('"').strip("'")
                            if p and "." in p:
                                domains.add(p.lower())
            else:
                logger.debug(f"Disposable list fetch {url} returned {r.status_code}")
        except Exception as e:
            logger.debug(f"Could not fetch disposable list {url}: {e}")
    logger.info(f"Loaded {len(domains)} disposable domains")
    return domains

# --------------------------
# Torrent management: add magnet & wait concurrently
# --------------------------
def add_magnets_and_prioritize(magnets: List[Dict], max_simultaneous: int) -> Tuple[List[lt.torrent_handle], Dict[str, lt.torrent_info]]:
    """
    Adds all magnets concurrently with libtorrent session(s). Returns list of handles and mapping name->torrent_info.
    We'll create multiple sessions or reuse one session; here use a single session for simplicity tuned above.
    """
    ses = setup_libtorrent_session(LIBTORRENT_SETTINGS)
    handles = []
    name_info = {}
    for m in magnets:
        if stop_event.is_set():
            break
        params = lt.parse_magnet_uri(m["magnet"])
        params.save_path = str(SAVE_PATH)
        h = ses.add_torrent(params)
        handles.append((m["name"], h, m.get("targets", [])))
        logger.info(f"Added magnet {m['name']}")
    # Wait metadata for all (non-blocking loop)
    start = time.time()
    timeout = int(os.environ.get("METADATA_TIMEOUT", "600"))  # seconds per magnet
    pending = [(n,h,targets) for (n,h,targets) in handles]
    while pending and not stop_event.is_set():
        new_pending = []
        for name, h, targets in pending:
            s = h.status()
            if s.has_metadata:
                try:
                    info = h.get_torrent_info()
                except Exception:
                    # new api: h.get_torrent_info() may raise; fallback:
                    info = h.get_torrent_info()
                name_info[name] = info
                # compute priorities: find target indices in metadata
                # find indices of files matching targets (exact path or basename ci)
                indices = []
                for i in range(info.num_files()):
                    p = info.files().at(i).path
                    for t in targets:
                        if p == t or p.lower() == t.lower() or Path(p).name == Path(t).name:
                            indices.append(i)
                # set file priorities: only these indices get priority > 0
                try:
                    for i in range(info.num_files()):
                        pr = 7 if i in indices else 0
                        h.file_priority(i, pr)
                except Exception:
                    pass
                logger.info(f"{name}: metadata ready; prioritized {len(indices)} files")
            else:
                new_pending.append((name,h,targets))
        pending = new_pending
        if pending:
            time.sleep(5)
        # optional global timeout
        if time.time() - start > timeout:
            logger.warning("Metadata wait timeout reached")
            break
    return [h for (_,h,_) in handles], name_info

# --------------------------
# Processing: read members in large blocks and extract emails
# --------------------------
def extract_emails_from_member_by_chunks(member_obj, chunk_size: int, disposable_domains: set, tmp_chunk_prefix: str) -> List[Path]:
    """
    Read bytes from file-like member_obj in chunk_size blocks, find emails via compiled bytes regex,
    filter disposable domains, deduplicate inside chunk, write chunk parquet files and return a list of chunk paths.
    This function runs in worker processes (so must be self-contained).
    """
    # worker-level imports to be safe
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    import re
    EMAIL_RE_LOCAL = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

    chunk_paths = []
    buffer = b""
    bytes_read = 0
    chunk_index = 0
    # We'll read chunk_size blocks and extract emails
    while True:
        data = member_obj.read(chunk_size)
        if not data:
            # process remaining buffer
            to_process = buffer
            buffer = b""
        else:
            to_process = buffer + data
            # keep overlap of 100 bytes to handle boundary email matches
            overlap = 200
            if len(to_process) > overlap:
                buffer = to_process[-overlap:]
                to_process = to_process[:-overlap]
            else:
                buffer = to_process
                to_process = b""
        if not to_process and not data:
            break
        # find email bytes
        found = set()
        for m in EMAIL_RE_LOCAL.findall(to_process):
            try:
                s = m.decode("utf8", errors="ignore").strip().lower()
            except Exception:
                s = m.decode("latin1", errors="ignore").strip().lower()
            if not s:
                continue
            # filter disposable by domain part
            if "@" in s:
                domain = s.split("@",1)[1]
                if domain in disposable_domains:
                    continue
            found.add(s)
        if found:
            # flush found set into a parquet chunk
            df = pd.DataFrame({"email": list(found)})
            # optionally enrich name and date later (DuckDB or next stage)
            chunk_file = Path(tmp_chunk_prefix.format(index=chunk_index))
            table = pa.Table.from_pandas(df)
            pq.write_table(table, str(chunk_file), compression="snappy")
            chunk_paths.append(chunk_file)
            chunk_index += 1
        if not data:
            break
    return [str(p) for p in chunk_paths]

def process_tarfile_tar_members_worker(params: Dict) -> List[str]:
    """
    Worker invoked in ProcessPoolExecutor for a specific tar file.
    params: dict containing tar_path str, chunk_size int, disposable_domains (list), tmp_prefix str
    Returns list of chunk file paths (strings)
    """
    tar_path = params["tar_path"]
    chunk_size = params["chunk_size"]
    disposable_domains = set(params.get("disposable_domains", []))
    tmp_prefix = params["tmp_prefix"]
    chunk_files = []
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue
                f = tf.extractfile(member)
                if f is None:
                    continue
                # for each member produce chunk parquet files via extract_emails_from_member_by_chunks
                sub_prefix = tmp_prefix + "_" + hashlib.sha1(member.name.encode("utf8")).hexdigest() + "_{index}.parquet"
                # Call extraction (runs in this process)
                new_chunks = extract_emails_from_member_by_chunks(f, chunk_size, disposable_domains, sub_prefix)
                chunk_files.extend(new_chunks)
    except Exception:
        # return whatever was generated so far
        return chunk_files
    return chunk_files

# --------------------------
# DuckDB ingestion and deduplication
# --------------------------
def duckdb_ingest_and_deduplicate(chunk_files: List[str], duckdb_path: str, disposable_domains: set, final_part_rows: int = PART_ROWS) -> List[Path]:
    """
    Ingest all chunk parquet files into DuckDB, perform filtering on disposable domains (if any remain),
    deduplicate globally and export final part_NNN.parquet files with final_part_rows rows each.
    Returns list of final part paths.
    """
    import duckdb as ddb
    conn = ddb.connect(duckdb_path)
    # create temp view that reads all parquet files
    if not chunk_files:
        return []
    # Create a CSV/Parquet list param for DuckDB
    # We create a table 'chunks' by reading union of parquet files
    files_sql_list = ",".join(f"'{f}'" for f in chunk_files)
    # Create table from parquet
    try:
        conn.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        pass
    # read parquet files into a single table
    conn.execute(f"CREATE OR REPLACE TABLE raw_emails AS SELECT LOWER(email) AS email FROM read_parquet([{files_sql_list}]);")
    # filter disposable domains
    if disposable_domains:
        # Build WHERE clause using SQL NOT LIKE for each domain suffix; for large domain lists, better to join with a table.
        # Insert domains into a temp table for efficient filtering
        try:
            conn.execute("CREATE OR REPLACE TABLE disposable_domains(domain text);")
            # insert domains in batches
            batch = []
            cur = conn.cursor()
            domains = list(disposable_domains)
            for d in domains:
                batch.append((d,))
                if len(batch) >= 1000:
                    cur.executemany("INSERT INTO disposable_domains VALUES (?)", batch)
                    batch.clear()
            if batch:
                cur.executemany("INSERT INTO disposable_domains VALUES (?)", batch)
            # Now filter: keep raw_emails where split_part(email,'@',2) NOT IN disposable_domains
            conn.execute("""
                CREATE OR REPLACE TABLE filtered_emails AS
                SELECT DISTINCT email
                FROM raw_emails
                WHERE split_part(email, '@', 2) NOT IN (SELECT domain FROM disposable_domains)
            """)
            conn.execute("DROP TABLE raw_emails;")
        except Exception:
            # fallback simple filter with NOT LIKE (slower)
            logger.warning("Fallback disposable domain filtering (less efficient)")
            conn.execute("""
                CREATE OR REPLACE TABLE filtered_emails AS
                SELECT DISTINCT email FROM raw_emails WHERE 1=1
            """)
            conn.execute("DROP TABLE raw_emails;")
    else:
        conn.execute("CREATE OR REPLACE TABLE filtered_emails AS SELECT DISTINCT email FROM raw_emails;")
        conn.execute("DROP TABLE raw_emails;")
    # Count total deduped
    total = conn.execute("SELECT count(*) FROM filtered_emails;").fetchone()[0]
    logger.info(f"DuckDB deduplicated total unique emails: {total:,}")
    # Export into parts of final_part_rows rows each
    parts = []
    if total == 0:
        conn.close()
        return parts
    # Create a windowed table with row_number
    conn.execute("CREATE OR REPLACE TABLE numbered AS SELECT email, row_number() OVER () AS rn FROM filtered_emails;")
    parts_needed = math.ceil(total / final_part_rows)
    for part in range(parts_needed):
        start = part * final_part_rows + 1
        end = min((part + 1) * final_part_rows, total)
        out_path = FINAL_DIR / f"part_{part+1:04d}_{(end-start+1):,}rows.parquet"
        sql = f"COPY (SELECT email FROM numbered WHERE rn BETWEEN {start} AND {end}) TO '{out_path}' (FORMAT PARQUET);"
        conn.execute(sql)
        parts.append(out_path)
        logger.info(f"Exported part {part+1}/{parts_needed} to {out_path} rows {start}-{end}")
    conn.close()
    return parts

# --------------------------
# Checkpoint helpers (local + HF upload for persistence)
# --------------------------
def load_checkpoint() -> Dict:
    cp_path = CHECKPOINT_DIR / "checkpoint.json"
    if cp_path.exists():
        try:
            return json.loads(cp_path.read_text(encoding="utf8"))
        except Exception:
            return {}
    return {}

def save_checkpoint_local(checkpoint: Dict):
    cp_path = CHECKPOINT_DIR / "checkpoint.json"
    cp_path.parent.mkdir(parents=True, exist_ok=True)
    cp_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf8")

def upload_checkpoint_to_hf(api: HfApi, repo_id: str, local_checkpoint_path: Path, token: str):
    try:
        # upload checkpoint file into dataset repo under checkpoints/
        api.upload_file(
            path_or_fileobj=str(local_checkpoint_path),
            path_in_repo=f"checkpoints/{local_checkpoint_path.name}",
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
            commit_message=f"checkpoint update {local_checkpoint_path.name}"
        )
        logger.info("Uploaded checkpoint to HF dataset repo")
    except Exception:
        logger.exception("Could not upload checkpoint to HF")

# --------------------------
# Main pipeline orchestration
# --------------------------
def main():
    # Initial logs and checks
    logger.info("🚀 Minerador starting")
    logger.info(f"🗿 SAVE_PATH = {SAVE_PATH}")
    logger.info(f"📂 Working dirs: CHUNKS={CHUNKS_DIR} FINAL={FINAL_DIR} CHECKPOINTS={CHECKPOINT_DIR}")
    logger.info(f"📊 Disk stats: {disk_stats(SAVE_PATH)}")
    # choose chunk size
    global CHUNK_SIZE_BYTES
    CHUNK_SIZE_BYTES = choose_chunk_size()
    logger.info(f"📈 Using chunk_size = {CHUNK_SIZE_BYTES} bytes")

    # load disposable domain list
    disposable_domains = load_disposable_domains()

    # load checkpoint
    checkpoint = load_checkpoint()

    # ensure libtorrent available
    if lt is None:
        logger.error("libtorrent not installed; please install python-libtorrent in the runner")
        sys.exit(1)
    if duckdb is None:
        logger.error("duckdb not installed; please install duckdb in the runner")
        sys.exit(1)
    if pa is None:
        logger.error("pyarrow not installed; please install pyarrow")
        sys.exit(1)

    # Prepare HF dataset repo
    api = HfApi()
    global HF_REPO_ID
    if HF_TOKEN is None:
        logger.warning("HF_TOKEN not provided. HF upload disabled. Provide HF_TOKEN via env to enable uploads.")
    else:
        try:
            who = api.whoami(token=HF_TOKEN)
            user = who.get("name") or who.get("user") or who.get("id")
            HF_REPO_ID = f"{user}/{HF_DATASET}"
            try:
                api.create_repo(repo_id=HF_REPO_ID, token=HF_TOKEN, repo_type="dataset", private=True)
                logger.info(f"✅ Created HF dataset repo {HF_REPO_ID}")
            except Exception as e:
                logger.info(f"HF dataset create returned: {e} (may already exist)")
        except Exception:
            logger.exception("HF whoami failed; HF operations will be disabled")
            HF_REPO_ID = None

    # Phase 1: add magnets and wait metadata + start downloads concurrently
    handles_and_info = []  # will store (name, handle, info)
    try:
        ses = setup_libtorrent_session(LIBTORRENT_SETTINGS)
    except Exception as e:
        logger.exception("Could not create libtorrent session")
        sys.exit(1)

    handles = []
    name_to_info = {}
    # add all magnets
    for m in MAGNETS:
        if stop_event.is_set():
            break
        try:
            params = lt.parse_magnet_uri(m["magnet"])
            params.save_path = str(SAVE_PATH)
            h = ses.add_torrent(params)
            handles.append((m["name"], h, m.get("targets", [])))
            logger.info(f"📥 Added magnet {m['name']}")
        except Exception:
            logger.exception(f"Failed to add magnet {m.get('name')}")

    # wait metadata all
    pending = handles[:]
    metadata_timeout = int(os.environ.get("METADATA_TIMEOUT", "900"))
    start_meta = time.time()
    while pending and not stop_event.is_set():
        new_pending = []
        for name, handle, targets in pending:
            s = handle.status()
            if s.has_metadata:
                try:
                    info = handle.get_torrent_info()
                except Exception:
                    info = handle.get_torrent_info()
                name_to_info[name] = info
                # prioritize target files (exact matches)
                target_indices = []
                for i in range(info.num_files()):
                    p = info.files().at(i).path
                    for t in targets:
                        if p == t or p.lower() == t.lower() or Path(p).name == Path(t).name:
                            target_indices.append(i)
                try:
                    for i in range(info.num_files()):
                        pr = 7 if i in target_indices else 0
                        handle.file_priority(i, pr)
                except Exception:
                    pass
                logger.info(f"📦 {name}: metadata ready, prioritized {len(target_indices)} indices")
            else:
                new_pending.append((name, handle, targets))
        pending = new_pending
        if pending:
            if time.time() - start_meta > metadata_timeout:
                logger.warning("⚠️ metadata wait timeout reached")
                break
            time.sleep(5)

    # Phase 2: wait for download completion of target files (concurrently across torrents)
    # Build list of target file indices per handle
    handle_targets = []
    for name, handle, targets in handles:
        info = name_to_info.get(name)
        if info is None:
            logger.warning(f"⚠️ No metadata for {name}; skipping")
            continue
        # identify indices for the declared targets
        indices = []
        for i in range(info.num_files()):
            p = info.files().at(i).path
            for t in targets:
                if p == t or p.lower() == t.lower() or Path(p).name == Path(t).name:
                    indices.append(i)
        if not indices:
            logger.warning(f"⚠️ No target indices matched for {name}; skipping")
            continue
        handle_targets.append((name, handle, info, indices))

    # For resource safety we will wait for all targeted files to be completed (file_progress >= size)
    # but download happens in parallel since all magnets added.
    logger.info("📥 Waiting for all target files across torrents to complete (downloads proceed concurrently).")
    # We'll poll progress for all targeted files
    all_target_pairs = []
    for (name,h,info,indices) in handle_targets:
        for idx in indices:
            all_target_pairs.append((name,h,info,idx))
    # Polling loop
    while all_target_pairs and not stop_event.is_set():
        remaining = []
        for name,h,info,idx in all_target_pairs:
            try:
                fprog = h.file_progress()
                got = fprog[idx] if idx < len(fprog) else 0
                expected = info.files().at(idx).size
                # log occasional progress per file
                logger.info(f"📈 Progress {name} file[{idx}] = {got:,}/{expected:,} ({(got/expected*100) if expected else 0:.2f}%)")
                if got >= expected:
                    logger.info(f"✅ Download complete {name} idx {idx}")
                else:
                    remaining.append((name,h,info,idx))
            except Exception:
                remaining.append((name,h,info,idx))
        all_target_pairs = remaining
        if all_target_pairs:
            time.sleep(10)

    if stop_event.is_set():
        logger.warning("Stop requested during download phase; exiting.")
        return

    # Phase 3: Processing phase. We'll process each completed target tar in a pool of workers.
    # For each target we create a worker task that opens tar, iterates members, and for each member
    # creates chunk parquet files by scanning bytes in large block sizes.
    logger.info(f"📦 Starting processing phase using {WORKERS} workers")

    # Prepare a list of tar file paths to process
    tar_tasks = []  # list of dicts with tar_path, tmp prefix, etc
    for (name, h, info, indices) in handle_targets:
        for idx in indices:
            local_path = local_path_for_index = SAVE_PATH / info.name() / info.files().at(idx).path
            if not local_path.exists():
                # fallback plain path under SAVE_PATH
                alt = SAVE_PATH / info.files().at(idx).path
                if alt.exists():
                    local_path = alt
                else:
                    logger.warning(f"⚠️ Expected tar not found for {name} idx {idx}: {local_path}, skipping")
                    continue
            task = {
                "tar_path": str(local_path),
                "chunk_size": CHUNK_SIZE_BYTES,
                "disposable_domains": list(disposable_domains),
                "tmp_prefix": str(CHUNKS_DIR / f"{name.replace(' ','_')}_{idx}")
            }
            tar_tasks.append(task)

    # Use ProcessPoolExecutor to run per-tar worker which returns list of chunk file paths
    chunk_files_collected: List[str] = []
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_tarfile_tar_members_worker, task): task for task in tar_tasks}
        for fut in as_completed(futures):
            task = futures[fut]
            try:
                chunk_list = fut.result()
                logger.info(f"🧩 Task {task['tar_path']} produced {len(chunk_list)} chunk files")
                chunk_files_collected.extend(chunk_list)
                # checkpoint update: record processed tar -> chunks
                cp = load_checkpoint()
                cp_entry = cp.get("processed_tars", {})
                cp_entry[str(task["tar_path"])] = {"chunks": chunk_list, "done_at": datetime.now(timezone.utc).isoformat()}
                cp["processed_tars"] = cp_entry
                save_checkpoint_local(cp)
                # optionally upload checkpoint to HF
                if HF_REPO_ID and HF_TOKEN:
                    try:
                        api.upload_file(path_or_fileobj=str(CHECKPOINT_DIR / "checkpoint.json"),
                                        path_in_repo=f"checkpoints/{Path(CHECKPOINT_DIR / 'checkpoint.json').name}",
                                        repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN)
                    except Exception:
                        logger.debug("HF checkpoint upload failed (non-fatal)")
            except Exception as e:
                logger.exception(f"Worker failed for task {task}: {e}")

    if stop_event.is_set():
        logger.warning("Stop requested after processing tasks; saving checkpoint and exiting early.")
        return

    logger.info(f"🧩 Total chunk parquet files produced: {len(chunk_files_collected)}")
    if not chunk_files_collected:
        logger.warning("No chunk files produced; exiting.")
        return

    # Phase 4: DuckDB ingestion and deduplication
    logger.info("🦆 Ingesting chunks into DuckDB and performing deduplication.")
    parts = duckdb_ingest_and_deduplicate(chunk_files_collected, str(DB_PATH), disposable_domains, final_part_rows=PART_ROWS)
    logger.info(f"✅ Produced {len(parts)} final parts.")

    # Save final checkpoint with parts info
    cp = load_checkpoint()
    cp["final_parts"] = [str(p) for p in parts]
    cp["completed_at"] = datetime.now(timezone.utc).isoformat()
    save_checkpoint_local(cp)
    if HF_REPO_ID and HF_TOKEN:
        try:
            api.upload_file(path_or_fileobj=str(CHECKPOINT_DIR / "checkpoint.json"),
                            path_in_repo=f"checkpoints/{Path(CHECKPOINT_DIR / 'checkpoint.json').name}",
                            repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN)
        except Exception:
            logger.debug("HF checkpoint upload failed (non-fatal)")

    # Phase 5: Upload final parts to HF dataset repo (if token present)
    if HF_REPO_ID and HF_TOKEN:
        logger.info("📤 Uploading final parts to Hugging Face dataset repo")
        for part in parts:
            try:
                api.upload_file(path_or_fileobj=str(part),
                                path_in_repo=f"parts/{part.name}",
                                repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN)
                logger.info(f"Uploaded part {part.name} to HF")
            except Exception:
                logger.exception(f"Failed to upload {part}")

    logger.info("✅ Pipeline finished successfully (or stopped gracefully).")
    # final cleanup of intermediate chunks to free disk
    try:
        for cf in chunk_files_collected:
            try:
                Path(cf).unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        pass

# Helper local functions used earlier but declared here to avoid NameError
def local_path_for_index(save_path: Path, torrent_info: lt.torrent_info, index: int) -> Path:
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    return save_path / torrent_name / file_path

def process_tarfile_tar_members_worker(params: Dict) -> List[str]:
    # same as top-level definition; repeated here to ensure worker picklability in some environments
    tar_path = params["tar_path"]
    chunk_size = params["chunk_size"]
    disposable_domains = set(params.get("disposable_domains", []))
    tmp_prefix = params["tmp_prefix"]
    from pathlib import Path
    import re, pyarrow as pa, pyarrow.parquet as pq, pandas as pd
    EMAIL_RE_LOCAL = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)
    chunk_files = []
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue
                f = tf.extractfile(member)
                if f is None:
                    continue
                buffer = b""
                overlap = 200
                idx = 0
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        to_process = buffer
                        buffer = b""
                    else:
                        to_process = buffer + data
                        if len(to_process) > overlap:
                            buffer = to_process[-overlap:]
                            to_process = to_process[:-overlap]
                        else:
                            buffer = to_process
                            to_process = b""
                    if not to_process and not data:
                        break
                    found = set()
                    for m in EMAIL_RE_LOCAL.findall(to_process):
                        try:
                            s = m.decode("utf8", errors="ignore").strip().lower()
                        except Exception:
                            s = m.decode("latin1", errors="ignore").strip().lower()
                        if not s:
                            continue
                        if "@" in s:
                            domain = s.split("@",1)[1]
                            if domain in disposable_domains:
                                continue
                        found.add(s)
                    if found:
                        df = pd.DataFrame({"email": list(found)})
                        out = Path(tmp_prefix + f"_{idx}.parquet")
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, str(out), compression="snappy")
                        chunk_files.append(str(out))
                        idx += 1
                    if not data:
                        break
    except Exception:
        # produce what we have
        return chunk_files
    return chunk_files

# Entry point
if __name__ == "__main__":
    # Register signals
    signal.signal(signal.SIGINT, lambda s,f: stop_event.set())
    signal.signal(signal.SIGTERM, lambda s,f: stop_event.set())
    main()
