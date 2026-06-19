#!/usr/bin/env python3
"""
minerador.py — Full pipeline: DOWNLOAD -> EXTRACTION -> FILTER -> CHUNKS -> DEDUP (DuckDB) -> FINAL PARTS -> UPLOAD

Key properties:
- Strict phased execution (no phase starts before prior phase fully completes).
- All paths derived from SAVE_PATH environment variable only.
- Uses libtorrent for concurrent downloads with prioritized files.
- Uses ProcessPoolExecutor for parallel extraction; reads in large byte blocks (configurable).
- Filters disposable email domains early.
- Writes Parquet chunk files to SAVE_PATH/chunks/.
- Uses DuckDB for deduplication and exporting final parts with ~30M rows each.
- Checkpointing: SAVE_PATH/checkpoints/checkpoint.json (and optional HF backup).
- Uploads final parts to HF dataset Trader_Emails (create if needed).
- Uses stop_event (threading.Event) for graceful shutdown.
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
import shutil
import signal
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Event
import multiprocessing
import hashlib
import subprocess

# External libs: ensure installed in runner: libtorrent, duckdb, pyarrow, pandas, requests, huggingface_hub, rich
try:
    import libtorrent as lt
except Exception:
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
import requests
from huggingface_hub import HfApi
from rich.console import Console
from rich.table import Table

# ----------------- Configuration (all paths derived from SAVE_PATH) -----------------
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data")).expanduser().resolve()
SAVE_PATH.mkdir(parents=True, exist_ok=True)

CHUNKS_DIR = SAVE_PATH / "chunks"
FINAL_DIR = SAVE_PATH / "final_parts"
CHECKPOINT_DIR = SAVE_PATH / "checkpoints"
TMP_DIR = SAVE_PATH / "tmp"
LOG_PATH = SAVE_PATH / "minerador.log"
DUCKDB_PATH = SAVE_PATH / "emails.duckdb"

for d in (CHUNKS_DIR, FINAL_DIR, CHECKPOINT_DIR, TMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Hugging Face
HF_TOKEN = os.environ.get("HF_TOKEN")  # required for upload
HF_DATASET = os.environ.get("HF_DATASET", "Trader_Emails")  # must be created/use this dataset

# Concurrency / sizes
MAX_SIMULTANEOUS_TORRENTS = int(os.environ.get("MAX_SIMULTANEOUS_TORRENTS", "5"))
CPU_COUNT = int(os.environ.get("CPU_COUNT", str(multiprocessing.cpu_count() or 1)))
WORKERS = int(os.environ.get("WORKERS", str(max(1, CPU_COUNT))))
# chunk-size bytes: auto-chosen between 512MB and 2GB depending on available memory
CHUNK_SIZE_BYTES = None  # set at runtime

# final part rows
PART_ROWS = int(os.environ.get("PART_ROWS", "30000000"))  # ~30 million per part

# email regex (bytes)
EMAIL_RE = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# public lists of disposable domains (examples; add more if desired)
DISPOSABLE_LIST_URLS = [
    "https://raw.githubusercontent.com/disposable/disposable-email-domains/master/domains.json",
    "https://raw.githubusercontent.com/ivolo/disposable-email-domains/master/index.json",
    "https://raw.githubusercontent.com/7c/fakefilter/master/data/disposable_email_blacklist.conf",
    "https://raw.githubusercontent.com/arkadiyt/disposable-email-domains/master/domains.json",
]

# libtorrent tuned settings (best effort; actual available settings depend on libtorrent build)
LIBTORRENT_SETTINGS = {
    "request_queue_size": 2048,
    "cache_size": 512 * 1024 * 1024,  # 512MB
    "allow_multiple_connections": True,
    "enable_dht": True,
    "enable_lsd": True,
    "enable_pex": True,
}

# MAGNETS must be supplied by user (exact metadata file paths or basenames)
# Example format:
# MAGNETS = [
#   {
#     "name": "Collection #2-#5",
#     "magnet": "magnet:?xt=urn:btih:...",
#     "targets": ["Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
#                 "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz"]
#   },
# ]
MAGNETS: List[Dict[str,Any]] = []  # modify with your actual magnets/targets before running

# logging
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(str(LOG_PATH)), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("minerador")

# emoji map
E = {
    "start": "🚀",
    "download": "📥",
    "extract": "📦",
    "filter": "🧹",
    "chunks": "🧩",
    "dedup": "🦆",
    "upload": "📤",
    "space": "📉",
    "ok": "✅",
    "warn": "⚠️",
    "error": "❌",
    "info": "🗿",
}

# stop event
stop_event = Event()

# ----------------- Utilities -----------------
def human(n:int)->str:
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_stats(path:Path=SAVE_PATH)->Dict[str,int]:
    du = shutil.disk_usage(str(path))
    return {"total":du.total, "used":du.used, "free":du.free}

def choose_chunk_size_bytes()->int:
    try:
        import psutil
        avail = psutil.virtual_memory().available
    except Exception:
        avail = None
    min_cs = 512 * 1024 * 1024
    max_cs = 2 * 1024 * 1024 * 1024
    if avail:
        candidate = int(avail / 6)  # keep memory for parallel workers
        candidate = max(min_cs, min(candidate, max_cs))
        return candidate
    return min_cs

def save_checkpoint(cp:Dict):
    path = CHECKPOINT_DIR / "checkpoint.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cp, indent=2), encoding="utf8")

def load_checkpoint()->Dict:
    path = CHECKPOINT_DIR / "checkpoint.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf8"))
    except Exception:
        return {}

def hf_create_dataset_if_missing(api:HfApi, token:str, dataset_name:str)->str:
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{dataset_name}"
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info(f"{E['ok']} Created HF dataset {repo_id}")
    except Exception as e:
        logger.info(f"{E['info']} HF create_repo returned: {e} (may already exist)")
    return repo_id

# Disposable domains loading (combine many sources + optional local file)
def load_disposable_domains(local_file:Path=None, extra_urls:List[str]=None)->set:
    domains = set()
    if local_file and local_file.exists():
        try:
            for ln in local_file.read_text(encoding="utf8", errors="ignore").splitlines():
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                domains.add(ln.lower())
        except Exception:
            logger.debug("Could not load local disposable list")
    urls = list(DISPOSABLE_LIST_URLS)
    if extra_urls:
        urls.extend(extra_urls)
    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                text = r.text
                try:
                    arr = json.loads(text)
                    if isinstance(arr, dict):
                        domains.update(k.lower() for k in arr.keys())
                    elif isinstance(arr, list):
                        domains.update(x.lower() for x in arr if isinstance(x,str))
                except Exception:
                    for line in text.splitlines():
                        line=line.strip()
                        if not line or line.startswith("#"):
                            continue
                        # take token that looks like domain
                        token = line.split()[0].strip().strip('"').strip("'")
                        if "." in token:
                            domains.add(token.lower())
            else:
                logger.debug(f"Disposable list {url} returned {r.status_code}")
        except Exception:
            logger.debug(f"Could not fetch disposable list {url}")
    logger.info(f"{E['info']} Loaded {len(domains):,} disposable domains")
    return domains

# ----------------- Phase 1: DOWNLOAD (concurrent magnets, wait for all targets complete) -----------------
def setup_libtorrent_session() -> lt.session:
    if lt is None:
        raise RuntimeError("libtorrent (python binding) is required")
    session = lt.session({'listen_interfaces':'0.0.0.0:6881'})
    # attempt to apply settings if supported
    try:
        settings = session.settings()
        if "request_queue_size" in LIBTORRENT_SETTINGS:
            settings["request_queue_size"] = LIBTORRENT_SETTINGS["request_queue_size"]
        if "cache_size" in LIBTORRENT_SETTINGS:
            settings["cache_size"] = LIBTORRENT_SETTINGS["cache_size"]
        session.set_settings(settings)
    except Exception:
        pass
    return session

def add_all_magnets(session:lt.session, magnets:List[Dict])->List[Tuple[str, lt.torrent_handle, List[str]]]:
    handles = []
    for m in magnets:
        if stop_event.is_set():
            break
        try:
            params = lt.parse_magnet_uri(m["magnet"])
            params.save_path = str(SAVE_PATH)
            h = session.add_torrent(params)
            handles.append((m["name"], h, m.get("targets", [])))
            logger.info(f"{E['download']} Added magnet {m['name']}")
        except Exception:
            logger.exception(f"{E['warn']} Could not add magnet {m.get('name')}")
    return handles

def find_target_indices_for_handle(info:lt.torrent_info, targets:List[str])->List[int]:
    indices=[]
    for i in range(info.num_files()):
        p = info.files().at(i).path
        for t in targets:
            if p == t or p.lower()==t.lower() or Path(p).name==Path(t).name:
                indices.append(i)
    return sorted(set(indices))

def wait_for_all_targets(handles:List[Tuple[str,lt.torrent_handle,List[str]]], timeout_s:int=0):
    """
    Wait until every declared target file from every handle is complete on disk.
    Completion criteria: local file exists and size >= expected size (from metadata).
    This function blocks until all targets are present or stop_event set.
    """
    # first, ensure each handle has metadata and get its info
    name_info = {}
    logger.info(f"{E['download']} Waiting for metadata for all torrents")
    meta_start=time.time()
    meta_timeout=int(os.environ.get("METADATA_TIMEOUT", "900"))
    pending = handles[:]
    while pending and not stop_event.is_set():
        new_pending=[]
        for name,h,targets in pending:
            st=h.status()
            if st.has_metadata:
                try:
                    info=h.get_torrent_info()
                except Exception:
                    info=h.get_torrent_info()
                name_info[name]=info
                logger.info(f"{E['info']} Metadata ready for {name}")
            else:
                new_pending.append((name,h,targets))
        pending=new_pending
        if pending:
            if time.time()-meta_start>meta_timeout:
                logger.error(f"{E['error']} Timeout waiting metadata")
                break
            time.sleep(3)
    # apply priorities for each handle
    for name,h,targets in handles:
        info=name_info.get(name)
        if not info:
            logger.warning(f"{E['warn']} No metadata for {name}; skipping prioritization")
            continue
        indices=find_target_indices_for_handle(info, targets)
        # set priorities: targets priority 7, others 0
        try:
            for i in range(info.num_files()):
                pr=7 if i in indices else 0
                h.file_priority(i, pr)
            logger.info(f"{E['download']} Set priorities for {name}: {len(indices)} target indices")
        except Exception:
            logger.debug("Could not set file priorities for handle")
    # Build global list of pending target file records
    pending_targets=[]
    for name,h,targets in handles:
        info=name_info.get(name)
        if not info:
            continue
        indices=find_target_indices_for_handle(info, targets)
        for idx in indices:
            # expected local path
            local_path=local_path_for_index(SAVE_PATH, info, idx)
            expected_size=info.files().at(idx).size
            pending_targets.append({"torrent":name,"handle":h,"info":info,"index":idx,"path":local_path,"expected":expected_size})
    if not pending_targets:
        logger.warning(f"{E['warn']} No target files discovered across all torrents")
        return []
    logger.info(f"{E['download']} Waiting for {len(pending_targets)} target files to finish downloading")
    # Now wait until each pending target meets local_size >= expected
    # Poll loop
    start=time.time()
    while pending_targets and not stop_event.is_set():
        remaining=[]
        for rec in pending_targets:
            idx=rec["index"]
            h=rec["handle"]
            info=rec["info"]
            expected=rec["expected"]
            local=rec["path"]
            try:
                # check file_progress
                try:
                    prog=h.file_progress()
                    got=prog[idx] if idx < len(prog) else 0
                except Exception:
                    # fallback to checking local file size
                    got = local.stat().st_size if local.exists() else 0
                # Also check disk file existence and size
                local_exists=local.exists()
                local_size=local.stat().st_size if local_exists else 0
                # Decide completion only when local_size >= expected AND got >= expected (if got available)
                complete = (local_exists and local_size >= expected) and (got >= expected if got is not None else True)
                if complete:
                    logger.info(f"{E['ok']} Completed: {rec['torrent']} idx {idx} -> {local} ({local_size:,}/{expected:,})")
                else:
                    # log progress occasionally
                    logger.info(f"{E['download']} Pending: {rec['torrent']} idx {idx} progress: local {local_size:,}/{expected:,} torrent_prog {got:,}")
                    remaining.append(rec)
            except Exception:
                logger.exception(f"{E['warn']} Error checking target {rec}")
                remaining.append(rec)
        pending_targets=remaining
        if pending_targets:
            time.sleep(8)
    if stop_event.is_set():
        logger.warning(f"{E['warn']} Stop requested while waiting for downloads")
    else:
        logger.info(f"{E['ok']} All declared targets appear complete on disk")
    return name_info

# helper to compute local file path for a torrent file index
def local_path_for_index(save_path:Path, info:lt.torrent_info, index:int)->Path:
    torrent_name = info.name()
    file_path = info.files().at(index).path
    return save_path / torrent_name / file_path

# ----------------- Phase 2: EXTRACTION (parallel, big-block reads) -----------------
def extract_members_to_chunk_parquets(tar_path:str, chunk_size:int, disposable_domains:set, tmp_prefix:str)->List[str]:
    """
    Run inside worker process.
    - For each member (.txt/.csv) inside tar, read in blocks of chunk_size bytes.
    - Extract emails via bytes regex, filter disposable domains, deduplicate within chunk.
    - Write parquet chunk files per chunk found: tmp_prefix_index.parquet
    Returns list of chunk file paths created.
    """
    import pyarrow as pa, pyarrow.parquet as pq, pandas as pd, re
    EMAIL_RE_LOCAL = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)
    chunk_paths=[]
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            for member in tf:
                if not member.isfile():
                    continue
                name = member.name
                if not (name.endswith(".txt") or name.endswith(".csv")):
                    continue
                f = tf.extractfile(member)
                if f is None:
                    continue
                buffer=b""
                idx=0
                overlap = 200
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
                    found=set()
                    for m in EMAIL_RE_LOCAL.findall(to_process):
                        try:
                            s = m.decode("utf8", "ignore").strip().lower()
                        except Exception:
                            s = m.decode("latin1","ignore").strip().lower()
                        if not s: continue
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
                        chunk_paths.append(str(out))
                        idx += 1
                    if not data:
                        break
    except Exception:
        # ensure partial results returned for checkpoint
        return chunk_paths
    return chunk_paths

def schedule_extraction(tar_files:List[str], chunk_size:int, disposable_domains:set, workers:int)->List[str]:
    """
    Submit extraction tasks to ProcessPoolExecutor: each tar file becomes a task that may produce multiple chunk files.
    Return aggregated list of chunk file paths.
    """
    chunk_files=[]
    tasks=[]
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures={}
        for i,tar in enumerate(tar_files):
            tmp_prefix = str(CHUNKS_DIR / f"chunk_{i:06d}")
            params = {"tar_path":tar,"chunk_size":chunk_size,"disposable_domains":list(disposable_domains),"tmp_prefix":tmp_prefix}
            fut = ex.submit(extract_members_to_chunk_parquets, tar, chunk_size, disposable_domains, tmp_prefix)
            futures[fut]=tar
        for fut in as_completed(futures):
            tar=futures[fut]
            try:
                res=fut.result()
                logger.info(f"{E['chunks']} Extraction of {tar} produced {len(res)} chunks")
                chunk_files.extend(res)
                # checkpoint update: record tar -> chunks
                cp = load_checkpoint()
                cp.setdefault("extraction",{})
                cp["extraction"][tar] = {"chunks":res,"time":datetime.now(timezone.utc).isoformat()}
                save_checkpoint(cp)
            except Exception:
                logger.exception(f"{E['warn']} Worker failed for tar {tar}")
    return chunk_files

# ----------------- Phase 5: DEDUP (DuckDB) -----------------
def deduplicate_with_duckdb(chunk_files:List[str], duckdb_path:str, part_rows:int)->List[Path]:
    """
    Ingest all chunk_files (parquet) into DuckDB, perform SELECT DISTINCT email,
    then export to part_0001.parquet ... where each part has ~part_rows rows.
    Returns list of exported final part paths.
    """
    if not chunk_files:
        return []
    try:
        import duckdb
    except Exception:
        raise RuntimeError("duckdb not installed in runner")

    conn = duckdb.connect(duckdb_path)
    # read all parquet files into one table
    # duckdb can read multiple parquet files via glob; but to be safe, we will create table per file and UNION
    # faster: use read_parquet([...]) if available
    files_list = ",".join(f"'{p}'" for p in chunk_files)
    try:
        # create raw_emails table from parquet list
        conn.execute(f"CREATE OR REPLACE TABLE raw_emails AS SELECT email FROM read_parquet([{files_list}]);")
    except Exception:
        # fallback: iterate
        conn.execute("CREATE OR REPLACE TABLE raw_emails(email VARCHAR);")
        cur = conn.cursor()
        for p in chunk_files:
            try:
                conn.execute(f"INSERT INTO raw_emails SELECT email FROM read_parquet('{p}');")
            except Exception:
                logger.exception(f"Could not ingest chunk {p}")
    # clean invalid emails in SQL: remove empty, malformed entries
    conn.execute("DELETE FROM raw_emails WHERE email IS NULL OR length(email)=0;")
    # lowercase
    conn.execute("CREATE OR REPLACE TABLE raw_lower AS SELECT lower(email) as email FROM raw_emails;")
    conn.execute("DROP TABLE raw_emails;")
    # dedupe
    conn.execute("CREATE OR REPLACE TABLE deduped AS SELECT DISTINCT email FROM raw_lower;")
    conn.execute("DROP TABLE raw_lower;")
    total = conn.execute("SELECT count(*) FROM deduped;").fetchone()[0]
    logger.info(f"{E['dedup']} DuckDB deduped total unique emails: {total:,}")
    if total == 0:
        conn.close()
        return []
    parts=[]
    parts_needed = math.ceil(total / part_rows)
    # use window function to export row ranges
    conn.execute("CREATE OR REPLACE TABLE numbered AS SELECT email, row_number() OVER () AS rn FROM deduped;")
    for i in range(parts_needed):
        start = i*part_rows + 1
        end = min((i+1)*part_rows, total)
        out_path = FINAL_DIR / f"part_{i+1:04d}_{end-start+1}_rows.parquet"
        sql = f"COPY (SELECT email FROM numbered WHERE rn BETWEEN {start} AND {end}) TO '{out_path}' (FORMAT PARQUET)"
        conn.execute(sql)
        parts.append(out_path)
        logger.info(f"{E['chunks']} Exported {out_path} rows {start}-{end}")
    conn.close()
    return parts

# ----------------- Phase: upload final parts and checkpoint to HF -----------------
def upload_parts_to_hf(parts:List[Path], hf_token:str, hf_dataset:str):
    if not hf_token:
        logger.warning("HF_TOKEN not provided; skipping upload")
        return
    api = HfApi()
    who = api.whoami(token=hf_token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{hf_dataset}"
    # create dataset if missing
    try:
        api.create_repo(repo_id=repo_id, token=hf_token, repo_type="dataset", private=True)
    except Exception:
        pass
    uploaded=[]
    for p in parts:
        try:
            api.upload_file(path_or_fileobj=str(p),
                            path_in_repo=f"parts/{p.name}",
                            repo_id=repo_id,
                            repo_type="dataset",
                            token=hf_token)
            uploaded.append(str(p))
            logger.info(f"{E['upload']} Uploaded {p.name} to HF dataset {repo_id}")
        except Exception:
            logger.exception(f"{E['warn']} Failed to upload {p.name} to HF")
    # upload checkpoint as well
    cp_path = CHECKPOINT_DIR / "checkpoint.json"
    if cp_path.exists():
        try:
            api.upload_file(path_or_fileobj=str(cp_path),
                            path_in_repo=f"checkpoints/{cp_path.name}",
                            repo_id=repo_id,
                            repo_type="dataset",
                            token=hf_token)
            logger.info("Uploaded checkpoint to HF")
        except Exception:
            logger.exception("Failed to upload checkpoint to HF")
    return uploaded

# ----------------- Main orchestration -----------------
def main():
    logger.info(f"{E['start']} Minerador started; SAVE_PATH={SAVE_PATH}")
    logger.info(f"{E['info']} Disk: {disk_stats(SAVE_PATH)}")
    global CHUNK_SIZE_BYTES
    CHUNK_SIZE_BYTES = choose_chunk_size_bytes()
    logger.info(f"{E['info']} CHUNK_SIZE_BYTES set to {CHUNK_SIZE_BYTES} bytes")
    # Load disposable domains
    disposable_domains = load_disposable_domains(local_file=Path(SAVE_PATH)/"disposable_domains_local.txt")
    # Ensure libtorrent & duckdb installed
    if lt is None:
        logger.error("libtorrent not installed; abort")
        return
    if duckdb is None:
        logger.error("duckdb not installed; abort")
        return

    # Create libtorrent session and add all magnets
    session = setup_libtorrent_session()
    handles = add_all_magnets(session, MAGNETS)

    # Phase 1: wait for all targets to finish downloading
    name_info = wait_for_all_targets(handles)

    # After this returns, ensure we have list of local tar files to process
    tar_files=[]
    for (name,h,targets) in handles:
        info = name_info.get(name)
        if not info:
            continue
        indices = find_target_indices_for_handle(info, targets)
        for idx in indices:
            local = local_path_for_index(SAVE_PATH, info, idx)
            if not local.exists():
                alt = SAVE_PATH / info.files().at(idx).path
                if alt.exists():
                    local = alt
            if local.exists():
                tar_files.append(str(local))
            else:
                logger.warning(f"{E['warn']} Target file missing after download phase: {local}")
    if not tar_files:
        logger.error("No tar files found to process; exiting")
        return

    # Phase 2: extraction -> produce chunk parquet files (parallel)
    logger.info(f"{E['extract']} Starting extraction of {len(tar_files)} tar files using {WORKERS} workers")
    chunk_files = schedule_extraction(tar_files, CHUNK_SIZE_BYTES, disposable_domains, WORKERS)
    logger.info(f"{E['chunks']} Extraction complete; produced {len(chunk_files)} chunk files")

    # Save checkpoint after extraction
    cp = load_checkpoint()
    cp["extraction_done"] = True
    cp["chunk_files"] = chunk_files
    cp["extraction_time"] = datetime.now(timezone.utc).isoformat()
    save_checkpoint(cp)

    # Phase 5: deduplication with DuckDB
    logger.info(f"{E['dedup']} Starting deduplication with DuckDB (ingesting {len(chunk_files)} chunks)")
    parts = deduplicate_with_duckdb(chunk_files, str(DUCKDB_PATH), PART_ROWS)
    logger.info(f"{E['ok']} Deduplication complete -> {len(parts)} final parts")

    # Save dedup checkpoint
    cp = load_checkpoint()
    cp["dedup_done"] = True
    cp["final_parts"] = [str(p) for p in parts]
    cp["dedup_time"] = datetime.now(timezone.utc).isoformat()
    save_checkpoint(cp)

    # Phase 6: upload final parts to HF dataset Trader_Emails
    if HF_TOKEN:
        logger.info(f"{E['upload']} Uploading final parts to HF dataset {HF_DATASET}")
        uploaded = upload_parts_to_hf([Path(p) for p in parts], HF_TOKEN, HF_DATASET)
        cp["uploaded_parts"] = uploaded
        save_checkpoint(cp)
    else:
        logger.warning(f"{E['warn']} HF_TOKEN not configured; skipping upload")

    logger.info(f"{E['ok']} Pipeline finished; parts produced: {len(parts)}")
    logger.info(f"{E['space']} Final disk: {disk_stats(SAVE_PATH)}")

if __name__ == "__main__":
    # Setup signal handling
    signal.signal(signal.SIGINT, lambda s,f: stop_event.set())
    signal.signal(signal.SIGTERM, lambda s,f: stop_event.set())
    main()
