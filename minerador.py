#!/usr/bin/env python3
"""
minerador.py — phased pipeline: DOWNLOAD -> EXTRACTION -> FILTER -> CHUNKS -> DEDUP(DuckDB) -> PARTS -> UPLOAD

Requirements (install on runner):
- python-libtorrent (python binding)
- duckdb
- pyarrow
- pandas
- requests
- huggingface_hub
- rich

Behavior summary:
- Reads magnets.json (required). If empty -> abort with error.
- Phase 1: Add all magnets, wait metadata, set file priorities, wait until every declared target file across all torrents is fully present on disk (local_size >= expected_size and torrent file_progress >= expected_size). No extraction starts before this is satisfied.
- Phase 2: Extraction: create a list of completed .tar.gz files, process them in parallel (ProcessPoolExecutor) using large block reads (configurable 512MB-2GB), extract emails via bytes regex, filter disposable domains early, write intermediate Parquet chunk files in SAVE_PATH/chunks.
- Phase 3: (Filtering is integrated in extraction)
- Phase 4: Chunks are created per worker; each chunk is flushed to PARQUET and immediately persisted to disk (no large RAM accumulation).
- Phase 5: Deduplicate using DuckDB (SELECT DISTINCT) and export final parts (approx PART_ROWS rows each).
- Phase 6: Upload parts, checkpoint and stats JSONs to HF dataset Trader_Emails (create if needed).
- Checkpointing: SAVE_PATH/checkpoints/checkpoint.json is updated after each phase. On startup, script reads checkpoint and resumes from the last completed phase.
- All paths are derived from SAVE_PATH environment variable only. No hardcoded /mnt or ./data.
"""
from __future__ import annotations

import os
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

# external libs
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

# -----------------------------
# CONFIGURATION (edit via env)
# -----------------------------
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

MAGNETS_JSON = Path("magnets.json")  # must exist in repo; script will read from here
HF_TOKEN = os.environ.get("HF_TOKEN")
HF_DATASET = os.environ.get("HF_DATASET", "Trader_Emails")
WORKERS = int(os.environ.get("WORKERS", str(max(1, multiprocessing.cpu_count()))))
PART_ROWS = int(os.environ.get("PART_ROWS", "30000000"))  # ~30M per final part
CHUNK_READ_MIN = int(os.environ.get("CHUNK_READ_MIN", str(512 * 1024 * 1024)))  # 512MB
CHUNK_READ_MAX = int(os.environ.get("CHUNK_READ_MAX", str(2 * 1024 * 1024 * 1024)))  # 2GB

# disposable domain sources (add more if needed)
DISPOSABLE_LIST_URLS = [
    "https://raw.githubusercontent.com/disposable/disposable-email-domains/master/domains.json",
    "https://raw.githubusercontent.com/ivolo/disposable-email-domains/master/index.json",
    "https://raw.githubusercontent.com/7c/fakefilter/master/data/disposable_email_blacklist.conf",
    "https://raw.githubusercontent.com/arkadiyt/disposable-email-domains/master/domains.json",
]

# libtorrent settings (best-effort)
LIBTORRENT_SETTINGS = {
    "request_queue_size": 2048,
    "cache_size": 512 * 1024 * 1024,
    "enable_dht": True,
    "enable_pex": True,
    "enable_lsd": True,
}

# email regex (bytes)
import re
EMAIL_RE = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# logging
console = Console()
logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)], format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("minerador")

# stop event
stop_event = Event()

# -----------------------------
# Utility helpers
# -----------------------------
def human(n:int)->str:
    for u in ("B","KB","MB","GB","TB"):
        if n < 1024: return f"{n:.2f}{u}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_stats(path:Path=SAVE_PATH)->Dict[str,int]:
    du = shutil.disk_usage(str(path))
    return {"total":du.total,"used":du.used,"free":du.free}

def save_json(path:Path, obj:Any)->None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf8")

def load_json(path:Path)->Any:
    if not path.exists(): return {}
    try:
        return json.loads(path.read_text(encoding="utf8"))
    except Exception:
        return {}

def choose_chunk_size() -> int:
    """Choose block read size between CHUNK_READ_MIN and CHUNK_READ_MAX based on available memory."""
    try:
        import psutil
        avail = psutil.virtual_memory().available
    except Exception:
        avail = None
    if avail:
        # use up to 1/6 of available memory per worker
        candidate = int(avail / (max(1, WORKERS) * 6))
        candidate = max(CHUNK_READ_MIN, min(candidate, CHUNK_READ_MAX))
        return candidate
    return CHUNK_READ_MIN

# -----------------------------
# Read magnets.json and validate
# -----------------------------
def load_magnets() -> List[Dict[str,Any]]:
    if not MAGNETS_JSON.exists():
        logger.error("❌ magnets.json not found. Create magnets.json with your magnet links and targets.")
        sys.exit(2)
    try:
        arr = json.loads(MAGNETS_JSON.read_text(encoding="utf8"))
        if not isinstance(arr, list) or not arr:
            logger.error("❌ magnets.json is empty or not a list. Aborting.")
            sys.exit(2)
        # quick validation: each entry must have name, magnet, targets(list)
        valid=[]
        for e in arr:
            if not isinstance(e, dict): continue
            if not e.get("name") or not e.get("magnet") or not isinstance(e.get("targets"), list):
                logger.warning(f"Skipping invalid magnet entry: {e}")
                continue
            valid.append(e)
        if not valid:
            logger.error("❌ No valid magnets found in magnets.json. Aborting.")
            sys.exit(2)
        return valid
    except Exception as ex:
        logger.exception("❌ Could not parse magnets.json")
        sys.exit(2)

# -----------------------------
# Disposable domains
# -----------------------------
def load_disposable_domains(local_file:Path=None) -> set:
    domains=set()
    # local
    if local_file and local_file.exists():
        for ln in local_file.read_text(encoding="utf8", errors="ignore").splitlines():
            ln=ln.strip()
            if not ln or ln.startswith("#"): continue
            domains.add(ln.lower())
    # remote lists (best effort)
    for url in DISPOSABLE_LIST_URLS:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code==200:
                try:
                    j = json.loads(r.text)
                    if isinstance(j, dict):
                        domains.update(k.lower() for k in j.keys())
                    elif isinstance(j, list):
                        domains.update(d.lower() for d in j if isinstance(d,str))
                except Exception:
                    for ln in r.text.splitlines():
                        ln=ln.strip()
                        if ln and not ln.startswith("#"): 
                            token = ln.split()[0].strip().strip('"').strip("'")
                            if "." in token: domains.add(token.lower())
        except Exception:
            logger.debug(f"Could not fetch disposable list {url}")
    logger.info(f"🗿 Loaded {len(domains):,} disposable domains")
    return domains

# -----------------------------
# LibTorrent helpers and download phase
# -----------------------------
def setup_session() -> "lt.session":
    if lt is None:
        logger.error("libtorrent is not installed in the runner. Install python-libtorrent.")
        sys.exit(1)
    ses = lt.session({'listen_interfaces':'0.0.0.0:6881'})
    # apply some settings if possible
    try:
        s = ses.settings()
        if "request_queue_size" in LIBTORRENT_SETTINGS:
            s["request_queue_size"] = LIBTORRENT_SETTINGS["request_queue_size"]
        if "cache_size" in LIBTORRENT_SETTINGS:
            s["cache_size"] = LIBTORRENT_SETTINGS["cache_size"]
        ses.set_settings(s)
    except Exception:
        logger.debug("Could not apply advanced libtorrent session settings")
    return ses

def add_all_magnets(session: "lt.session", magnets:List[Dict[str,Any]]) -> List[Tuple[str, "lt.torrent_handle", List[str]]]:
    handles=[]
    for m in magnets:
        if stop_event.is_set(): break
        try:
            params = lt.parse_magnet_uri(m["magnet"])
            params.save_path = str(SAVE_PATH)
            h = session.add_torrent(params)
            handles.append((m["name"], h, m["targets"]))
            logger.info(f"📥 Added magnet {m['name']}")
        except Exception:
            logger.exception(f"⚠️ Failed to add magnet {m.get('name')}")
    return handles

def wait_metadata_and_prioritize(handles: List[Tuple[str,"lt.torrent_handle",List[str]]], metadata_timeout:int=600) -> Dict[str,"lt.torrent_info"]:
    name_info={}
    pending=handles[:]
    start=time.time()
    while pending and not stop_event.is_set():
        new=[]
        for name,h,targets in pending:
            try:
                status=h.status()
                if status.has_metadata:
                    info=h.get_torrent_info()
                    name_info[name]=info
                else:
                    new.append((name,h,targets))
            except Exception:
                new.append((name,h,targets))
        pending=new
        if pending:
            if time.time()-start>metadata_timeout:
                logger.error("⚠️ Timeout while waiting metadata")
                break
            time.sleep(3)
    # prioritize target files
    for name,h,targets in handles:
        info=name_info.get(name)
        if not info:
            logger.warning(f"⚠️ No metadata for {name}; skipping prioritization")
            continue
        indices=[]
        for i in range(info.num_files()):
            p=info.files().at(i).path
            for t in targets:
                if p==t or p.lower()==t.lower() or Path(p).name==Path(t).name:
                    indices.append(i)
        # set priorities
        try:
            for i in range(info.num_files()):
                pr=7 if i in indices else 0
                h.file_priority(i,pr)
            logger.info(f"📥 Prioritized {len(indices)} files for {name}")
        except Exception:
            logger.debug("Could not set file priorities for handle")
    return name_info

def wait_for_all_downloads(handles_with_info: List[Tuple[str,"lt.torrent_handle","lt.torrent_info",List[int]]]):
    """
    Blocking: waits until every target file (for all torrents) is present on disk and local_size >= expected_size.
    Only returns when all target files satisfy the condition.
    """
    # Build list of records
    pending=[]
    for name,h,targets in handles_with_info:
        info = targets["info"] if isinstance(targets,dict) else targets
    # Instead of complicated structure, we receive earlier a mapping name->info, so reconstruct
    all_targets=[]
    # handles_with_info will be list of (name, handle, info, indices)
    for name,h,info,indices in handles_with_info:
        for idx in indices:
            local = local_path_for_index(SAVE_PATH, info, idx)
            expected = info.files().at(idx).size
            all_targets.append({"name":name,"handle":h,"info":info,"index":idx,"path":local,"expected":expected})
    if not all_targets:
        logger.error("❌ No target files discovered across all torrents")
        return []
    logger.info(f"📥 Waiting for {len(all_targets)} target files to finish downloading (local existence + size check)")
    # poll loop
    while all_targets and not stop_event.is_set():
        next_pending=[]
        for rec in all_targets:
            local=rec["path"]
            expected=rec["expected"]
            try:
                # check file progress in torrent if possible
                try:
                    prog = rec["handle"].file_progress()
                    got = prog[rec["index"]] if rec["index"] < len(prog) else 0
                except Exception:
                    got = None
                # local size
                local_exists = local.exists()
                local_size = local.stat().st_size if local_exists else 0
                if local_exists and local_size >= expected and (got is None or got >= expected):
                    logger.info(f"✅ Download complete: {rec['name']} idx {rec['index']} -> {local} ({local_size:,}/{expected:,})")
                    continue
                else:
                    logger.info(f"⏳ Pending: {rec['name']} idx {rec['index']} local {local_size:,}/{expected:,} torrent_prog {got}")
                    next_pending.append(rec)
            except Exception:
                logger.exception("Error checking download progress for target")
                next_pending.append(rec)
        all_targets = next_pending
        if all_targets:
            time.sleep(8)
    if stop_event.is_set():
        logger.warning("Stop requested while waiting downloads")
    return all_targets  # empty list means all completed

def local_path_for_index(save_path:Path, info:"lt.torrent_info", index:int) -> Path:
    # libtorrent saves under save_path / torrent_name / file_path
    torrent_name = info.name()
    file_path = info.files().at(index).path
    return save_path / torrent_name / file_path

# -----------------------------
# Phase 2/3/4: Extraction + filter + chunk creation (workers)
# -----------------------------
def extract_tar_to_chunks_worker(tar_path:str, chunk_size:int, disposable_domains:list, tmp_prefix:str) -> Dict[str,Any]:
    """
    Worker executed in separate process.
    - Reads members in tar_path
    - For each member (.txt/.csv) reads in large blocks (chunk_size) and extracts emails (bytes regex)
    - Filters disposable domains early
    - Writes parquet chunk files to disk named tmp_prefix_{n}.parquet
    Returns dict: {'tar':tar_path, 'chunks':[paths], 'stats':{counts}}
    """
    import re, pyarrow as pa, pyarrow.parquet as pq, pandas as pd
    EMAIL_RE_LOCAL = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)
    disposable_set = set(disposable_domains or [])
    chunks=[]
    stats={"raw":0,"discard_temp":0,"invalid":0,"unique_written":0}
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            for member in tf:
                if not member.isfile(): continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")): continue
                f = tf.extractfile(member)
                if f is None: continue
                buffer=b""
                overlap = 200
                idx = 0
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        to_proc = buffer
                        buffer = b""
                    else:
                        to_proc = buffer + data
                        if len(to_proc) > overlap:
                            buffer = to_proc[-overlap:]
                            to_proc = to_proc[:-overlap]
                        else:
                            buffer = to_proc
                            to_proc = b""
                    if not to_proc and not data:
                        break
                    found=set()
                    for m in EMAIL_RE_LOCAL.findall(to_proc):
                        try:
                            s = m.decode("utf8","ignore").strip().lower()
                        except Exception:
                            s = m.decode("latin1","ignore").strip().lower()
                        if not s:
                            stats["invalid"] += 1
                            continue
                        stats["raw"] += 1
                        if "@" not in s:
                            stats["invalid"] += 1
                            continue
                        domain = s.split("@",1)[1]
                        if domain in disposable_set:
                            stats["discard_temp"] += 1
                            continue
                        found.add(s)
                    if found:
                        df = pd.DataFrame({"email":list(found)})
                        out = Path(tmp_prefix + f"_{idx:06d}.parquet")
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, str(out), compression="snappy")
                        chunks.append(str(out))
                        stats["unique_written"] += len(found)
                        idx += 1
                    if not data:
                        break
    except Exception:
        # return partial results and stats
        return {"tar":tar_path,"chunks":chunks,"stats":stats,"error":True}
    return {"tar":tar_path,"chunks":chunks,"stats":stats,"error":False}

def run_extraction_phase(tar_files:List[str], chunk_size:int, disposable_domains:set, workers:int) -> Tuple[List[str], Dict[str,int]]:
    """
    Submit tar_files to worker pool, aggregate chunk file list and stats.
    Returns (chunk_files, aggregated_stats)
    """
    chunk_files=[]
    agg_stats={"raw":0,"discard_temp":0,"invalid":0,"unique_written":0}
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(extract_tar_to_chunks_worker, tar, chunk_size, list(disposable_domains), str(CHUNKS_DIR / (Path(tar).stem))):tar for tar in tar_files}
        for fut in as_completed(futs):
            tar=futs[fut]
            try:
                res=fut.result()
                if res.get("error"):
                    logger.warning(f"⚠️ Worker reported error for {tar} but returning partial results")
                chunk_files.extend(res.get("chunks",[]))
                s=res.get("stats",{})
                for k in agg_stats:
                    agg_stats[k]+=s.get(k,0)
                # checkpoint update per tar
                cp=load_checkpoint()
                cp.setdefault("extraction",{})
                cp["extraction"][tar]={"chunks":res.get("chunks",[]),"stats":s,"time":datetime.now(timezone.utc).isoformat()}
                save_json(CHECKPOINT_DIR / "checkpoint.json", cp)
            except Exception:
                logger.exception(f"Worker failed for tar {tar}")
    logger.info(f"🧩 Extraction produced {len(chunk_files):,} chunk files")
    return chunk_files, agg_stats

# -----------------------------
# DEDUPLICATION phase (DuckDB)
# -----------------------------
def run_dedup_phase(chunk_files:List[str], duckdb_path:str, part_rows:int) -> Tuple[List[str], Dict[str,int]]:
    """
    Ingest chunk_files into DuckDB, produce deduplicated parts of ~part_rows rows.
    Returns (part_paths_list, stats)
    """
    if not chunk_files:
        return [],{}
    conn = duckdb.connect(duckdb_path)
    # read all parquet chunks into raw_emails table
    # duckdb supports read_parquet([...]) but escape paths
    file_list_sql = ",".join(f"'{p}'" for p in chunk_files)
    try:
        conn.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        pass
    try:
        conn.execute(f"CREATE OR REPLACE TABLE raw_emails AS SELECT email FROM read_parquet([{file_list_sql}]);")
    except Exception:
        # fallback ingest iteratively
        conn.execute("CREATE OR REPLACE TABLE raw_emails(email VARCHAR);")
        for p in chunk_files:
            try:
                conn.execute(f"INSERT INTO raw_emails SELECT email FROM read_parquet('{p}');")
            except Exception:
                logger.exception(f"Could not import chunk {p} into DuckDB")
    # Counts before filtering
    total_raw = conn.execute("SELECT count(*) FROM raw_emails").fetchone()[0]
    # Remove null/invalid quickly
    conn.execute("DELETE FROM raw_emails WHERE email IS NULL OR length(trim(email))=0;")
    # Lowercase and DISTINCT
    conn.execute("CREATE OR REPLACE TABLE deduped AS SELECT DISTINCT lower(trim(email)) AS email FROM raw_emails;")
    # Stats
    total_deduped = conn.execute("SELECT count(*) FROM deduped").fetchone()[0]
    # Export parts
    parts=[]
    if total_deduped > 0:
        conn.execute("CREATE OR REPLACE TABLE numbered AS SELECT email, row_number() OVER () AS rn FROM deduped;")
        parts_needed = math.ceil(total_deduped / part_rows)
        for i in range(parts_needed):
            start = i*part_rows + 1
            end = min((i+1)*part_rows, total_deduped)
            out = FINAL_DIR / f"part_{i+1:04d}_{end-start+1}_rows.parquet"
            conn.execute(f"COPY (SELECT email FROM numbered WHERE rn BETWEEN {start} AND {end}) TO '{out}' (FORMAT PARQUET);")
            parts.append(str(out))
            logger.info(f"🦆 Exported part {i+1}/{parts_needed} -> {out} rows {start}-{end}")
    conn.close()
    stats={"total_raw":total_raw,"total_deduped":total_deduped,"parts":len(parts)}
    return parts, stats

# -----------------------------
# Upload parts + checkpoint + stats to HF
# -----------------------------
def hf_upload_parts_and_metadata(parts:List[str], stats:Dict[str,int], checkpoint:Dict[str,Any], hf_token:str, hf_dataset:str):
    if not hf_token:
        logger.warning("HF_TOKEN not set; skipping upload")
        return False
    api=HfApi()
    who=api.whoami(token=hf_token)
    user=who.get("name") or who.get("user") or who.get("id")
    repo_id=f"{user}/{hf_dataset}"
    try:
        api.create_repo(repo_id=repo_id, token=hf_token, repo_type="dataset", private=True)
    except Exception:
        pass
    # upload parts
    for p in parts:
        try:
            api.upload_file(path_or_fileobj=str(p), path_in_repo=f"parts/{Path(p).name}", repo_id=repo_id, repo_type="dataset", token=hf_token)
            logger.info(f"📤 Uploaded part {Path(p).name} to {repo_id}")
        except Exception:
            logger.exception(f"Failed uploading {p}")
    # upload checkpoint and stats
    cp_path = CHECKPOINT_DIR / "checkpoint.json"
    stats_path = CHECKPOINT_DIR / "stats.json"
    save_json(cp_path, checkpoint)
    save_json(stats_path, stats)
    try:
        api.upload_file(path_or_fileobj=str(cp_path), path_in_repo=f"checkpoints/{cp_path.name}", repo_id=repo_id, repo_type="dataset", token=hf_token)
        api.upload_file(path_or_fileobj=str(stats_path), path_in_repo=f"stats/{stats_path.name}", repo_id=repo_id, repo_type="dataset", token=hf_token)
        logger.info("📤 Uploaded checkpoint and stats to HF")
    except Exception:
        logger.exception("Failed uploading checkpoint/stats to HF")
        return False
    return True

# -----------------------------
# Main: orchestrate strict phases with checkpointing
# -----------------------------
def main():
    logger.info(f"🚀 Minerador started; SAVE_PATH={SAVE_PATH}")
    logger.info(f"🗿 Disk: {disk_stats(SAVE_PATH)}")
    # choose chunk read size
    chunk_size = choose_chunk_size()
    logger.info(f"📈 CHUNK_READ size chosen = {chunk_size:,} bytes")
    # load magnets
    magnets = load_magnets()
    logger.info(f"🗿 Loaded {len(magnets)} magnet entries from magnets.json")
    # load checkpoint
    checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
    # load disposable domains
    disposables = load_disposable_domains(local_file=SAVE_PATH / "disposable_domains_local.txt")
    # Phase 1: DOWNLOAD — only if not already recorded as downloads_completed in checkpoint
    if not checkpoint.get("downloads_completed"):
        session = setup_session()
        handles = add_all_magnets(session, magnets)
        # wait metadata and prioritize
        name_info = wait_metadata_and_prioritize(handles)
        # Build handles_with_indices list
        handles_with_indices=[]
        for name,h,targets in handles:
            info = name_info.get(name)
            if not info:
                logger.warning(f"⚠️ No metadata for {name}; skipping")
                continue
            indices = []
            for i in range(info.num_files()):
                p = info.files().at(i).path
                for t in targets:
                    if p==t or p.lower()==t.lower() or Path(p).name==Path(t).name:
                        indices.append(i)
            if not indices:
                logger.warning(f"⚠️ No targets matched for {name}")
            handles_with_indices.append((name,h,info,indices))
        # If no targets found across all magnets, abort (this addresses the empty MAGNETS problem)
        total_targets = sum(len(x[3]) for x in handles_with_indices)
        if total_targets==0:
            logger.error("❌ No target files discovered across all torrents; aborting.")
            sys.exit(3)
        # Wait until all downloads complete
        # Build a simplified list -> reuse wait_for_all_downloads logic: we pass (name,handle,info,indices)
        pending = []
        for name,h,info,indices in handles_with_indices:
            for idx in indices:
                local = local_path_for_index(SAVE_PATH, info, idx)
                expected = info.files().at(idx).size
                pending.append({"name":name,"handle":h,"info":info,"index":idx,"path":local,"expected":expected})
        # Poll until pending empty
        logger.info(f"📥 Waiting for {len(pending)} target files to finish downloading (this can take a long time)")
        while pending and not stop_event.is_set():
            new_pending=[]
            for rec in pending:
                local = rec["path"]
                expected=rec["expected"]
                try:
                    prog = rec["handle"].file_progress()
                    got = prog[rec["index"]] if rec["index"]<len(prog) else None
                except Exception:
                    got = None
                local_exists = local.exists()
                local_size = local.stat().st_size if local_exists else 0
                complete = local_exists and local_size>=expected and (got is None or got>=expected)
                if complete:
                    logger.info(f"✅ Download complete: {rec['name']} idx {rec['index']} -> {local.name} ({local_size:,}/{expected:,})")
                else:
                    logger.info(f"⏳ Pending: {rec['name']} idx {rec['index']} local {local_size:,}/{expected:,} torrent_prog {got}")
                    new_pending.append(rec)
            pending = new_pending
            if pending:
                time.sleep(10)
        if stop_event.is_set():
            logger.warning("Stop requested during download phase; exiting")
            return
        # mark downloads completed in checkpoint and persist
        checkpoint["downloads_completed"]=True
        checkpoint["download_time"]=datetime.now(timezone.utc).isoformat()
        # record downloaded file paths
        downloaded_files=[]
        for name,h,info,indices in handles_with_indices:
            for idx in indices:
                local = local_path_for_index(SAVE_PATH, info, idx)
                if not local.exists():
                    alt = SAVE_PATH / info.files().at(idx).path
                    if alt.exists(): local=alt
                if local.exists():
                    downloaded_files.append(str(local))
        checkpoint["downloaded_files"]=downloaded_files
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        logger.info(f"✅ Downloads phase complete: {len(downloaded_files)} files present")
    else:
        logger.info("🗿 downloads_completed=true in checkpoint; skipping download phase")
        downloaded_files = checkpoint.get("downloaded_files",[])
        if not downloaded_files:
            logger.error("❌ checkpoint says downloads_completed but no downloaded_files recorded. Aborting.")
            sys.exit(4)

    # Phase 2+3+4: Extraction -> filter (disposable) -> chunk Parquet creation
    if not checkpoint.get("extraction_completed"):
        # Build tar_files queue from downloaded_files
        tar_files = [p for p in (checkpoint.get("downloaded_files") or []) if p and Path(p).exists()]
        if not tar_files:
            logger.error("❌ No .tar.gz files found to process; aborting.")
            sys.exit(5)
        logger.info(f"📦 Starting extraction phase for {len(tar_files)} tar files using {WORKERS} workers")
        # Determine chunk size
        chunk_size = choose_chunk_size()
        logger.info(f"📈 Extraction block size: {chunk_size:,} bytes")
        # Run extraction in parallel
        chunk_files, extraction_stats = run_extraction_phase(tar_files, chunk_size, set(disposables), WORKERS)
        # Save checkpoint with chunk files and stats
        checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
        checkpoint["extraction_completed"] = True
        checkpoint["chunk_files"] = chunk_files
        checkpoint["extraction_stats"] = extraction_stats
        checkpoint["extraction_time"] = datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        logger.info(f"🗿 Extraction complete: {extraction_stats}")
    else:
        logger.info("🗿 extraction_completed=true in checkpoint; skipping extraction")
        chunk_files = checkpoint.get("chunk_files",[])
        if not chunk_files:
            logger.error("❌ checkpoint says extraction_completed but no chunk_files recorded. Aborting.")
            sys.exit(6)

    # Phase 5: Deduplication with DuckDB
    if not checkpoint.get("dedup_completed"):
        logger.info(f"🦆 Starting deduplication ingest with DuckDB from {len(chunk_files)} chunks")
        parts, dedup_stats = run_dedup_phase(chunk_files, str(DUCKDB_PATH), PART_ROWS)
        checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
        checkpoint["dedup_completed"] = True
        checkpoint["dedup_stats"] = dedup_stats
        checkpoint["final_parts"] = parts
        checkpoint["dedup_time"] = datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        logger.info(f"🦆 Dedup complete: {dedup_stats}")
    else:
        logger.info("🦆 dedup_completed=true in checkpoint; skipping dedup")
        parts = checkpoint.get("final_parts",[])
        if not parts:
            logger.error("❌ checkpoint says dedup_completed but no final_parts recorded. Aborting.")
            sys.exit(7)

    # Phase 6: Upload parts and stats to HF
    if not checkpoint.get("uploaded_completed"):
        logger.info("📤 Uploading parts and stats to Hugging Face dataset")
        success = hf_upload_parts_and_metadata(parts, {
            "extraction_stats": checkpoint.get("extraction_stats",{}),
            "dedup_stats": checkpoint.get("dedup_stats",{})
        }, checkpoint, HF_TOKEN, HF_DATASET)
        checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
        checkpoint["uploaded_completed"] = bool(success)
        checkpoint["upload_time"] = datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        if success:
            logger.info("📤 Upload finished and checkpoint saved")
        else:
            logger.warning("⚠️ Upload failed or skipped (no HF_TOKEN); checkpoint updated locally")
    else:
        logger.info("📤 uploaded_completed=true in checkpoint; skipping upload")

    logger.info("✅ Pipeline complete. See checkpoints and final parts in SAVE_PATH.")
    logger.info(f"📉 Final disk usage: {disk_stats(SAVE_PATH)}")

# Extra: implementations of helper functions used in main (run_extraction_phase, run_dedup_phase, hf_upload_parts_and_metadata)
# They are defined below (copied from above functions for clarity). See full code body above that contains definitions.
# For brevity of this file snippet, assume they are present. In actual saved file they must be implemented exactly as in the full code.

if __name__ == "__main__":
    # safe signal handling
    signal.signal(signal.SIGINT, lambda s,f: stop_event.set())
    signal.signal(signal.SIGTERM, lambda s,f: stop_event.set())
    main()
