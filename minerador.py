#!/usr/bin/env python3
"""
minerador.py — Phased pipeline: DOWNLOAD -> EXTRACTION -> FILTER -> CHUNKS -> DEDUP(DuckDB) -> PARTS -> UPLOAD

All paths derive from SAVE_PATH environment variable (default ./data).
MAGNETS are embedded below — update them here if needed.
"""

from __future__ import annotations
import os, sys, json, time, math, tarfile, logging, shutil, signal
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Event
import multiprocessing
import re
import hashlib

# External libs - ensure installed in runner via workflow
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

# -------- CONFIG & PATHS (all derived from SAVE_PATH) --------
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data")).expanduser().resolve()
SAVE_PATH.mkdir(parents=True, exist_ok=True)

CHUNKS_DIR   = SAVE_PATH / "chunks"
FINAL_DIR    = SAVE_PATH / "final_parts"
CHECKPOINT_DIR = SAVE_PATH / "checkpoints"
TMP_DIR      = SAVE_PATH / "tmp"
LOG_PATH     = SAVE_PATH / "minerador.log"
DUCKDB_PATH  = SAVE_PATH / "emails.duckdb"

for d in (CHUNKS_DIR, FINAL_DIR, CHECKPOINT_DIR, TMP_DIR):
    d.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_DATASET = os.environ.get("HF_DATASET", "Trader_Emails")

WORKERS = int(os.environ.get("WORKERS", str(max(1, multiprocessing.cpu_count()))))
PART_ROWS = int(os.environ.get("PART_ROWS", "30000000"))  # ~30M per part
CHUNK_READ_MIN = 512 * 1024 * 1024   # 512MB
CHUNK_READ_MAX = 2 * 1024 * 1024 * 1024  # 2GB

# --------------- MAGNETS (embedded) ---------------
MAGNETS = [
  {
    "name": "Collection #2-#5 & Antipublic",
    "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
    "targets": [
      "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
      "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz"
    ]
  },
  {
    "name": "Collection #1",
    "magnet": "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E&dn=Collection%201&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2f%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce",
    "targets": [
      "Collection #1/Collection #1_BTC combos.tar.gz",
      "Collection #1/Collection #1_OLD CLOUD_Trading combos.tar.gz",
      "Collection #1/Collection #1_OLD CLOUD_BTC combos.tar.gz"
    ]
  }
]

# disposable lists to fetch
DISPOSABLE_LIST_URLS = [
    "https://raw.githubusercontent.com/disposable/disposable-email-domains/master/domains.json",
    "https://raw.githubusercontent.com/ivolo/disposable-email-domains/master/index.json",
    "https://raw.githubusercontent.com/7c/fakefilter/master/data/disposable_email_blacklist.conf",
    "https://raw.githubusercontent.com/arkadiyt/disposable-email-domains/master/domains.json",
]

# libtorrent settings
LIBTORRENT_SETTINGS = {
    "request_queue_size": 2048,
    "cache_size": 512 * 1024 * 1024,
    "enable_dht": True,
    "enable_pex": True,
    "enable_lsd": True,
}

# email regex (bytes)
EMAIL_RE = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# logging
console = Console()
logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(str(LOG_PATH)), logging.StreamHandler(sys.stdout)], format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("minerador")

# stop event
stop_event = Event()

# ----------------- helpers -----------------
def human(n:int)->str:
    for u in ("B","KB","MB","GB","TB"):
        if n < 1024: return f"{n:.2f}{u}"
        n /= 1024
    return f"{n:.2f}PB"

def save_json(p:Path, obj:Any):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf8")

def load_json(p:Path)->Any:
    if not p.exists(): return {}
    try:
        return json.loads(p.read_text(encoding="utf8"))
    except Exception:
        return {}

def choose_chunk_size():
    try:
        import psutil
        avail = psutil.virtual_memory().available
    except Exception:
        avail = None
    if avail:
        cand = int(avail / (max(1, WORKERS) * 6))
        return max(CHUNK_READ_MIN, min(cand, CHUNK_READ_MAX))
    return CHUNK_READ_MIN

# ----------------- disposable domains -----------------
def load_disposable_domains() -> set:
    domains = set()
    # local override file
    local = SAVE_PATH / "disposable_domains_local.txt"
    if local.exists():
        for ln in local.read_text(encoding="utf8", errors="ignore").splitlines():
            ln=ln.strip()
            if ln and not ln.startswith("#"): domains.add(ln.lower())
    # fetch lists
    for url in DISPOSABLE_LIST_URLS:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code==200:
                try:
                    j = json.loads(r.text)
                    if isinstance(j, dict): domains.update(k.lower() for k in j.keys())
                    elif isinstance(j, list): domains.update(x.lower() for x in j if isinstance(x,str))
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

# ----------------- libtorrent session & download phase -----------------
def setup_session():
    if lt is None:
        logger.error("libtorrent is not installed in the runner. Install python-libtorrent.")
        sys.exit(1)
    ses = lt.session({'listen_interfaces':'0.0.0.0:6881'})
    try:
        s = ses.settings()
        if "request_queue_size" in LIBTORRENT_SETTINGS:
            s["request_queue_size"] = LIBTORRENT_SETTINGS["request_queue_size"]
        if "cache_size" in LIBTORRENT_SETTINGS:
            s["cache_size"] = LIBTORRENT_SETTINGS["cache_size"]
        ses.set_settings(s)
    except Exception:
        pass
    return ses

def add_magnets(session, magnets):
    handles=[]
    for m in magnets:
        if stop_event.is_set(): break
        try:
            params = lt.parse_magnet_uri(m["magnet"])
            params.save_path = str(SAVE_PATH)
            h = session.add_torrent(params)
            handles.append((m["name"], h, m["targets"]))
            logger.info(f"📥 Added magnet: {m['name']}")
        except Exception:
            logger.exception("Could not add magnet")
    return handles

def wait_for_metadata_and_prioritize(handles, timeout=900):
    name_info={}
    pending=handles[:]
    start=time.time()
    while pending and not stop_event.is_set():
        new=[]
        for name,h,targets in pending:
            st=h.status()
            if st.has_metadata:
                try:
                    info=h.get_torrent_info()
                except Exception:
                    info=h.get_torrent_info()
                name_info[name]=info
            else:
                new.append((name,h,targets))
        pending=new
        if pending:
            if time.time()-start > timeout:
                logger.error("⚠️ metadata wait timeout")
                break
            time.sleep(3)
    # set priorities
    for name,h,targets in handles:
        info=name_info.get(name)
        if not info:
            logger.warning(f"⚠️ No metadata for {name}")
            continue
        indices=[]
        for i in range(info.num_files()):
            p = info.files().at(i).path
            for t in targets:
                if p==t or p.lower()==t.lower() or Path(p).name==Path(t).name:
                    indices.append(i)
        try:
            for i in range(info.num_files()):
                pr=7 if i in indices else 0
                h.file_priority(i, pr)
            logger.info(f"📥 Prioritized {len(indices)} files for {name}")
        except Exception:
            logger.debug("Could not set priorities")
    return name_info

def wait_for_all_targets_to_complete(handles_with_info):
    # handles_with_info: list of (name, handle, info, indices)
    pending=[]
    for name,h,info,indices in handles_with_info:
        for idx in indices:
            local = local_path_for_index(info, idx)
            expected = info.files().at(idx).size
            pending.append({"name":name,"handle":h,"info":info,"index":idx,"path":local,"expected":expected})
    if not pending:
        logger.error("❌ No targets discovered across all torrents")
        return []
    logger.info(f"📥 Waiting for {len(pending)} target files to finish downloading")
    while pending and not stop_event.is_set():
        new=[]
        for rec in pending:
            try:
                prog=rec["handle"].file_progress()
                got = prog[rec["index"]] if rec["index"] < len(prog) else None
            except Exception:
                got = None
            local = rec["path"]
            local_exists = local.exists()
            local_size = local.stat().st_size if local_exists else 0
            expected = rec["expected"]
            if local_exists and local_size >= expected and (got is None or got >= expected):
                logger.info(f"✅ Download complete {rec['name']} idx {rec['index']} -> {local.name} ({local_size:,}/{expected:,})")
            else:
                logger.info(f"⏳ Pending {rec['name']} idx {rec['index']} local {local_size:,}/{expected:,} torrent_prog {got}")
                new.append(rec)
        pending=new
        if pending:
            time.sleep(8)
    if stop_event.is_set():
        logger.warning("Stop requested during download")
    return pending  # empty -> success

def local_path_for_index(info, idx):
    return SAVE_PATH / info.name() / info.files().at(idx).path

# ----------------- Extraction worker -----------------
def extract_worker(tar_path:str, chunk_size:int, disposable_domains:list, tmp_prefix:str):
    """
    Runs in separate process.
    Reads members and writes chunk parquet files. Returns dict with chunk paths and stats.
    """
    import re, pyarrow as pa, pyarrow.parquet as pq, pandas as pd
    EMAIL_RE_LOCAL = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)
    disposable_set = set(disposable_domains or [])
    out_chunks=[]
    stats={"raw":0,"discarded_temp":0,"invalid":0,"written_unique":0}
    try:
        with tarfile.open(tar_path, "r:*") as tf:
            for member in tf:
                if not member.isfile(): continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")): continue
                f = tf.extractfile(member)
                if f is None: continue
                buffer=b""
                overlap=200
                idx=0
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        to_proc = buffer
                        buffer=b""
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
                            stats["invalid"]+=1; continue
                        stats["raw"]+=1
                        if "@" not in s:
                            stats["invalid"]+=1; continue
                        domain = s.split("@",1)[1]
                        if domain in disposable_set:
                            stats["discarded_temp"]+=1; continue
                        found.add(s)
                    if found:
                        df = pd.DataFrame({"email":list(found)})
                        out = Path(tmp_prefix + f"_{idx:06d}.parquet")
                        table = pa.Table.from_pandas(df)
                        pq.write_table(table, str(out), compression="snappy")
                        out_chunks.append(str(out))
                        stats["written_unique"] += len(found)
                        idx += 1
                    if not data:
                        break
    except Exception:
        return {"tar":tar_path,"chunks":out_chunks,"stats":stats,"error":True}
    return {"tar":tar_path,"chunks":out_chunks,"stats":stats,"error":False}

def run_extraction(tar_files:List[str], chunk_size:int, disposable_domains:set, workers:int):
    chunk_files=[]
    agg_stats={"raw":0,"discarded_temp":0,"invalid":0,"written_unique":0}
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures={ex.submit(extract_worker, tar, chunk_size, list(disposable_domains), str(CHUNKS_DIR / Path(tar).stem)): tar for tar in tar_files}
        for fut in as_completed(futures):
            tar = futures[fut]
            try:
                res = fut.result()
                chunk_files.extend(res.get("chunks",[]))
                s = res.get("stats",{})
                agg_stats["raw"] += s.get("raw",0)
                agg_stats["discarded_temp"] += s.get("discarded_temp",0)
                agg_stats["invalid"] += s.get("invalid",0)
                agg_stats["written_unique"] += s.get("written_unique",0)
                # update checkpoint per tar
                cp = load_json(CHECKPOINT_DIR / "checkpoint.json")
                cp.setdefault("extraction_detail",{})
                cp["extraction_detail"][tar] = {"chunks":res.get("chunks",[]),"stats":s,"time":datetime.now(timezone.utc).isoformat()}
                save_json(CHECKPOINT_DIR / "checkpoint.json", cp)
            except Exception:
                logger.exception("Worker failed for tar %s", tar)
    logger.info("🧩 Extraction aggregated stats: %s", agg_stats)
    return chunk_files, agg_stats

# ----------------- Dedup (DuckDB) -----------------
def run_dedup(chunk_files:List[str], duckdb_path:str, part_rows:int):
    if not chunk_files:
        return [], {}
    conn = duckdb.connect(duckdb_path)
    files_sql = ",".join(f"'{p}'" for p in chunk_files)
    try:
        conn.execute(f"CREATE OR REPLACE TABLE raw_emails AS SELECT email FROM read_parquet([{files_sql}]);")
    except Exception:
        conn.execute("CREATE OR REPLACE TABLE raw_emails(email VARCHAR);")
        for p in chunk_files:
            try:
                conn.execute(f"INSERT INTO raw_emails SELECT email FROM read_parquet('{p}');")
            except Exception:
                logger.exception("Could not ingest chunk %s", p)
    total_raw = conn.execute("SELECT count(*) FROM raw_emails").fetchone()[0]
    conn.execute("DELETE FROM raw_emails WHERE email IS NULL OR length(trim(email))=0;")
    conn.execute("CREATE OR REPLACE TABLE deduped AS SELECT DISTINCT lower(trim(email)) AS email FROM raw_emails;")
    total_deduped = conn.execute("SELECT count(*) FROM deduped").fetchone()[0]
    parts=[]
    if total_deduped>0:
        conn.execute("CREATE OR REPLACE TABLE numbered AS SELECT email, row_number() OVER () AS rn FROM deduped;")
        parts_needed = math.ceil(total_deduped / part_rows)
        for i in range(parts_needed):
            start=i*part_rows+1; end=min((i+1)*part_rows, total_deduped)
            out = FINAL_DIR / f"part_{i+1:04d}_{end-start+1}_rows.parquet"
            conn.execute(f"COPY (SELECT email FROM numbered WHERE rn BETWEEN {start} AND {end}) TO '{out}' (FORMAT PARQUET);")
            parts.append(str(out))
            logger.info("🦆 Exported part %s rows %d-%d", out.name, start, end)
    conn.close()
    return parts, {"total_raw":total_raw, "total_deduped": total_deduped, "parts": len(parts)}

# ----------------- HF upload & checkpoint/stats -----------------
def hf_upload(parts:List[str], checkpoint:Dict[str,Any], stats:Dict[str,Any], hf_token:str, hf_dataset:str):
    if not hf_token:
        logger.warning("HF_TOKEN not provided; skipping upload")
        return False
    api = HfApi()
    who = api.whoami(token=hf_token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{hf_dataset}"
    try:
        api.create_repo(repo_id=repo_id, token=hf_token, repo_type="dataset", private=True)
    except Exception:
        pass
    for p in parts:
        try:
            api.upload_file(path_or_fileobj=str(p), path_in_repo=f"parts/{Path(p).name}", repo_id=repo_id, repo_type="dataset", token=hf_token)
            logger.info("📤 Uploaded part %s", Path(p).name)
        except Exception:
            logger.exception("Failed to upload part %s", p)
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
        logger.exception("Failed to upload checkpoint/stats")
        return False
    return True

# ----------------- Main orchestration -----------------
def main():
    logger.info("🚀 Minerador started; SAVE_PATH=%s", SAVE_PATH)
    logger.info("🗿 Disk: %s", disk_stats(SAVE_PATH))
    chunk_size = choose_chunk_size()
    logger.info("📈 CHUNK_READ size chosen = %d bytes", chunk_size)
    # load checkpoint
    checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
    # load disposable domains
    disposable_domains = load_disposable_domains()
    # Phase 1: DOWNLOAD
    if not checkpoint.get("downloads_completed"):
        if not MAGNETS:
            logger.error("❌ MAGNETS list is empty (no magnet links). Aborting.")
            sys.exit(2)
        session = setup_session()
        handles = add_magnets(session, MAGNETS)
        name_info = wait_for_metadata_and_prioritize(handles)
        # build list of (name,handle,info,indices)
        handles_with_indices=[]
        total_targets=0
        for name,h,targets in handles:
            info = name_info.get(name)
            if not info:
                logger.warning("⚠️ No metadata for %s", name); continue
            indices=[]
            for i in range(info.num_files()):
                p=info.files().at(i).path
                for t in targets:
                    if p==t or p.lower()==t.lower() or Path(p).name==Path(t).name:
                        indices.append(i)
            if indices:
                total_targets += len(indices)
                handles_with_indices.append((name,h,info,indices))
            else:
                logger.warning("⚠️ No targets matched for %s", name)
        if total_targets == 0:
            logger.error("❌ No target files discovered across all magnets; aborting.")
            sys.exit(3)
        pending = []
        for name,h,info,indices in handles_with_indices:
            for idx in indices:
                local = local_path_for_index(info, idx)
                expected = info.files().at(idx).size
                pending.append({"name":name,"handle":h,"info":info,"index":idx,"path":local,"expected":expected})
        # wait loop
        logger.info("📥 Waiting for all target files to complete (this can take long)")
        while pending and not stop_event.is_set():
            next_pending=[]
            for rec in pending:
                try:
                    prog = rec["handle"].file_progress()
                    got = prog[rec["index"]] if rec["index"] < len(prog) else None
                except Exception:
                    got = None
                local = rec["path"]
                local_exists = local.exists()
                local_size = local.stat().st_size if local_exists else 0
                expected = rec["expected"]
                if local_exists and local_size >= expected and (got is None or got >= expected):
                    logger.info("✅ Download complete %s idx %d -> %s (%d/%d)", rec["name"], rec["index"], local.name, local_size, expected)
                else:
                    logger.info("⏳ Pending %s idx %d local %d/%d torrent_prog %s", rec["name"], rec["index"], local_size, expected, str(got))
                    next_pending.append(rec)
            pending = next_pending
            if pending:
                time.sleep(10)
        if stop_event.is_set():
            logger.warning("Stop requested during downloads; exiting.")
            return
        # compute downloaded files
        downloaded_files=[]
        for name,h,info,indices in handles_with_indices:
            for idx in indices:
                local = local_path_for_index(info, idx)
                if not local.exists():
                    alt = SAVE_PATH / info.files().at(idx).path
                    if alt.exists():
                        local = alt
                if local.exists():
                    downloaded_files.append(str(local))
        checkpoint["downloads_completed"]=True
        checkpoint["downloaded_files"]=downloaded_files
        checkpoint["download_time"]=datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        logger.info("✅ Download phase complete: %d files", len(downloaded_files))
    else:
        logger.info("🗿 downloads_completed found in checkpoint; skipping download phase")
        downloaded_files = checkpoint.get("downloaded_files", [])
        if not downloaded_files:
            logger.error("❌ downloads_completed=true but no downloaded_files present; aborting")
            sys.exit(4)

    # Phase 2/3/4: Extraction, filter disposable, produce chunk parquet
    if not checkpoint.get("extraction_completed"):
        # validate tar files exist
        tar_files = [p for p in downloaded_files if Path(p).exists()]
        if not tar_files:
            logger.error("❌ No tar files found to process; aborting")
            sys.exit(5)
        logger.info("📦 Starting extraction on %d tar files with %d workers", len(tar_files), WORKERS)
        chunk_files, extraction_stats = run_extraction(tar_files, chunk_size, disposable_domains, WORKERS)
        checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
        checkpoint["extraction_completed"] = True
        checkpoint["chunk_files"] = chunk_files
        checkpoint["extraction_stats"] = extraction_stats
        checkpoint["extraction_time"] = datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        logger.info("✅ Extraction completed; chunks: %d", len(chunk_files))
    else:
        logger.info("🗿 extraction_completed found in checkpoint; skipping extraction")
        chunk_files = checkpoint.get("chunk_files", [])
        if not chunk_files:
            logger.error("❌ extraction_completed=true but chunk_files empty; aborting")
            sys.exit(6)

    # Phase 5: Deduplication (DuckDB)
    if not checkpoint.get("dedup_completed"):
        logger.info("🦆 Starting deduplication with DuckDB")
        parts, dedup_stats = run_dedup(chunk_files, str(DUCKDB_PATH), PART_ROWS)
        checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
        checkpoint["dedup_completed"] = True
        checkpoint["dedup_stats"] = dedup_stats
        checkpoint["final_parts"] = parts
        checkpoint["dedup_time"] = datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        logger.info("✅ Dedup completed: parts=%d", len(parts))
    else:
        logger.info("🦆 dedup_completed found in checkpoint; skipping dedup")
        parts = checkpoint.get("final_parts", [])
        if not parts:
            logger.error("❌ dedup_completed=true but final_parts empty; aborting")
            sys.exit(7)

    # build stats.json
    stats = {
        "extraction_stats": checkpoint.get("extraction_stats", {}),
        "dedup_stats": checkpoint.get("dedup_stats", {}),
        "final_parts": len(parts)
    }
    save_json(CHECKPOINT_DIR / "stats.json", stats)

    # Phase 6: Upload parts + checkpoint + stats to HF dataset Trader_Emails
    if not checkpoint.get("uploaded_completed"):
        success = hf_upload(parts, checkpoint, stats, HF_TOKEN, HF_DATASET)
        checkpoint = load_json(CHECKPOINT_DIR / "checkpoint.json")
        checkpoint["uploaded_completed"] = bool(success)
        checkpoint["upload_time"] = datetime.now(timezone.utc).isoformat()
        save_json(CHECKPOINT_DIR / "checkpoint.json", checkpoint)
        if success:
            logger.info("✅ Upload to HF completed")
        else:
            logger.warning("⚠️ Upload skipped or failed")
    else:
        logger.info("📤 uploaded_completed found in checkpoint; skipping upload")

    logger.info("✅ Pipeline finished. Results in %s", SAVE_PATH)
    logger.info("📉 Disk final: %s", disk_stats(SAVE_PATH))

# graceful stop
signal.signal(signal.SIGINT, lambda s,f: stop_event.set())
signal.signal(signal.SIGTERM, lambda s,f: stop_event.set())

if __name__ == "__main__":
    main()
