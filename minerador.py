#!/usr/bin/env python3
"""
minerador.py — robust miner adapted to use SAVE_PATH for all filesystem operations.

- All paths derived from SAVE_PATH
- stop_event used for graceful stops
- MAGNETS embedded
- Throttled polling/logging (POLL_INTERVAL default 60s)
- Streaming extraction of .tar.gz members -> sqlite (as in your preferred original)
- Bulk export to parquet per-member and upload via Hugging Face during processing
- Checkpointing saved to SAVE_PATH/checkpoints/checkpoint.json
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
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any
from threading import Event

import libtorrent as lt
from huggingface_hub import HfApi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table

# ===== Configuration =====
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data")).expanduser().resolve()
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
DB_PATH = SAVE_PATH / "emails.db"
CHECKPOINT_PATH = SAVE_PATH / "checkpoints" / "checkpoint.json"
LOG_PATH = SAVE_PATH / "minerador.log"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
Path(CHECKPOINT_PATH).parent.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_DATASET_NAME = os.environ.get("HF_DATASET_NAME", "Trader_Emails")

# Keep default polling low-frequency to avoid huge logs
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BATCH_INSERT = int(os.environ.get("BATCH_INSERT", "5000"))
BATCH_EXPORT_ROWS = int(os.environ.get("BATCH_EXPORT_ROWS", "200000"))
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(256 * 1024 * 1024)))  # 256MB

# Embedded MAGNETS (from your message)
MAGNETS = [
  {
    "name": "Collection #2-#5 & Antipublic",
    "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2f%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2f%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2f%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
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

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# Logging
console = Console()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s", handlers=[RichHandler(console=console, rich_tracebacks=True)])
logger = logging.getLogger("minerador")
logger.setLevel(LOG_LEVEL)
file_handler = logging.FileHandler(str(LOG_PATH))
file_handler.setLevel(LOG_LEVEL)
file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# stop event
stop_event = Event()

def handle_signal(signum, frame):
    logger.warning("⚠️ Signal %s received — stopping after current item", signum)
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Utilities
def human(n:int)->str:
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024: return f"{n:.2f}{unit}"; n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path:Path=SAVE_PATH) -> Dict[str,int]:
    du = shutil.disk_usage(str(path))
    return {"total":du.total,"used":du.used,"free":du.free}

def save_json(path:Path, obj:Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf8")

def load_json(path:Path)->Any:
    if not path.exists(): return {}
    try: return json.loads(path.read_text(encoding="utf8"))
    except Exception: return {}

# SQLite helpers (preserve your original)
def init_sqlite(db_path:Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    try:
        conn.execute("PRAGMA mmap_size = 268435456;")
    except Exception:
        pass
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        email TEXT PRIMARY KEY,
        nome TEXT,
        origem TEXT,
        data TEXT
    );
    """)
    conn.commit()
    return conn

def batch_insert(conn:sqlite3.Connection, records:List[Tuple[str,str,str,str]])->int:
    if not records: return 0
    cur = conn.cursor()
    try:
        cur.executemany("INSERT OR IGNORE INTO emails(email,nome,origem,data) VALUES (?, ?, ?, ?)", records)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    except Exception:
        conn.rollback()
        logger.exception("❌ SQLite batch insert failed")
        return 0

def delete_uploaded_rows(conn:sqlite3.Connection, emails:List[str]):
    if not emails: return
    try:
        cur = conn.cursor()
        cur.executemany("DELETE FROM emails WHERE email=?", [(e,) for e in emails])
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("⚠️ Failed to delete uploaded rows")

# HF helpers
def hf_prepare(token:str, dataset_name:str):
    if not token:
        raise RuntimeError("HF_TOKEN not provided")
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{dataset_name}"
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info("✅ Created HF dataset %s", repo_id)
    except Exception as e:
        logger.info("ℹ️ HF create returned: %s", e)
    return api, repo_id, user

def hf_upload_file(api:HfApi, token:str, repo_id:str, local_path:Path, repo_path:str)->bool:
    try:
        api.upload_file(path_or_fileobj=str(local_path), path_in_repo=repo_path, repo_id=repo_id, repo_type="dataset", token=token)
        logger.info("📤 Uploaded %s", repo_path)
        return True
    except Exception:
        logger.exception("❌ HF upload failed for %s", local_path)
        return False

# Name guess
def guess_name(email:str)->str:
    local = email.split("@",1)[0]
    no_digits = re.sub(r"\d+", "", local)
    spaced = re.sub(r"[_.\-]+", " ", no_digits).strip()
    if not spaced: return ""
    return " ".join([p.capitalize() for p in spaced.split()])

# Torrent helpers
def print_metadata(info:lt.torrent_info):
    n = info.num_files()
    table = Table(title="Torrent metadata files", show_header=True, header_style="bold magenta")
    table.add_column("idx", style="dim", width=6)
    table.add_column("path", overflow="fold")
    table.add_column("size", justify="right")
    for i in range(n):
        try:
            fe = info.files().at(i)
            table.add_row(str(i), fe.path, f"{fe.size:,}")
        except Exception:
            table.add_row(str(i), "<error>", "0")
    console.print(table)

def find_target_indices(info:lt.torrent_info, targets:List[str])->Tuple[List[int],List[str]]:
    n = info.num_files()
    idx_to_path = {}
    for i in range(n):
        try:
            idx_to_path[i] = info.files().at(i).path
        except Exception:
            idx_to_path[i] = ""
    paths_lower = {i: p.lower() for i,p in idx_to_path.items()}
    basenames = {i: Path(p).name for i,p in idx_to_path.items()}
    basenames_lower = {i: basenames[i].lower() for i in basenames}
    found=[]
    missing=[]
    for t in targets:
        matched=False
        for i,p in idx_to_path.items():
            if p==t:
                found.append(i); matched=True; break
        if matched: continue
        tl = t.lower()
        for i,pl in paths_lower.items():
            if pl==tl:
                found.append(i); matched=True; break
        if matched: continue
        tb = Path(t).name
        for i,b in basenames.items():
            if b==tb:
                found.append(i); matched=True; break
        if matched: continue
        for i,bl in basenames_lower.items():
            if bl==tb.lower():
                found.append(i); matched=True; break
        if not matched:
            missing.append(t)
    return sorted(set(found)), missing

def local_path_for_index(save_path:Path, info:lt.torrent_info, index:int)->Path:
    try:
        torrent_name = info.name()
        file_path = info.files().at(index).path
        return save_path / torrent_name / file_path
    except Exception:
        return save_path / "unknown" / f"file_{index}"

# Download wait (throttled logging)
def wait_for_file_complete(session, handle, file_index:int, expected_size:int, poll_interval:int=POLL_INTERVAL):
    last_log=0
    last_pct=-1.0
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        try:
            fprog = handle.file_progress()
            got = fprog[file_index] if file_index < len(fprog) else 0
        except Exception:
            got = 0
        pct = (got / expected_size * 100.0) if expected_size else 0.0
        now = time.time()
        if (pct - last_pct >= 1.0) or (now - last_log >= POLL_INTERVAL):
            logger.info("📈 Progress file[%d] = %d/%d (%.2f%%)", file_index, got, expected_size, pct)
            last_log = now
            last_pct = pct
        if expected_size and got >= expected_size:
            # try flush
            try:
                if hasattr(session, "flush_cache"):
                    session.flush_cache()
            except Exception:
                pass
            # wait short time for on-disk file
            return True
        time.sleep(poll_interval)

# Sanitize
def sanitize_filename(s:str)->str:
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in s)[:200].replace(" ","_")

# Process tar and upload (streaming)
def process_tar_and_upload(conn:sqlite3.Connection, api:HfApi, token:str, repo_id:str, info:lt.torrent_info, tar_path:Path):
    logger.info("📦 Opening tar %s", tar_path)
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar:
                if stop_event.is_set():
                    logger.warning("⚠️ Stop event set; breaking member loop")
                    break
                if not member.isfile(): continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")): continue
                logger.info("📄 Processing member %s", member.name)
                fobj = tar.extractfile(member)
                if fobj is None:
                    logger.warning("⚠️ Could not extract member %s", member.name); continue
                batch=[]
                extracted_count=0
                inserted_count=0
                for raw_line in fobj:
                    if stop_event.is_set():
                        break
                    for email_b in EMAIL_REGEX.findall(raw_line):
                        try:
                            email = email_b.decode("utf8","ignore").strip().lower()
                        except Exception:
                            email = email_b.decode("latin1","ignore").strip().lower()
                        if not email: continue
                        nome = guess_name(email)
                        data_iso = datetime.now(timezone.utc).isoformat()
                        batch.append((email, nome, member.name, data_iso))
                        extracted_count += 1
                        if len(batch) >= BATCH_INSERT:
                            inserted = batch_insert(conn, batch)
                            inserted_count += inserted
                            batch.clear()
                    # occasional free-space check every 50k extracted
                    if extracted_count and (extracted_count % 50000 == 0):
                        free = disk_usage(SAVE_PATH)["free"]
                        logger.info("📉 Free disk: %s", human(free))
                        if free < MIN_FREE_BYTES:
                            logger.error("❌ Disk critically low: %s", human(free))
                            raise RuntimeError("No space left")
                if batch:
                    inserted = batch_insert(conn, batch)
                    inserted_count += inserted
                    batch.clear()
                logger.info("📧 Member %s: extracted=%d new_inserted=%d", member.name, extracted_count, inserted_count)
                # Export a batch to parquet and upload
                cur = conn.cursor()
                cur.execute("SELECT email,nome,origem,data FROM emails LIMIT ?", (BATCH_EXPORT_ROWS,))
                rows = cur.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=["email","nome","origem","data"])
                    out_dir = EXPORT_DIR / sanitize_filename(info.name()) / sanitize_filename(tar_path.name)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    fname = f"{sanitize_filename(member.name)}_{ts}.parquet"
                    out_path = out_dir / fname
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, str(out_path), compression="snappy")
                    repo_path = f"{sanitize_filename(info.name())}/{sanitize_filename(tar_path.name)}/{fname}"
                    if hf_upload_file(api, token, repo_id, out_path, repo_path):
                        emails_exported = df["email"].tolist()
                        delete_uploaded_rows(conn, emails_exported)
                        try:
                            out_path.unlink(missing_ok=True)
                        except Exception:
                            logger.debug("Could not delete uploaded parquet")
                        logger.info("🧹 Uploaded and cleared %d rows", len(emails_exported))
        try:
            tar_path.unlink(missing_ok=True)
            logger.info("🧹 Removed processed tar %s", tar_path)
        except Exception:
            logger.debug("Could not delete tar file")
    except Exception:
        logger.exception("❌ Error processing tar %s", tar_path)

# Runner cleanup
def safe_runner_cleanup():
    logger.info("🧹 Performing runner cleanup")
    try:
        user_cache = Path.home() / ".cache"
        if user_cache.exists():
            shutil.rmtree(user_cache, ignore_errors=True); logger.info("Cleared user cache")
    except Exception:
        logger.debug("Could not clear user cache")
    try:
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR, ignore_errors=True); TEMP_DIR.mkdir(parents=True, exist_ok=True); logger.info("Cleared temp dir")
    except Exception:
        logger.debug("Could not clear temp dir")

# Main
def main():
    logger.info("🚀 Minerador starting; SAVE_PATH=%s", SAVE_PATH)
    logger.info("🗿 Disk before: %s", disk_usage(SAVE_PATH))
    safe_runner_cleanup()
    if not ensure_min_free_space():
        logger.warning("⚠️ Low free space at start; continuing but watch disk")

    if not HF_TOKEN:
        logger.error("❌ HF_TOKEN not set in env; set secret HF_TOKEN to enable HF uploads")
        # proceed; uploads will be skipped

    api=None; repo_id=None; hf_user=None
    if HF_TOKEN:
        try:
            api, repo_id, hf_user = hf_prepare(HF_TOKEN, HF_DATASET_NAME)
        except Exception:
            logger.exception("⚠️ HF prepare failed; continuing without uploads")

    conn = init_sqlite(DB_PATH)
    checkpoint = load_json(Path(CHECKPOINT_PATH))
    overall_start = time.time()

    # Add all magnets at once, then wait for metadata, prioritize and wait for downloads
    session = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    handles = []
    for t in MAGNETS:
        if stop_event.is_set(): break
        params = lt.parse_magnet_uri(t["magnet"])
        params.save_path = str(SAVE_PATH)
        h = session.add_torrent(params)
        handles.append((t["name"], h, t["targets"]))
        logger.info("📥 Added magnet %s", t["name"])

    # Wait for metadata for all
    meta_start = time.time()
    meta_timeout = 600
    pending = handles[:]
    name_info = {}
    while pending and not stop_event.is_set():
        new=[]
        for name,h,targets in pending:
            st = h.status()
            if st.has_metadata:
                try:
                    info = h.get_torrent_info()
                except Exception:
                    info = h.get_torrent_info()
                name_info[name] = info
                logger.info("ℹ️ Metadata ready for %s", name)
            else:
                new.append((name,h,targets))
        pending = new
        if pending:
            if time.time() - meta_start > meta_timeout:
                logger.warning("⚠️ Metadata wait timeout")
                break
            time.sleep(3)

    # Prioritize targets
    handles_with_indices=[]
    for name,h,targets in handles:
        info = name_info.get(name)
        if not info:
            logger.warning("⚠️ No metadata for %s; skipping", name); continue
        found_indices, missing = find_target_indices(info, targets)
        if missing:
            logger.warning("⚠️ Missing targets for %s: %s", name, missing)
            # print metadata to help user fix
            print_metadata(info)
        # set priorities
        for i in range(info.num_files()):
            try:
                pr = 7 if i in found_indices else 0
                h.file_priority(i, pr)
            except Exception:
                pass
        if found_indices:
            handles_with_indices.append((name,h,info,found_indices))
            logger.info("📥 Prioritized %d target files for %s", len(found_indices), name)

    # Build pending list and wait for all target files to be fully downloaded (pieces + local file)
    pending_files=[]
    for name,h,info,indices in handles_with_indices:
        for idx in indices:
            expected = info.files().at(idx).size
            local = local_path_for_index(SAVE_PATH, info, idx)
            pending_files.append({"name":name,"handle":h,"info":info,"index":idx,"path":local,"expected":expected})
    if not pending_files:
        logger.error("❌ No target files discovered across torrents — aborting")
        return

    logger.info("📥 Waiting for all %d target files to complete", len(pending_files))
    while pending_files and not stop_event.is_set():
        new=[]
        for rec in pending_files:
            try:
                fprog = rec["handle"].file_progress()
                got = fprog[rec["index"]] if rec["index"] < len(fprog) else 0
            except Exception:
                got = 0
            logger.debug("Progress %s idx %d pieces=%d expected=%d", rec["name"], rec["index"], got, rec["expected"])
            if rec["expected"] and got >= rec["expected"]:
                # flush cache if possible and wait for local file
                try:
                    if hasattr(session, "flush_cache"):
                        session.flush_cache()
                except Exception:
                    pass
                ok = wait_for_local_file(rec["path"], rec["expected"], timeout=120)
                if ok:
                    logger.info("✅ Local file ready: %s", rec["path"])
                    continue
                else:
                    logger.info("⚠️ Local file not written yet for %s; will continue waiting", rec["path"])
                    new.append(rec)
            else:
                new.append(rec)
        pending_files = new
        if pending_files:
            time.sleep(POLL_INTERVAL)

    if stop_event.is_set():
        logger.warning("Stop requested during download phase; exiting")
        return

    # Now process each downloaded file (streaming)
    downloaded = []
    for name,h,info,indices in handles_with_indices:
        for idx in indices:
            local = local_path_for_index(SAVE_PATH, info, idx)
            if local.exists():
                downloaded.append(local)
            else:
                alt = SAVE_PATH / info.files().at(idx).path
                if alt.exists():
                    downloaded.append(alt)
    logger.info("📦 Starting processing of %d tar files", len(downloaded))
    for tar_path in downloaded:
        if stop_event.is_set(): break
        process_tar_and_upload(conn, api, HF_TOKEN, repo_id, info, tar_path)
        # checkpoint per file
        cp = load_json(Path(CHECKPOINT_PATH))
        cp.setdefault("processed_files", [])
        cp["processed_files"].append(str(tar_path))
        cp["last_processed_at"] = datetime.now(timezone.utc).isoformat()
        save_json(Path(CHECKPOINT_PATH), cp)

    # finalize
    try:
        cur = conn.cursor(); cur.execute("SELECT COUNT(*) FROM emails"); total = cur.fetchone()[0]; logger.info("📧 Total unique emails (sqlite): %d", total)
    except Exception:
        logger.exception("⚠️ Could not fetch final count")
    finally:
        try: conn.close() 
        except Exception: pass

    logger.info("✅ Minerador finished. Disk after: %s", disk_usage(SAVE_PATH))

def wait_for_local_file(path:Path, expected_size:int, timeout:int=120)->bool:
    start=time.time()
    while time.time()-start < timeout:
        if path.exists():
            try: s = path.stat().st_size
            except Exception: s = 0
            if s >= expected_size: return True
        time.sleep(1)
    return False

if __name__ == "__main__":
    main()
