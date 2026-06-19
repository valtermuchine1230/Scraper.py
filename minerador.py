#!/usr/bin/env python3
"""
minerador.py — Robust miner with SAVE_PATH-only paths and safe stop_event handling.

Changes in this version:
- Replaced the module-global boolean `_stop_requested` with a threading.Event `stop_event`.
  All checks and signal handlers use stop_event.is_set() / stop_event.set() respectively.
  This prevents UnboundLocalError and provides thread-safe stop signaling.
- All filesystem paths are derived from SAVE_PATH (no hardcoded /mnt or ./data inside the code).
- Preserves functionality: libtorrent download of exact target files, wait via file_progress,
  streaming extraction of .tar.gz members, deduplication via SQLite, export to Parquet,
  upload to Hugging Face, checkpointing, cleanup and rich logs.
- Ensures all temporary files, exports, and DB are under SAVE_PATH.
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
import subprocess
import gc
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any
from threading import Event

import unicodedata
import difflib

import libtorrent as lt
from huggingface_hub import HfApi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table

# ====== Configuration (NO hardcoded paths except SAVE_PATH fallback) ======
SAVE_PATH = Path(os.environ.get("SAVE_PATH", "./data"))
SAVE_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_DIR = SAVE_PATH / "exports"
TEMP_DIR = SAVE_PATH / "temp"
DB_PATH = SAVE_PATH / "emails.db"
CHECKPOINT_PATH = SAVE_PATH / "checkpoint.json"
LOG_PATH = SAVE_PATH / "minerador.log"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.environ.get("HF_TOKEN")  # required (pass via secrets)
HF_DATASET_NAME = os.environ.get("HF_DATASET_NAME", "email_miner_dataset")

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "6"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BATCH_INSERT = int(os.environ.get("BATCH_INSERT", "5000"))
BATCH_EXPORT_ROWS = int(os.environ.get("BATCH_EXPORT_ROWS", "200000"))
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_BYTES", str(256 * 1024 * 1024)))  # 256MB

# Define MAGNETS with exact metadata targets (paths or basenames).
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

# ===== Logging setup (console + file inside SAVE_PATH) =====
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

# emojis
E = {
    "start": "🚀",
    "download": "📥",
    "extract": "📦",
    "stats": "📊",
    "speed": "📈",
    "space": "📉",
    "email": "📧",
    "upload": "📤",
    "clean": "🧹",
    "warn": "⚠️",
    "error": "❌",
    "ok": "✅",
    "info": "🗿",
}

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# ===== Stop event (thread-safe, avoids UnboundLocalError) =====
stop_event = Event()

def handle_signal(signum, frame):
    logger.warning(f"{E['warn']} Signal {signum} received; setting stop_event.")
    stop_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ===== Utilities =====
def human(n: int) -> str:
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path: Path = SAVE_PATH) -> Dict[str,int]:
    du = shutil.disk_usage(str(path))
    return {"total": du.total, "used": du.used, "free": du.free}

def ensure_min_free_space(min_bytes: int = MIN_FREE_BYTES) -> bool:
    free = disk_usage(SAVE_PATH)["free"]
    logger.info(f"{E['space']} Espaço livre em {SAVE_PATH}: {human(free)}")
    return free >= min_bytes

def save_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

# ===== SQLite helpers =====
def init_sqlite(db_path: Path) -> sqlite3.Connection:
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

def batch_insert(conn: sqlite3.Connection, records: List[Tuple[str,str,str,str]]) -> int:
    if not records:
        return 0
    cur = conn.cursor()
    try:
        cur.executemany("INSERT OR IGNORE INTO emails(email,nome,origem,data) VALUES (?, ?, ?, ?)", records)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    except Exception:
        conn.rollback()
        logger.exception(f"{E['error']} SQLite batch insert failed")
        return 0

def delete_uploaded_rows(conn: sqlite3.Connection, emails: List[str]):
    if not emails:
        return
    try:
        cur = conn.cursor()
        cur.executemany("DELETE FROM emails WHERE email=?", [(e,) for e in emails])
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(f"{E['warn']} Failed to delete uploaded rows")

# ===== Hugging Face helpers =====
def hf_prepare(token: str, dataset_name: str):
    if not token:
        raise RuntimeError("HF_TOKEN not provided in environment")
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{dataset_name}"
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info(f"{E['ok']} Created dataset {repo_id}")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"{E['ok']} Dataset already exists: {repo_id}")
        else:
            logger.warning(f"{E['warn']} create_repo returned: {e}")
    return api, repo_id, user

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str) -> bool:
    try:
        api.upload_file(path_or_fileobj=str(local_path),
                        path_in_repo=repo_path,
                        repo_id=repo_id,
                        repo_type="dataset",
                        token=token)
        logger.info(f"{E['upload']} Uploaded {repo_path}")
        return True
    except Exception:
        logger.exception(f"{E['error']} HF upload failed for {local_path}")
        return False

# ===== Name guess heuristic =====
def guess_name(email: str) -> str:
    local = email.split("@",1)[0]
    no_digits = re.sub(r"\d+", "", local)
    spaced = re.sub(r"[_.\-]+", " ", no_digits).strip()
    if not spaced:
        return ""
    return " ".join([p.capitalize() for p in spaced.split()])

# ===== Torrent helpers (strict matching) =====
def print_metadata(info: lt.torrent_info):
    n = info.num_files()
    table = Table(title="Torrent metadata files", show_header=True, header_style="bold magenta")
    table.add_column("idx", style="dim", width=6)
    table.add_column("path", overflow="fold")
    table.add_column("size", justify="right")
    for i in range(n):
        fe = info.files().at(i)
        table.add_row(str(i), fe.path, f"{fe.size:,}")
    console.print(table)

def find_target_indices(torrent_info: lt.torrent_info, targets: List[str]) -> Tuple[List[int], List[str]]:
    n = torrent_info.num_files()
    idx_to_path = {i: torrent_info.files().at(i).path for i in range(n)}
    paths_lower = {i: idx_to_path[i].lower() for i in idx_to_path}
    basenames = {i: Path(idx_to_path[i]).name for i in idx_to_path}
    basenames_lower = {i: basenames[i].lower() for i in basenames}
    found = []
    missing = []
    for t in targets:
        matched = False
        for i,p in idx_to_path.items():
            if p == t:
                found.append(i); matched=True; break
        if matched: continue
        tl = t.lower()
        for i,pl in paths_lower.items():
            if pl == tl:
                found.append(i); matched=True; break
        if matched: continue
        tb = Path(t).name
        for i,b in basenames.items():
            if b == tb:
                found.append(i); matched=True; break
        if matched: continue
        for i,bl in basenames_lower.items():
            if bl == tb.lower():
                found.append(i); matched=True; break
        if not matched:
            missing.append(t)
    found = sorted(set(found))
    return found, missing

def local_path_for_index(save_path: Path, torrent_info: lt.torrent_info, index: int) -> Path:
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    return save_path / torrent_name / file_path

# ===== Processing (streaming) =====
def wait_for_file_complete(handle: lt.torrent_handle, file_index: int, expected_size: int, poll_interval: int = POLL_INTERVAL):
    last_log = 0
    while True:
        if stop_event.is_set():
            raise KeyboardInterrupt()
        fprog = handle.file_progress()
        got = fprog[file_index] if file_index < len(fprog) else 0
        pct = (got / expected_size * 100) if expected_size else 0.0
        now = time.time()
        if now - last_log >= 5:
            logger.info(f"{E['download']} Progresso file[{file_index}] = {got:,}/{expected_size:,} ({pct:.2f}%)")
            last_log = now
        if expected_size and got >= expected_size:
            logger.info(f"{E['ok']} File index={file_index} download complete ({got:,} bytes).")
            return True
        time.sleep(poll_interval)

def sanitize_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in s)[:200].replace(" ", "_")

def process_tar_and_upload(conn: sqlite3.Connection, api: HfApi, token: str, repo_id: str, torrent_info: lt.torrent_info, tar_path: Path):
    logger.info(f"{E['extract']} Opening tar {tar_path}")
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            for member in tar:
                if stop_event.is_set():
                    logger.warning(f"{E['warn']} Stop event set; exiting member loop.")
                    break
                if not member.isfile():
                    continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue
                logger.info(f"{E['extract']} Member: {member.name}")
                fobj = tar.extractfile(member)
                if fobj is None:
                    logger.warning(f"{E['warn']} Could not extract member {member.name}")
                    continue
                batch = []
                extracted_count = 0
                inserted_count = 0
                for raw_line in fobj:
                    if stop_event.is_set():
                        logger.warning(f"{E['warn']} Stop event set during member processing.")
                        break
                    for email_b in EMAIL_REGEX.findall(raw_line):
                        try:
                            email = email_b.decode("utf8", "ignore").strip().lower()
                        except Exception:
                            email = email_b.decode("latin1", "ignore").strip().lower()
                        if not email:
                            continue
                        nome = guess_name(email)
                        data_iso = datetime.now(timezone.utc).isoformat()
                        batch.append((email, nome, member.name, data_iso))
                        extracted_count += 1
                        if len(batch) >= BATCH_INSERT:
                            inserted = batch_insert(conn, batch)
                            inserted_count += inserted
                            batch.clear()
                    if extracted_count and extracted_count % 50000 == 0:
                        free = disk_usage(SAVE_PATH)["free"]
                        logger.info(f"{E['space']} Espaço livre: {human(free)}")
                        if free < MIN_FREE_BYTES:
                            logger.error(f"{E['error']} Espaço crítico during processing: {human(free)}")
                            raise RuntimeError("No space left during processing")
                if batch:
                    inserted = batch_insert(conn, batch)
                    inserted_count += inserted
                    batch.clear()
                logger.info(f"{E['email']} Member {member.name}: extracted={extracted_count:,} new_inserted={inserted_count:,}")
                # Export and upload
                cur = conn.cursor()
                cur.execute("SELECT email,nome,origem,data FROM emails LIMIT ?", (BATCH_EXPORT_ROWS,))
                rows = cur.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=["email","nome","origem","data"])
                    out_dir = EXPORT_DIR / sanitize_filename(torrent_info.name()) / sanitize_filename(tar_path.name)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    fname = f"{sanitize_filename(member.name)}_{ts}.parquet"
                    out_path = out_dir / fname
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, str(out_path), compression="snappy")
                    repo_path = f"{sanitize_filename(torrent_info.name())}/{sanitize_filename(tar_path.name)}/{fname}"
                    if hf_upload_file(api, token, repo_id, out_path, repo_path):
                        emails_exported = df["email"].tolist()
                        delete_uploaded_rows(conn, emails_exported)
                        try:
                            out_path.unlink(missing_ok=True)
                        except Exception:
                            logger.debug("Could not delete parquet after upload")
                        logger.info(f"{E['clean']} Uploaded and cleared {len(emails_exported):,} rows from sqlite")
        try:
            tar_path.unlink(missing_ok=True)
            logger.info(f"{E['clean']} Removed processed tar {tar_path}")
        except Exception:
            logger.debug("Could not remove tar (may be open elsewhere)")
    except tarfile.ReadError:
        logger.exception(f"{E['error']} tarfile.ReadError when opening {tar_path}")
    except Exception:
        logger.exception(f"{E['error']} Error processing tar {tar_path}")

# ===== Runner cleanup (safe) =====
def safe_runner_cleanup():
    logger.info(f"{E['clean']} Performing safe cleanup.")
    try:
        user_cache = Path.home() / ".cache"
        if user_cache.exists():
            shutil.rmtree(user_cache, ignore_errors=True)
            logger.info("Cleared user cache")
    except Exception:
        logger.debug("Could not clear user cache")
    try:
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR, ignore_errors=True)
            TEMP_DIR.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cleared temporary dir {TEMP_DIR}")
    except Exception:
        logger.debug("Could not clear temp dir")

# ===== Main =====
def main():
    logger.info(f"{E['start']} Minerador starting")
    logger.info(f"{E['info']} SAVE_PATH detected: {SAVE_PATH}")
    logger.info(f"{E['info']} Working directory: {SAVE_PATH}")
    logger.info(f"{E['info']} Exports directory: {EXPORT_DIR}")
    logger.info(f"{E['info']} Temp directory: {TEMP_DIR}")
    logger.info(f"{E['info']} SQLite DB path: {DB_PATH}")
    logger.info(f"{E['stats']} Disk before cleanup: {disk_usage(SAVE_PATH)}")

    safe_runner_cleanup()
    if not ensure_min_free_space():
        logger.warning(f"{E['warn']} Low free space at start; continuing but watch disk usage")

    if not HF_TOKEN:
        logger.error(f"{E['error']} HF_TOKEN not set in environment. Set secret HF_TOKEN and re-run.")
        sys.exit(2)

    api, repo_id, hf_user = hf_prepare(HF_TOKEN, HF_DATASET_NAME)

    conn = init_sqlite(DB_PATH)

    checkpoint = load_json(CHECKPOINT_PATH)

    overall_start = time.time()

    for torrent_item in MAGNETS:
        if stop_event.is_set():
            logger.info(f"{E['warn']} Stop event set before starting next torrent; breaking.")
            break
        tname = torrent_item.get("name")
        magnet_uri = torrent_item.get("magnet")
        targets = torrent_item.get("targets", [])
        logger.info(f"{E['download']} Starting torrent: {tname}")

        session = lt.session({'listen_interfaces': '0.0.0.0:6881'})
        params = lt.parse_magnet_uri(magnet_uri)
        params.save_path = str(SAVE_PATH)
        handle = session.add_torrent(params)

        while not handle.has_metadata():
            st = handle.status()
            logger.info(f"{E['download']} Waiting metadata: peers={st.num_peers} state={st.state}")
            if stop_event.is_set():
                break
            time.sleep(POLL_INTERVAL)
        if stop_event.is_set():
            break

        info = handle.get_torrent_info()
        print_metadata(info)

        found_indices, missing_targets = find_target_indices(info, targets)
        if missing_targets:
            logger.error(f"{E['error']} The following targets were NOT found in torrent metadata for '{tname}':")
            for mt in missing_targets:
                logger.error(f"  - {mt}")
            logger.error("Please correct MAGNETS[].targets to match metadata exactly. Skipping this torrent.")
            continue

        logger.info(f"{E['download']} Found target file indices: {found_indices}")

        nfiles = info.num_files()
        for i in range(nfiles):
            pr = 7 if i in found_indices else 0
            handle.file_priority(i, pr)
        logger.info(f"{E['download']} Priorities applied; only targets will be downloaded.")

        for idx in found_indices:
            if stop_event.is_set():
                logger.info(f"{E['warn']} Stop event set; breaking target loop for {tname}.")
                break
            expected_size = info.files().at(idx).size
            logger.info(f"{E['download']} Waiting for file index {idx} to complete, expected {expected_size:,} bytes")
            try:
                wait_for_file_complete(handle, idx, expected_size)
            except KeyboardInterrupt:
                logger.warning(f"{E['warn']} Interrupted while waiting for file {idx}")
                stop_event.set()
                break
            except Exception:
                logger.exception(f"{E['error']} Error while waiting file {idx}; skipping")
                continue

            local_tar = local_path_for_index(SAVE_PATH, info, idx)
            if not local_tar.exists():
                alt = SAVE_PATH / info.files().at(idx).path
                if alt.exists():
                    local_tar = alt
                    logger.info(f"{E['warn']} Using fallback path for tar: {local_tar}")
                else:
                    logger.error(f"{E['error']} Expected file not present on disk: {local_tar}; skipping")
                    continue

            if local_tar.stat().st_size < expected_size:
                logger.warning(f"{E['warn']} Local file smaller than expected: {local_tar} ({local_tar.stat().st_size:,} < {expected_size:,}) — skipping")
                continue

            process_tar_and_upload(conn, api, HF_TOKEN, repo_id, info, local_tar)

            key = f"{tname}||{local_tar.name}"
            checkpoint[key] = {"index": idx, "path": str(local_tar), "processed_at": datetime.now(timezone.utc).isoformat()}
            save_json(CHECKPOINT_PATH, checkpoint)

        logger.info(f"{E['ok']} Torrent '{tname}' processing finished (targets done or skipped).")

    total_time = time.time() - overall_start
    logger.info(f"{E['stats']} Total runtime: {total_time/60:.2f} minutes")
    usage_after = disk_usage(SAVE_PATH)
    logger.info(f"{E['space']} Disk usage after: {usage_after}")

    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM emails")
        total_emails = cur.fetchone()[0]
        logger.info(f"{E['email']} Total unique emails persisted (sqlite): {total_emails:,}")
    except Exception:
        logger.exception("Could not fetch final email count")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info(f"{E['ok']} Minerador finished")

if __name__ == "__main__":
    main()
