#!/usr/bin/env python3
"""
Minerador -> Hugging Face (corrigido)

- Diagnóstico robusto quando arquivos alvo não são encontrados.
- Matching tolerante (basename, substring, normalized, close-match).
- Checkpoint local (checkpoint.json).
- Deduplicação persistente via SQLite (emails.db).
- Export incremental para Parquet e upload para Hugging Face Dataset privado.
- Logs com rich.
"""
import os
import re
import sys
import json
import time
import tarfile
import signal
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import difflib
import unicodedata

# bittorrent
import libtorrent as lt

# Hugging Face
from huggingface_hub import HfApi

# data export
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# rich logging
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table

# -------- CONFIG --------
# HF token: env HF_TOKEN overrides HF_TOKEN_DEFAULT.
HF_TOKEN_DEFAULT = "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"  # if you insist to embed token
HF_TOKEN = os.getenv("HF_TOKEN", HF_TOKEN_DEFAULT)

HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "email_miner_dataset")
SAVE_PATH = Path(os.getenv("SAVE_PATH", "."))
CHECKPOINT_PATH = SAVE_PATH / "checkpoint.json"
SQLITE_DB = SAVE_PATH / "emails.db"
EXPORT_DIR = SAVE_PATH / "exports"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "8"))
WAIT_RETRIES = int(os.getenv("WAIT_RETRIES", "20"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_REF = int(os.getenv("BATCH_REF", "200000"))  # how many rows to export in a batch

# magnets and targets (edit if needed)
MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        "targets": [
            "Collection #2_New combo cloud_Trading Collection.tar.gz",
            "Collection #4_BTC combos.tar.gz",
        ],
    },
    {
        "name": "Collection #1",
        "magnet": "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E&dn=Collection%201&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2f%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce",
        "targets": [
            "Collection #1_BTC combos.tar.gz",
            "Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection #1_OLD CLOUD_BTC combos.tar.gz",
        ],
    },
]

# ---------- Logging ----------
console = Console()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger("minerador")
logger.setLevel(LOG_LEVEL)

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# ---------- Helpers ----------
def normalize_for_match(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[^0-9a-zA-Z]+', ' ', s)
    return s.strip().lower()

def sanitize_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in s)[:200].replace(" ", "_")

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def list_disk_files(root: Path, max_files=2000):
    out = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = Path(dirpath) / fn
            try:
                size = full.stat().st_size
            except Exception:
                size = 0
            out.append((full, size))
            if len(out) >= max_files:
                return out
    return out

def print_metadata_file_list(info):
    n = info.num_files()
    table = Table(title="Torrent metadata files", show_header=True, header_style="bold magenta")
    table.add_column("idx", style="dim", width=6)
    table.add_column("path", overflow="fold")
    table.add_column("size", justify="right")
    for i in range(n):
        f = info.files().at(i)
        table.add_row(str(i), f.path, f"{f.size:,}")
    console.print(table)

def build_torrent_file_size_map(torrent_info):
    mapping = {}
    n = torrent_info.num_files()
    for i in range(n):
        p = torrent_info.files().at(i).path
        s = torrent_info.files().at(i).size
        mapping[p] = s
        mapping[os.path.basename(p)] = s
    return mapping

def expected_size_for_local_path(local_path: Path, file_size_map: dict):
    local_lower = str(local_path).replace("\\", "/").lower()
    candidates = []
    for meta_path, size in file_size_map.items():
        meta_norm = meta_path.replace("\\", "/").lower()
        if local_lower.endswith(meta_norm):
            candidates.append((len(meta_norm), size))
    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1]
    base = os.path.basename(local_path).lower()
    return file_size_map.get(base)

# ---------- SQLite persistence ----------
def init_sqlite(db_path: Path):
    safe_mkdir(db_path.parent)
    conn = sqlite3.connect(str(db_path), timeout=30)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        email TEXT PRIMARY KEY,
        nome TEXT,
        origem TEXT,
        data TEXT,
        uploaded INTEGER DEFAULT 0
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_uploaded ON emails(uploaded);")
    conn.commit()
    return conn

def insert_email_sqlite(conn, email, nome, origem, data_iso):
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO emails(email,nome,origem,data,uploaded) VALUES (?, ?, ?, ?, 0)",
                    (email, nome, origem, data_iso))
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        logger.exception("SQLite insert error for %s", email)
        return False

def mark_uploaded_for_rows(conn, emails_list):
    if not emails_list:
        return
    try:
        cur = conn.cursor()
        cur.executemany("UPDATE emails SET uploaded=1 WHERE email=?", [(e,) for e in emails_list])
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to mark uploaded rows")

# ---------- Checkpoint ----------
def load_checkpoint():
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.warning("Failed to load checkpoint.json; recreating.")
    return {}

def save_checkpoint(data):
    safe_mkdir(CHECKPOINT_PATH.parent)
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---------- Name guess ----------
def guess_name(email: str) -> str:
    local = email.split("@", 1)[0]
    no_digits = re.sub(r"\d+", "", local)
    spaced = re.sub(r"[_.\-]+", " ", no_digits).strip()
    if not spaced:
        return ""
    return " ".join([p.capitalize() for p in spaced.split()])

# ---------- Hugging Face helpers ----------
def hf_api_login_and_prepare_repo(token: str, dataset_name: str):
    api = HfApi()
    try:
        who = api.whoami(token=token)
        user = who.get("name") or who.get("user") or who.get("id")
    except Exception as e:
        logger.exception("HF whoami failed: %s", e)
        raise
    repo_id = f"{user}/{dataset_name}"
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info(f"✅ Created dataset: {repo_id}")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"Dataset already exists: {repo_id}")
        else:
            logger.info(f"Dataset check/create: {repo_id} (maybe exists). Msg: {e}")
    return api, repo_id, user

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str):
    try:
        api.upload_file(path_or_fileobj=str(local_path),
                        path_in_repo=repo_path,
                        repo_id=repo_id,
                        repo_type="dataset",
                        token=token)
        logger.info(f"Uploaded {repo_path}")
        return True
    except Exception:
        logger.exception("Upload failed for %s", local_path)
        return False

# ---------- Matching helpers ----------
def find_local_target_files(root: Path, targets):
    """
    Matching strategy:
    - exact basename
    - substring of full path (case-insensitive)
    - normalized basename match
    - close matches using difflib on normalized basenames
    """
    disk_files = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            disk_files.append(Path(dirpath) / fn)
    disk_norm_map = {normalize_for_match(p.name): p for p in disk_files}
    disk_full_norm_map = {normalize_for_match(str(p)): p for p in disk_files}
    matches = set()
    for target in targets:
        t_base = os.path.basename(target)
        t_norm = normalize_for_match(t_base)
        # exact basename
        for p in disk_files:
            if p.name.lower() == t_base.lower():
                matches.add(p)
        # substring in full path
        for p in disk_files:
            if t_base.lower() in str(p).lower():
                matches.add(p)
        # normalized exact
        if t_norm in disk_norm_map:
            matches.add(disk_norm_map[t_norm])
        # close matches
        close = difflib.get_close_matches(t_norm, list(disk_norm_map.keys()), n=3, cutoff=0.6)
        for c in close:
            matches.add(disk_norm_map[c])
    return sorted(matches)

# ---------- Diagnostics + download wait ----------
def download_and_wait_with_diagnostics(magnet: str, save_path: Path, targets):
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(magnet)
    params.save_path = str(save_path)
    handle = ses.add_torrent(params)
    logger.info("🔗 Magnet added. Waiting metadata...")
    while not handle.has_metadata():
        s = handle.status()
        logger.info(f"⏳ waiting metadata: peers={s.num_peers} state={s.state}")
        time.sleep(POLL_INTERVAL)
    logger.info("📦 Metadata obtained.")
    info = handle.get_torrent_info()
    print_metadata_file_list(info)
    file_size_map = build_torrent_file_size_map(info)
    # prioritize
    n_files = info.num_files()
    prioritized_count = 0
    for i in range(n_files):
        p = info.files().at(i).path
        for t in targets:
            if t.lower() in p.lower() or os.path.basename(p).lower() == t.lower():
                handle.file_priority(i, 7)
                prioritized_count += 1
                break
    logger.info(f"Prioritized {prioritized_count} files (if matched).")
    # wait loop
    retries = 0
    while True:
        s = handle.status()
        found = find_local_target_files(save_path, targets)
        logger.info(f"↓ Download global: {s.progress*100:.2f}% peers={s.num_peers} downrate={s.download_rate} B/s found={len(found)}")
        if found:
            logger.info(f"[green]Found {len(found)} matching file(s):[/green] {', '.join(str(p) for p in found)}")
            return handle, found, file_size_map
        if s.progress >= 1.0 or retries >= WAIT_RETRIES:
            logger.warning("Torrent appears complete or retries exceeded but no target files found. Running diagnostics...")
            # list disk files (first 500)
            disk_files = list_disk_files(save_path, max_files=500)
            if not disk_files:
                logger.warning(f"No files found on disk under {save_path} — maybe save_path is wrong.")
            else:
                table = Table(title=f"Disk files under {save_path} (first 200)", show_header=True)
                table.add_column("path", overflow="fold")
                table.add_column("size", justify="right")
                for p, sz in disk_files[:200]:
                    table.add_row(str(p), f"{sz:,}")
                console.print(table)
            # suggest close matches between metadata and disk
            meta_names = [info.files().at(i).path for i in range(info.num_files())]
            disk_names = [os.path.basename(p) for p, _ in disk_files]
            disk_norms = {normalize_for_match(n): n for n in disk_names}
            suggestions = {}
            for m in meta_names:
                mbase = os.path.basename(m)
                mnorm = normalize_for_match(mbase)
                close = difflib.get_close_matches(mnorm, list(disk_norms.keys()), n=3, cutoff=0.5)
                if close:
                    suggestions[mbase] = [disk_norms[c] for c in close]
            if suggestions:
                table2 = Table(title="Suggested close matches (metadata -> disk)", show_header=True)
                table2.add_column("metadata")
                table2.add_column("close_matches")
                for meta, closelist in suggestions.items():
                    table2.add_row(meta, ", ".join(closelist))
                console.print(table2)
            else:
                logger.info("No close matches found between metadata filenames and disk filenames.")
            # targets -> best matches
            target_norms = {t: normalize_for_match(os.path.basename(t)) for t in targets}
            best_matches = {}
            for t, tnorm in target_norms.items():
                best = difflib.get_close_matches(tnorm, list(disk_norms.keys()), n=5, cutoff=0.4)
                best_matches[t] = [disk_norms[b] for b in best]
            table3 = Table(title="Targets -> Best disk matches", show_header=True)
            table3.add_column("target")
            table3.add_column("best_matches")
            for t, bm in best_matches.items():
                table3.add_row(t, ", ".join(bm) if bm else "(none)")
            console.print(table3)
            return handle, [], file_size_map
        retries += 1
        time.sleep(POLL_INTERVAL)

# ---------- Main flow ----------
_stop_requested = False
def handle_sig(signum, frame):
    global _stop_requested
    logger.warning(f"Signal {signum} received; will stop after current item.")
    _stop_requested = True

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

def main():
    global HF_TOKEN
    HF_TOKEN = os.getenv("HF_TOKEN", HF_TOKEN)
    if not HF_TOKEN:
        logger.error("HF_TOKEN not set. Set env HF_TOKEN or embed in code.")
        sys.exit(1)
    api, repo_id, hf_user = hf_api_login_and_prepare_repo(HF_TOKEN, HF_DATASET_NAME)
    conn_sqlite = init_sqlite(SQLITE_DB)
    checkpoint = load_checkpoint()
    overall_report = {}

    for magnet_item in MAGNETS:
        if _stop_requested:
            break
        torrent_name = magnet_item.get("name")
        magnet_link = magnet_item.get("magnet")
        targets = magnet_item.get("targets", [])
        logger.info(f"[blue]Iniciando torrent:[/blue] {torrent_name}")
        handle, found_files, file_size_map = download_and_wait_with_diagnostics(magnet_link, SAVE_PATH, targets)
        if not found_files:
            logger.warning(f"No target files found for torrent {torrent_name}. See diagnostics above.")
            continue
        report_for_torrent = {"files_processed": 0, "members_processed": 0, "emails_found_per_file": {}, "emails_saved_per_file": {}}
        for tar_path in found_files:
            if _stop_requested:
                break
            tar_name = tar_path.name
            logger.info(f"[blue]Abrindo tar:[/blue] {tar_path}")
            if not tar_path.exists():
                logger.warning(f"Arquivo não existe (pular): {tar_path}")
                continue
            expected = expected_size_for_local_path(tar_path, file_size_map)
            if expected and tar_path.stat().st_size < expected:
                logger.warning(f"Arquivo {tar_path} ainda incompleto (pular).")
                continue
            try:
                with tarfile.open(tar_path, "r:*") as t:
                    for member in t:
                        if _stop_requested:
                            break
                        if not member.isfile():
                            continue
                        if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                            continue
                        key = f"{torrent_name}||{tar_name}||{member.name}"
                        if checkpoint.get(key) == "done":
                            logger.info(f"Skipping already done member {member.name}")
                            continue
                        logger.info(f"Processing member {member.name}")
                        checkpoint[key] = "processing"
                        save_checkpoint(checkpoint)
                        extracted = 0
                        inserted = 0
                        f = t.extractfile(member)
                        if f is None:
                            logger.warning(f"Could not extract member {member.name}; marking failed.")
                            checkpoint[key] = "failed"
                            save_checkpoint(checkpoint)
                            continue
                        for raw_line in f:
                            for email_b in EMAIL_REGEX.findall(raw_line):
                                try:
                                    email = email_b.decode("utf8", "ignore").strip().lower()
                                except Exception:
                                    email = email_b.decode("latin1", "ignore").strip().lower()
                                if not email:
                                    continue
                                nome = guess_name(email)
                                data_iso = datetime.now(timezone.utc).isoformat()
                                inserted_flag = insert_email_sqlite(conn_sqlite, email, nome, member.name, data_iso)
                                extracted += 1
                                if inserted_flag:
                                    inserted += 1
                        # export batch up to BATCH_REF
                        cur = conn_sqlite.cursor()
                        cur.execute("SELECT email,nome,origem,data FROM emails WHERE uploaded=0 LIMIT ?", (BATCH_REF,))
                        rows = cur.fetchall()
                        exported = 0
                        uploaded = 0
                        if rows:
                            df = pd.DataFrame(rows, columns=["email","nome","origem","data"])
                            safe_mkdir(EXPORT_DIR)
                            out_dir = EXPORT_DIR / sanitize_filename(torrent_name) / sanitize_filename(tar_name)
                            safe_mkdir(out_dir)
                            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                            fname = f"{sanitize_filename(member.name)}_{ts}.parquet"
                            out_path = out_dir / fname
                            table = pa.Table.from_pandas(df)
                            pq.write_table(table, str(out_path), compression="snappy")
                            exported = len(df)
                            repo_path = f"{sanitize_filename(torrent_name)}/{sanitize_filename(tar_name)}/{fname}"
                            if hf_upload_file(api, HF_TOKEN, repo_id, out_path, repo_path):
                                uploaded = exported
                                mark_uploaded_for_rows(conn_sqlite, df["email"].tolist())
                        checkpoint[key] = "done"
                        save_checkpoint(checkpoint)
                        report_for_torrent["members_processed"] += 1
                        report_for_torrent["files_processed"] = report_for_torrent.get("files_processed", 0) + 0
                        report_for_torrent["emails_found_per_file"].setdefault(tar_name, 0)
                        report_for_torrent["emails_saved_per_file"].setdefault(tar_name, 0)
                        report_for_torrent["emails_found_per_file"][tar_name] += extracted
                        report_for_torrent["emails_saved_per_file"][tar_name] += uploaded
                        logger.info(f"[green]Member finalizado:[/green] {member.name} extraídos={extracted} inseridos_new={inserted} exported={exported} uploaded={uploaded}")
            except tarfile.ReadError as e:
                logger.exception("ReadError on tar %s: %s", tar_path, e)
                continue
            except Exception as e:
                logger.exception("Error processing tar %s: %s", tar_path, e)
                continue
            report_for_torrent["files_processed"] += 1
        overall_report[torrent_name] = report_for_torrent

    # final summary
    cur = conn_sqlite.cursor()
    cur.execute("SELECT COUNT(*) FROM emails")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM emails WHERE uploaded=1")
    total_up = cur.fetchone()[0]
    console.rule("[bold green]FINAL REPORT[/bold green]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Torrent")
    table.add_column("Files Processed", justify="right")
    table.add_column("Members Processed", justify="right")
    table.add_column("Emails Found", justify="right")
    table.add_column("Emails Uploaded", justify="right")
    for tname, rep in overall_report.items():
        files = rep.get("files_processed", 0)
        members = rep.get("members_processed", 0)
        emails_found = sum(rep.get("emails_found_per_file", {}).values())
        emails_uploaded = sum(rep.get("emails_saved_per_file", {}).values())
        table.add_row(tname, str(files), str(members), f"{emails_found:,}", f"{emails_uploaded:,}")
    console.print(table)
    console.print(f"TOTAL UNIQUE EMAILS (SQLite): {total:,}")
    console.print(f"TOTAL UPLOADED TO HF: {total_up:,}")
    console.print(f"Export files location: {EXPORT_DIR}")
    console.rule("[bold green]FIM[/bold green]")

if __name__ == "__main__":
    main()
