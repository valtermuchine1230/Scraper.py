#!/usr/bin/env python3
"""
Minerador robusto -> Hugging Face
- Prioriza apenas arquivos alvo listados no metadata do torrent.
- Espera file_progress[file_index] >= expected_size antes de processar.
- Streaming: tar -> linhas -> regex -> batch INSERT OR IGNORE no SQLite.
- SQLite otimizado: WAL, synchronous=NORMAL, temp_store=MEMORY.
- Export Parquet incremental e upload para Hugging Face.
- Checkpoint local (checkpoint.json).
- Limpeza imediata de tar / parquet / exports após upload.
- Logs bonitos com rich, frequência controlada.
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
from pathlib import Path
from datetime import datetime, timezone
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

# ---------- CONFIG (ajuste conforme necessário) ----------
HF_TOKEN = os.getenv("HF_TOKEN")  # obrigatório; configure como secret do Actions
HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "email_miner_dataset")
# SAVE_PATH default; preferir /mnt/downloads se disponível (script detecta)
SAVE_PATH = Path(os.getenv("SAVE_PATH", "/home/runner/work/data"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "6"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_INSERT = int(os.getenv("BATCH_INSERT", "5000"))  # inserir em batches no sqlite
BATCH_EXPORT_ROWS = int(os.getenv("BATCH_EXPORT_ROWS", "200000"))
MIN_FREE_BYTES = int(os.getenv("MIN_FREE_BYTES", 256 * 1024 * 1024))  # 256MB mínimo livre
# MAGNETS: lista de torrents com targets (paths EXATOS conforme metadata)
MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        "targets": [
            "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
            "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz",
        ],
    },
    # adicione mais torrents aqui...
]

# ---------- paths ----------
# cria SAVE_PATH adequado (prefer /mnt if present)
if Path("/mnt").exists():
    # hábito: usar /mnt/data ou /mnt/downloads quando possível
    MNT_BASE = Path("/mnt/data") if Path("/mnt/data").exists() else Path("/mnt")
    SAVE_PATH = MNT_BASE / "minerador"
SAVE_PATH.mkdir(parents=True, exist_ok=True)
CHECKPOINT_PATH = SAVE_PATH / "checkpoint.json"
SQLITE_DB = SAVE_PATH / "emails.db"
EXPORT_DIR = SAVE_PATH / "exports"

# ---------- logging (rich) ----------
console = Console()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger("minerador")
logger.setLevel(LOG_LEVEL)

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
}

# email regex (bytes)
EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# ---------- helpers ----------
def human(n: int) -> str:
    # human readable bytes
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024:
            return f"{n:.2f}{unit}"
        n /= 1024
    return f"{n:.2f}PB"

def disk_usage(path: Path = Path("/")):
    du = shutil.disk_usage(str(path))
    return {"total": du.total, "used": du.used, "free": du.free}

def ensure_min_free_space(path: Path = SAVE_PATH, min_bytes: int = MIN_FREE_BYTES):
    free = disk_usage(path)["free"]
    if free < min_bytes:
        logger.warning(f"{E['warn']} Espaço livre insuficiente em {path}: {human(free)} < {human(min_bytes)}")
        return False
    return True

def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s

def save_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(p: Path):
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

# ---------- sqlite helpers and performance tweaks ----------
def init_sqlite(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA mmap_size = 268435456;")  # 256MB mmap hint
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

def batch_insert_emails(conn, records):
    """
    records: list of tuples (email, nome, origem, data_iso)
    usa executemany INSERT OR IGNORE em batches
    """
    if not records:
        return 0
    cur = conn.cursor()
    try:
        cur.executemany("INSERT OR IGNORE INTO emails(email,nome,origem,data) VALUES (?, ?, ?, ?)", records)
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    except Exception as e:
        conn.rollback()
        logger.exception("SQLite batch insert failed: %s", e)
        return 0

# ---------- HF helpers ----------
def hf_prepare_repo(token: str, dataset_name: str):
    if not token:
        raise RuntimeError("HF_TOKEN is required (set as env secret).")
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{dataset_name}"
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info(f"{E['ok']} Dataset criado: {repo_id}")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"{E['ok']} Dataset já existe: {repo_id}")
        else:
            logger.info(f"{E['warn']} create_repo: {e}")
    return api, repo_id, user

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str):
    try:
        api.upload_file(path_or_fileobj=str(local_path),
                        path_in_repo=repo_path,
                        repo_id=repo_id,
                        repo_type="dataset",
                        token=token)
        logger.info(f"{E['upload']} Uploaded {repo_path}")
        return True
    except Exception:
        logger.exception(f"{E['error']} Upload failed for {local_path}")
        return False

# ---------- name heuristic ----------
def guess_name(email: str) -> str:
    local = email.split("@",1)[0]
    no_digits = re.sub(r"\d+", "", local)
    spaced = re.sub(r"[_.\-]+", " ", no_digits).strip()
    if not spaced:
        return ""
    return " ".join([p.capitalize() for p in spaced.split()])

# ---------- torrent helpers (strict) ----------
def print_metadata_files(info):
    n = info.num_files()
    table = Table(title="Torrent metadata files", show_header=True, header_style="bold magenta")
    table.add_column("idx", style="dim", width=6)
    table.add_column("path", overflow="fold")
    table.add_column("size", justify="right")
    for i in range(n):
        f = info.files().at(i)
        table.add_row(str(i), f.path, f"{f.size:,}")
    console.print(table)

def find_target_indices(torrent_info, targets):
    idx_map = {}
    n = torrent_info.num_files()
    for i in range(n):
        idx_map[i] = torrent_info.files().at(i).path
    found = []
    missing = []
    paths_lower = {i: idx_map[i].lower() for i in idx_map}
    basenames = {i: os.path.basename(idx_map[i]) for i in idx_map}
    basenames_lower = {i: basenames[i].lower() for i in basenames}
    for t in targets:
        matched = False
        # exact path
        for i,p in idx_map.items():
            if p == t:
                found.append(i); matched=True; break
        if matched: continue
        # case-insensitive path
        tl = t.lower()
        for i,pl in paths_lower.items():
            if pl == tl:
                found.append(i); matched=True; break
        if matched: continue
        tb = os.path.basename(t)
        # basename exact
        for i,b in basenames.items():
            if b == tb:
                found.append(i); matched=True; break
        if matched: continue
        # basename ci
        for i,bl in basenames_lower.items():
            if bl == tb.lower():
                found.append(i); matched=True; break
        if not matched:
            missing.append(t)
    found = sorted(set(found))
    return found, missing

def local_path_for_index(save_path: Path, torrent_info, index: int):
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(index).path
    return save_path / torrent_name / file_path

# ---------- processing: streaming, batch insertion, cleanup ----------
_stop = False
def handle_signal(sig, frame):
    global _stop
    logger.warning(f"{E['warn']} Signal {sig} received — will stop after current member.")
    _stop = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def wait_for_file_complete(handle, index, expected_size, poll_interval=POLL_INTERVAL):
    # uses handle.file_progress() to monitor
    last_log = 0
    while True:
        if _stop: raise KeyboardInterrupt()
        fprog = handle.file_progress()
        got = fprog[index] if index < len(fprog) else 0
        pct = (got / expected_size * 100) if expected_size else 0.0
        now = time.time()
        if now - last_log >= 5:
            logger.info(f"{E['download']} Progresso file[{index}] = {got:,}/{expected_size:,} ({pct:.2f}%)")
            last_log = now
        if expected_size and got >= expected_size:
            logger.info(f"{E['ok']} File index {index} com tamanho completo ({got:,} bytes).")
            return True
        time.sleep(poll_interval)

def process_tar_stream_and_upload(conn_sqlite, api, token, repo_id, torrent_info, local_tar_path):
    logger.info(f"{E['extract']} Abrindo {local_tar_path}")
    try:
        with tarfile.open(local_tar_path, "r:*") as t:
            for member in t:
                if _stop: break
                if not member.isfile(): continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")): continue
                logger.info(f"{E['extract']} Member: {member.name}")
                f = t.extractfile(member)
                if f is None:
                    logger.warning(f"{E['warn']} Não extraiu member {member.name}")
                    continue
                # stream, collect batch records
                batch = []
                inserted_total = 0
                extracted_total = 0
                for raw_line in f:
                    for email_b in EMAIL_REGEX.findall(raw_line):
                        try:
                            email = email_b.decode("utf8","ignore").strip().lower()
                        except Exception:
                            email = email_b.decode("latin1","ignore").strip().lower()
                        if not email: continue
                        nome = guess_name(email)
                        data_iso = datetime.now(timezone.utc).isoformat()
                        batch.append((email, nome, member.name, data_iso))
                        extracted_total += 1
                        if len(batch) >= BATCH_INSERT:
                            inserted = batch_insert_emails(conn_sqlite, batch)
                            inserted_total += inserted
                            batch.clear()
                    # periodically check disk space
                    if extracted_total and extracted_total % 50000 == 0:
                        free = disk_usage(SAVE_PATH)["free"]
                        logger.info(f"{E['space']} Espaço livre: {human(free)}")
                        if free < MIN_FREE_BYTES:
                            logger.warning(f"{E['warn']} Espaço crítico: {human(free)} < {human(MIN_FREE_BYTES)}. Parando processamento.")
                            raise RuntimeError("No space left during processing")
                # flush remaining batch
                if batch:
                    inserted = batch_insert_emails(conn_sqlite, batch)
                    inserted_total += inserted
                    batch.clear()
                logger.info(f"{E['email']} Member {member.name}: extraídos={extracted_total:,} inseridos_novos={inserted_total:,}")
                # export & upload batch of new rows
                cur = conn_sqlite.cursor()
                cur.execute("SELECT email,nome,origem,data FROM emails LIMIT ?", (BATCH_EXPORT_ROWS,))
                rows = cur.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=["email","nome","origem","data"])
                    out_dir = EXPORT_DIR / sanitize_filename(torrent_info.name()) / sanitize_filename(local_tar_path.name)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    fname = f"{sanitize_filename(member.name)}_{ts}.parquet"
                    out_path = out_dir / fname
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, str(out_path), compression="snappy")
                    repo_path = f"{sanitize_filename(torrent_info.name())}/{sanitize_filename(local_tar_path.name)}/{fname}"
                    if hf_upload_file(api, token, repo_id, out_path, repo_path):
                        # mark uploaded: use simple approach - delete rows already uploaded to free sqlite (to avoid db growth)
                        # alternative: keep uploaded flag table; here we remove to free space
                        emails_to_remove = df["email"].tolist()
                        cur.executemany("DELETE FROM emails WHERE email=?", [(e,) for e in emails_to_remove])
                        conn_sqlite.commit()
                        logger.info(f"{E['clean']} Uploaded and removed {len(emails_to_remove):,} rows from sqlite to save space.")
                        try:
                            out_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                # after finishing member, collect gc and continue
        # after tar processed, remove tar to free space
        try:
            local_tar_path.unlink(missing_ok=True)
            logger.info(f"{E['clean']} Removido tar: {local_tar_path}")
        except Exception:
            logger.debug("Não foi possível remover tar (maybe in use).")
    except Exception:
        logger.exception(f"{E['error']} Erro ao processar tar {local_tar_path}")

# ---------- pre-run cleanup (safe) ----------
def runner_cleanup():
    logger.info(f"{E['clean']} Cleanup inicial para recuperar espaço (limpeza de caches usuais).")
    # limpar apt cache
    try:
        subprocess.run(["sudo","rm","-rf","/var/lib/apt/lists/*"], check=False)
        logger.info("Limpeza /var/lib/apt/lists")
    except Exception:
        pass
    # limpar runner cache e pip cache (user cache)
    home_cache = Path.home() / ".cache"
    try:
        if home_cache.exists():
            shutil.rmtree(str(home_cache))
            logger.info("Removido ~./cache")
    except Exception:
        pass
    # limpar /tmp (cautela)
    try:
        tmp = Path("/tmp")
        for child in tmp.iterdir():
            try:
                if child.is_file():
                    child.unlink()
                else:
                    shutil.rmtree(child, ignore_errors=True)
            except Exception:
                pass
        logger.info("Limpeza /tmp")
    except Exception:
        pass
    # we DO NOT remove /opt/hostedtoolcache by default (may break runner)
    # if you REALLY want, uncomment below in workflow (use with caution):
    # subprocess.run(["sudo","rm","-rf","/opt/hostedtoolcache"], check=False)

# ---------- main ----------
def main():
    logger.info(f"{E['start']} Minerador iniciando. SAVE_PATH={SAVE_PATH}")
    # pre-run cleanup
    runner_cleanup()
    usage_before = disk_usage(SAVE_PATH)
    logger.info(f"{E['space']} Espaco antes: total={human(usage_before['total'])} free={human(usage_before['free'])}")
    if not ensure_min_free_space(SAVE_PATH):
        logger.warning(f"{E['warn']} Espaço insuficiente para iniciar. Aborting.")
        # still continue but likely to fail
    # HF prepare
    api, repo_id, hf_user = hf_prepare_repo(HF_TOKEN, HF_DATASET_NAME)
    # sqlite init
    global conn_sqlite
    conn_sqlite = init_sqlite(SQLITE_DB)
    checkpoint = load_json(CHECKPOINT_PATH)
    overall_start = time.time()
    # iterate magnets
    for m in MAGNETS:
        if _stop: break
        mag_name = m.get("name")
        magnet_link = m.get("magnet")
        targets = m.get("targets", [])
        logger.info(f"{E['download']} Torrent: {mag_name}")
        session = lt.session({'listen_interfaces':'0.0.0.0:6881'})
        params = lt.parse_magnet_uri(magnet_link)
        params.save_path = str(SAVE_PATH)
        handle = session.add_torrent(params)
        # wait metadata
        while not handle.has_metadata():
            s = handle.status()
            logger.info(f"{E['download']} Waiting metadata peers={s.num_peers} state={s.state}")
            if _stop: break
            time.sleep(POLL_INTERVAL)
        info = handle.get_torrent_info()
        print_metadata_files(info)
        # find indices strictly matching targets (must be exact or basename)
        found_indices, missing = find_target_indices(info, targets)
        if missing:
            logger.error(f"{E['error']} Targets missing in metadata for {mag_name}: {missing}")
            logger.error("Corrija MAGNETS[].targets para corresponder ao metadata; pular este torrent.")
            continue
        logger.info(f"{E['download']} Prioritizing indices: {found_indices}")
        # set priorities
        n = info.num_files()
        for i in range(n):
            prio = 7 if i in found_indices else 0
            handle.file_priority(i, prio)
        # wait each target file to complete via file_progress
        for idx in found_indices:
            if _stop: break
            expected_size = info.files().at(idx).size
            wait_for_file_complete(handle, idx, expected_size)
            local_path = local_path_for_index(SAVE_PATH, info, idx)
            # if not found at expected location, test fallback (direct under save_path)
            if not local_path.exists():
                alt = SAVE_PATH / info.files().at(idx).path
                if alt.exists():
                    local_path = alt
                    logger.info(f"{E['warn']} Usando fallback path {alt}")
                else:
                    logger.error(f"{E['error']} Arquivo não encontrado em disco para idx {idx}; pular.")
                    continue
            if local_path.stat().st_size < expected_size:
                logger.warning(f"{E['warn']} Arquivo incompleto no disco {local_path}; skipping.")
                continue
            # process tar -> stream -> sqlite -> export -> upload -> cleanup
            process_tar_stream_and_upload(conn_sqlite, api, HF_TOKEN, repo_id, info, local_path)
            # checkpoint file processed
            key = f"{mag_name}||{os.path.basename(local_path)}"
            checkpoint[key] = {"index": idx, "path": str(local_path), "processed_at": datetime.now(timezone.utc).isoformat()}
            save_json(CHECKPOINT_PATH, checkpoint)
        # end torrent
    total_time = time.time()-overall_start
    usage_after = disk_usage(SAVE_PATH)
    logger.info(f"{E['stats']} Tempo total: {total_time/60:.2f} min")
    logger.info(f"{E['space']} Espaco depois: free={human(usage_after['free'])}")
    # final stats (sqlite counts)
    try:
        cur = conn_sqlite.cursor()
        cur.execute("SELECT COUNT(*) FROM emails")
        total = cur.fetchone()[0]
        logger.info(f"{E['stats']} Total emails persistidos no sqlite: {total:,}")
    except Exception:
        logger.exception("Erro contando sqlite")
    logger.info(f"{E['ok']} Processo finalizado.")
    # close sqlite
    try:
        conn_sqlite.close()
    except Exception:
        pass

if __name__ == "__main__":
    main()
