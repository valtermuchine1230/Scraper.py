#!/usr/bin/env python3
"""
Minerador — versão segura: prioriza e baixa APENAS arquivos declarados em MAGNETS[].targets
- Usa libtorrent para baixar apenas os arquivos EXATOS (por path ou basename) do metadata.
- Espera file_progress[file_index] >= file_size antes de abrir e processar o tar.
- Processa members (.txt/.csv) em streaming, dedup via SQLite, export Parquet e upload incremental ao HF Hub.
- Checkpoint local (checkpoint.json). Logs com rich.
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

import libtorrent as lt
from huggingface_hub import HfApi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table

# ------------- CONFIG -------------
# HF token: env HF_TOKEN overrides HF_TOKEN_DEFAULT.
HF_TOKEN_DEFAULT = "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"
HF_TOKEN = os.getenv("HF_TOKEN", HF_TOKEN_DEFAULT)
HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "email_miner_dataset")
# onde os arquivos serão gravados pelo libtorrent (save_path)
SAVE_PATH = Path(os.getenv("SAVE_PATH", "."))
CHECKPOINT_PATH = SAVE_PATH / "checkpoint.json"
SQLITE_DB = SAVE_PATH / "emails.db"
EXPORT_DIR = SAVE_PATH / "exports"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "6"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_EXPORT_ROWS = int(os.getenv("BATCH_EXPORT_ROWS", "200000"))

# Configure aqui os magnets e ALVOS exatos por torrent (paths mostrados no metadata)
MAGNETS = [
    {
        "name": "Collection #2-#5",
        "magnet": "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce",
        # IMPORTANTE: prefira inserir os paths exatamente como aparecem no metadata (veja logs "Torrent metadata files")
        "targets": [
            "Collection #2-#5 & Antipublic/Collection #2_New combo cloud_Trading Collection.tar.gz",
            "Collection #2-#5 & Antipublic/Collection #4_BTC combos.tar.gz",
        ],
    },
    {
        "name": "Collection #1",
        "magnet": "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E&dn=Collection%201&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce",
        "targets": [
            "Collection #1/Collection #1_BTC combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection #1/Collection #1_OLD CLOUD_BTC combos.tar.gz",
        ],
    },
]

# ---------- logging ----------
console = Console()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s", handlers=[RichHandler(console=console, rich_tracebacks=True)])
logger = logging.getLogger("minerador")
logger.setLevel(LOG_LEVEL)

# ---------- email regex ----------
EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# ---------- utilities ----------
def normalize_for_compare(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.strip()

def sanitize_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in s)[:200].replace(" ", "_")

def save_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(p: Path):
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# ---------- SQLite dedupe ----------
def init_sqlite(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS emails (
        email TEXT PRIMARY KEY,
        nome TEXT,
        origem TEXT,
        data TEXT,
        uploaded INTEGER DEFAULT 0
    )
    """)
    conn.commit()
    return conn

def insert_email(conn, email, nome, origem, data_iso):
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO emails(email,nome,origem,data,uploaded) VALUES (?, ?, ?, ?, 0)",
                    (email, nome, origem, data_iso))
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        logger.exception("SQLite insert failed for %s", email)
        return False

def mark_uploaded(conn, emails):
    if not emails:
        return
    try:
        cur = conn.cursor()
        cur.executemany("UPDATE emails SET uploaded=1 WHERE email=?", [(e,) for e in emails])
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed to mark uploaded rows")

# ---------- HF helpers ----------
def hf_prepare_repo(token: str, dataset_name: str):
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user") or who.get("id")
    repo_id = f"{user}/{dataset_name}"
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info(f"✅ Dataset criado: {repo_id}")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"Dataset já existe: {repo_id}")
        else:
            logger.info(f"Dataset check/create: {repo_id} (mensagem: {e})")
    return api, repo_id, user

def hf_upload(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str):
    try:
        api.upload_file(path_or_fileobj=str(local_path),
                        path_in_repo=repo_path,
                        repo_id=repo_id,
                        repo_type="dataset",
                        token=token)
        logger.info(f"[green]Uploaded[/green] {repo_path}")
        return True
    except Exception:
        logger.exception("Upload failed for %s", local_path)
        return False

# ---------- name guess ----------
def guess_name(email: str) -> str:
    local = email.split("@", 1)[0]
    no_digits = re.sub(r"\d+", "", local)
    spaced = re.sub(r"[_.\-]+", " ", no_digits).strip()
    if not spaced:
        return ""
    return " ".join([p.capitalize() for p in spaced.split()])

# ---------- torrent helpers (strict matching) ----------
def print_metadata(info):
    n = info.num_files()
    table = Table(title="Torrent metadata files", show_header=True, header_style="bold magenta")
    table.add_column("idx", style="dim", width=6)
    table.add_column("path", overflow="fold")
    table.add_column("size", justify="right")
    for i in range(n):
        f = info.files().at(i)
        table.add_row(str(i), f.path, f"{f.size:,}")
    console.print(table)

def find_target_file_indices(torrent_info, targets):
    """Retorna lista de file indices (inteiros) que correspondem exatamente aos targets.
       Matching: exact path (case-sensitive), exact path case-insensitive, basename equals, basename case-insensitive.
       Se um target não for encontrado, devolve (found_indices, missing_targets) para diagnóstico.
    """
    found = []
    missing = []
    # build maps
    n = torrent_info.num_files()
    idx_to_path = {i: torrent_info.files().at(i).path for i in range(n)}
    # lowercase/basename maps
    path_lower_map = {i: idx_to_path[i].lower() for i in idx_to_path}
    basename_map = {i: os.path.basename(idx_to_path[i]) for i in idx_to_path}
    basename_lower_map = {i: basename_map[i].lower() for i in basename_map}
    for t in targets:
        matched = False
        # try exact path (case-sensitive)
        for i, p in idx_to_path.items():
            if p == t:
                found.append(i)
                matched = True
                break
        if matched:
            continue
        # exact path case-insensitive
        tl = t.lower()
        for i, pl in path_lower_map.items():
            if pl == tl:
                found.append(i)
                matched = True
                break
        if matched:
            continue
        # basename exact
        tb = os.path.basename(t)
        for i, b in basename_map.items():
            if b == tb:
                found.append(i)
                matched = True
                break
        if matched:
            continue
        # basename case-insensitive
        for i, bl in basename_lower_map.items():
            if bl == tb.lower():
                found.append(i)
                matched = True
                break
        if not matched:
            missing.append(t)
    # remove duplicates and sort
    found = sorted(set(found))
    return found, missing

def local_path_for_file(save_path: Path, torrent_info, file_index):
    # libtorrent usually saves under save_path / torrent_name / file_path
    torrent_name = torrent_info.name()
    file_path = torrent_info.files().at(file_index).path
    local = save_path / torrent_name / file_path
    return local

# ---------- core logic ----------
_stop_requested = False
def handle_sig(signum, frame):
    global _stop_requested
    logger.warning("Signal %s received; finishing current work then stopping.", signum)
    _stop_requested = True

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

def wait_for_file_complete(handle, file_index, expected_size, poll_interval=POLL_INTERVAL):
    logger.info(f"Esperando file index={file_index} completar (tamanho esperado {expected_size:,} B)...")
    last_logged = 0
    while True:
        if _stop_requested:
            raise KeyboardInterrupt()
        fprogress = handle.file_progress()
        got = fprogress[file_index] if file_index < len(fprogress) else 0
        pct = (got / expected_size * 100) if expected_size else 0.0
        now = time.time()
        if now - last_logged > 5:
            logger.info(f"Progresso file[{file_index}] = {got:,}/{expected_size:,} ({pct:.2f}%)")
            last_logged = now
        if expected_size and got >= expected_size:
            logger.info(f"[green]File index={file_index} completou ({got:,} bytes)[/green]")
            return True
        # if torrent overall finished but file progress smaller -> may be missing; log and retry few times
        s = handle.status()
        if s.progress >= 1.0:
            logger.info("Torrent progress 100%%; cheque file_progress, se file incompleto algo está errado no peer se está corrompido.")
        time.sleep(poll_interval)

def process_tar_file_and_upload(conn_sqlite, api, token, repo_id, torrent_info, local_tar_path, file_index, member_targets=None):
    # open tar file (random access) — file is already complete
    logger.info(f"Abrindo tar local: {local_tar_path}")
    try:
        with tarfile.open(local_tar_path, "r:*") as t:
            for member in t:
                if _stop_requested:
                    break
                if not member.isfile():
                    continue
                if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                    continue
                # process member
                logger.info(f"Processing member: {member.name}")
                member_obj = t.extractfile(member)
                if member_obj is None:
                    logger.warning(f"Não foi possível extrair member {member.name}, pular.")
                    continue
                extracted = 0
                inserted = 0
                for raw_line in member_obj:
                    for email_b in EMAIL_REGEX.findall(raw_line):
                        try:
                            email = email_b.decode("utf8", "ignore").strip().lower()
                        except Exception:
                            email = email_b.decode("latin1", "ignore").strip().lower()
                        if not email:
                            continue
                        nome = guess_name(email)
                        data_iso = datetime.now(timezone.utc).isoformat()
                        ok = insert_email(conn_sqlite, email, nome, member.name, data_iso)
                        extracted += 1
                        if ok:
                            inserted += 1
                logger.info(f"Member finished: {member.name} extracted={extracted} new_inserted={inserted}")
                # export batch (uploaded=0) up to BATCH_EXPORT_ROWS
                cur = conn_sqlite.cursor()
                cur.execute("SELECT email,nome,origem,data FROM emails WHERE uploaded=0 LIMIT ?", (BATCH_EXPORT_ROWS,))
                rows = cur.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=["email","nome","origem","data"])
                    safe_dir = EXPORT_DIR / sanitize_filename(torrent_info.name()) / sanitize_filename(local_tar_path.name)
                    safe_dir.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    fname = f"{sanitize_filename(member.name)}_{ts}.parquet"
                    out_path = safe_dir / fname
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, str(out_path), compression="snappy")
                    repo_path = f"{sanitize_filename(torrent_info.name())}/{sanitize_filename(local_tar_path.name)}/{fname}"
                    if hf_upload(api, token, repo_id, out_path, repo_path):
                        # mark uploaded
                        mark_uploaded(conn_sqlite, df["email"].tolist())
                        logger.info(f"Exported+uploaded {len(df)} rows for member {member.name}")
    except tarfile.ReadError:
        logger.exception("tarfile.ReadError ao abrir %s", local_tar_path)
    except Exception:
        logger.exception("Erro processando tar %s", local_tar_path)

def main():
    global HF_TOKEN
    HF_TOKEN = os.getenv("HF_TOKEN", HF_TOKEN)
    if not HF_TOKEN:
        logger.error("HF_TOKEN não definido. Defina HF_TOKEN como secret ou altere HF_TOKEN_DEFAULT no código.")
        sys.exit(1)
    api, repo_id, hf_user = hf_prepare_repo(HF_TOKEN, HF_DATASET_NAME)
    conn_sqlite = init_sqlite(SQLITE_DB)
    checkpoint = load_json(CHECKPOINT_PATH)

    for magnet_item in MAGNETS:
        if _stop_requested:
            break
        tname = magnet_item.get("name")
        magnet = magnet_item.get("magnet")
        targets = magnet_item.get("targets", [])
        logger.info(f"[blue]Iniciando torrent:[/blue] {tname}")
        ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
        params = lt.parse_magnet_uri(magnet)
        params.save_path = str(SAVE_PATH)
        handle = ses.add_torrent(params)
        logger.info("Adicionado magnet. Aguardando metadata...")
        while not handle.has_metadata():
            s = handle.status()
            logger.info(f"metadata: peers={s.num_peers} state={s.state}")
            if _stop_requested: break
            time.sleep(POLL_INTERVAL)
        if _stop_requested: break
        info = handle.get_torrent_info()
        print_metadata(info)
        # find indices matching EXACT targets
        found_indices, missing_targets = find_target_file_indices(info, targets)
        if missing_targets:
            logger.error("Os seguintes targets não foram encontrados no metadata do torrent (verifique paths em MAGNETS[].targets):")
            for m in missing_targets:
                logger.error(f" - {m}")
            logger.error("Abortando este torrent. Corrija os targets para corresponder ao metadata (veja a tabela 'Torrent metadata files').")
            continue
        logger.info(f"Found file indices for targets: {found_indices}")
        # set priorities: only the found_indices -> priority 7; others -> 0
        nfiles = info.num_files()
        for i in range(nfiles):
            priority = 7 if i in found_indices else 0
            handle.file_priority(i, priority)
        logger.info("Prioridades aplicadas: somente arquivos alvo serão baixados.")
        # wait each file to complete using file_progress
        for idx in found_indices:
            if _stop_requested:
                break
            file_size = info.files().at(idx).size
            # wait until progress >= size
            wait_for_file_complete(handle, idx, file_size)
            # build local path (save_path / torrent_name / file_path)
            local = local_path_for_file(SAVE_PATH, info, idx)
            logger.info(f"Local file path resolved: {local}")
            # sanity: ensure file exists and size >= expected
            if not local.exists():
                logger.error(f"Arquivo esperado não encontrado em disco: {local}")
                # Try checking alternative path: maybe libtorrent saved without torrent_name folder
                alt = SAVE_PATH / info.files().at(idx).path
                if alt.exists():
                    logger.info(f"Arquivo encontrado em alternativa: {alt}")
                    local = alt
                else:
                    logger.error("Arquivo não encontrado em nenhum local esperado — pular este arquivo.")
                    continue
            actual_size = local.stat().st_size
            if actual_size < file_size:
                logger.warning(f"Arquivo {local} tem tamanho {actual_size:,} < esperado {file_size:,} — pular.")
                continue
            # process tarfile now (safe to open as file fully present)
            process_tar_file_and_upload(conn_sqlite, api, HF_TOKEN, repo_id, info, local)
            # mark checkpoint for file completion
            key = f"{tname}||{os.path.basename(local)}"
            checkpoint[key] = {"file_index": idx, "file_path": str(local), "status": "done", "processed_at": datetime.now(timezone.utc).isoformat()}
            save_json(CHECKPOINT_PATH, checkpoint)
        logger.info(f"Torrent {tname} concluído (alvos processados).")

    # final summary
    cur = conn_sqlite.cursor()
    cur.execute("SELECT COUNT(*) FROM emails")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM emails WHERE uploaded=1")
    total_up = cur.fetchone()[0]
    console = Console()
    console.rule("[bold green]RELATÓRIO FINAL[/bold green]")
    console.print(f"Total de emails únicos (SQLite): {total:,}")
    console.print(f"Total uploaded to HF: {total_up:,}")
    console.rule("[bold green]FIM[/bold green]")

if __name__ == "__main__":
    main()
