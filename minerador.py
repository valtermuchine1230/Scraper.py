#!/usr/bin/env python3
"""
Minerador -> Hugging Face Datasets (Parquet)

- Processa múltiplos magnets (configuráveis em MAGNETS).
- Prioriza arquivos alvo dentro do torrent.
- Espera arquivo .tar.gz estar completo antes de processar.
- Processa members (.txt/.csv) em streaming, extrai e-mails por regex.
- Deduplicação persistente via SQLite (emails.db).
- Enriquecimento: guess_name(email) conforme regras especificadas.
- Export incremental para Parquet por member e upload incremental para HF dataset privado.
- Checkpoint local (checkpoint.json).
- Logs bonitos com rich.
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
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# networking / bittorrent
import libtorrent as lt

# HF
from huggingface_hub import HfApi, hf_hub_url, whoami
# data export
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# logging - rich
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table

# -------- CONFIG --------
# Se quiser que o token venha do env, set HF_TOKEN no Actions; caso contrário o script usa HF_TOKEN_DEFAULT.
HF_TOKEN_DEFAULT = "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"  # token que você forneceu
HF_TOKEN = os.getenv("HF_TOKEN", HF_TOKEN_DEFAULT)

# Dataset name (no Hub será: <username>/<HF_DATASET_NAME>)
HF_DATASET_NAME = os.getenv("HF_DATASET_NAME", "email_miner_dataset")

# Save path (onde o torrent salvará os arquivos e onde exportaremos parquet)
SAVE_PATH = Path(os.getenv("SAVE_PATH", "."))

# Checkpoint file
CHECKPOINT_PATH = SAVE_PATH / "checkpoint.json"

# SQLite DB path
SQLITE_DB = SAVE_PATH / "emails.db"

# Export dir
EXPORT_DIR = SAVE_PATH / "exports"

# Polling intervals and retries
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "8"))
WAIT_RETRIES = int(os.getenv("WAIT_RETRIES", "6"))  # re-tentativas antes de pular arquivo incompleto (multiples of POLL_INTERVAL)

# Logs
console = Console()
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger("minerador")

# Email regex (bytes-safe)
EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# MAGNETS configuration (adapte/estenda conforme necessário)
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
        "magnet": "magnet:?xt=urn:btih:B39C603C7E18DB8262067C5926E7D5EA5D20E12E&dn=Collection%201&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2f%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce",
        "targets": [
            "Collection #1_BTC combos.tar.gz",
            "Collection #1_OLD CLOUD_Trading combos.tar.gz",
            "Collection #1_OLD CLOUD_BTC combos.tar.gz",
        ],
    },
]

# ---------- Utilities ----------
def add_sslmode_if_needed(dsn: str) -> str:
    """Se a variável HF/DB tivesse query params, ajuste. (Mantido para compatibilidade se precisar)."""
    if not dsn:
        return dsn
    try:
        parsed = urlparse(dsn)
        qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "sslmode" not in qs:
            qs["sslmode"] = "require"
            new_query = urlencode(qs)
            new = parsed._replace(query=new_query)
            return urlunparse(new)
    except Exception:
        pass
    return dsn

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---------- Checkpoint handling ----------
def load_checkpoint():
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.warning("Não consegui carregar checkpoint.json; re-criando.")
    return {}

def save_checkpoint(data):
    safe_mkdir(CHECKPOINT_PATH.parent)
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
    """Insere com INSERT OR IGNORE; retorna True se inseriu, False se já existia."""
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR IGNORE INTO emails(email,nome,origem,data,uploaded) VALUES (?, ?, ?, ?, 0)",
            (email, nome, origem, data_iso),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        logger.exception("Erro SQLite ao inserir email %s: %s", email, e)
        return False

def mark_uploaded_for_rows(conn, emails_list):
    if not emails_list:
        return
    cur = conn.cursor()
    try:
        cur.executemany("UPDATE emails SET uploaded=1 WHERE email=?", [(e,) for e in emails_list])
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Erro marcando uploaded em SQLite.")

# ---------- Name guessing ----------
import re as _re
def guess_name(email: str) -> str:
    """
    Heurística conforme especificada:
    1) take part before @
    2) remove digits
    3) replace [_.-]+ with space
    4) title-case each word
    """
    local = email.split("@", 1)[0]
    # remove digits
    no_digits = _re.sub(r"\d+", "", local)
    # replace separators with space
    spaced = _re.sub(r"[_.\-]+", " ", no_digits)
    spaced = spaced.strip()
    if not spaced:
        return ""
    # capitalize
    parts = [p.capitalize() for p in spaced.split()]
    return " ".join(parts)

# ---------- Hugging Face Hub helpers ----------
def hf_api_login_and_prepare_repo(token: str, dataset_name: str):
    api = HfApi()
    who = api.whoami(token=token)
    user = who.get("name") or who.get("user") or who.get("id")  # fallbacks
    repo_id = f"{user}/{dataset_name}"
    # create repo if not exists (private dataset)
    try:
        api.create_repo(repo_id=repo_id, token=token, repo_type="dataset", private=True)
        logger.info(f"✅ Criado dataset privado no Hub: {repo_id}")
    except Exception as e:
        # se já existir, ok. outros erros serão logados.
        if "already exists" in str(e).lower():
            logger.info(f"Dataset já existe: {repo_id}")
        else:
            logger.info(f"Dataset check/create: {repo_id} (possível já existente). Mensagem: {e}")
    return api, repo_id, user

def hf_upload_file(api: HfApi, token: str, repo_id: str, local_path: Path, repo_path: str, commit_message: str = None):
    commit_message = commit_message or f"Upload {repo_path}"
    logger.debug(f"Subindo {local_path} -> {repo_id}/{repo_path}")
    try:
        api.upload_file(
            path_or_fileobj=str(local_path),
            path_in_repo=repo_path,
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
            commit_message=commit_message,
        )
        logger.info(f"[green]Uploaded[/green] {repo_path}")
        return True
    except Exception as e:
        logger.exception("Erro ao subir arquivo ao HF Hub: %s", e)
        return False

# ---------- Torrent helpers ----------
def find_local_target_files(root: Path, targets):
    matches = []
    lower_targets = [t.lower() for t in targets]
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            fl = full.lower()
            base = fn.lower()
            for t in lower_targets:
                if t in fl or t == base:
                    matches.append(Path(full))
                    break
    return sorted(set(matches))

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

# ---------- Core processing ----------
def process_member_stream_and_store(conn_sqlite, tar_obj, member, origin_member_name, dataset_upload_context):
    """
    Reads member stream, extract emails, inserts into sqlite. Returns (extracted_count, inserted_count)
    dataset_upload_context dict has keys: api, token, repo_id, hf_user, dataset_name
    """
    extracted = 0
    inserted = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        f = tar_obj.extractfile(member)
        if f is None:
            raise RuntimeError("extractfile returned None")
        # iterate lines as bytes
        for raw_line in f:
            for email_b in EMAIL_REGEX.findall(raw_line):
                try:
                    email = email_b.decode("utf8", "ignore").strip().lower()
                except Exception:
                    email = email_b.decode("latin1", "ignore").strip().lower()
                if not email:
                    continue
                nome = guess_name(email)
                # insert into sqlite; insert_email_sqlite handles duplicates
                inserted_flag = insert_email_sqlite(conn_sqlite, email, nome, origin_member_name, now_iso)
                extracted += 1
                if inserted_flag:
                    inserted += 1
        return extracted, inserted
    except Exception as e:
        logger.exception("Erro lendo member %s: %s", member.name, e)
        return extracted, inserted

def export_new_rows_to_parquet_and_upload(conn_sqlite, api, token, repo_id, hf_user, dataset_name, torrent_name, tar_name, member_name):
    """
    Export rows with uploaded=0 to a parquet file located under EXPORT_DIR/<torrent>/<tar>/ and upload to HF.
    After successful upload, mark uploaded=1 for the emails included.
    Returns (exported_count, uploaded_count)
    """
    cur = conn_sqlite.cursor()
    cur.execute("SELECT email,nome,origem,data FROM emails WHERE uploaded=0")
    rows = cur.fetchmany(200000)  # batch up to 200k rows per export - adjustable
    if not rows:
        return 0, 0
    # build df
    df = pd.DataFrame(rows, columns=["email", "nome", "origem", "data"])
    safe_mkdir(EXPORT_DIR)
    subdir = EXPORT_DIR / sanitize_filename(torrent_name) / sanitize_filename(tar_name)
    safe_mkdir(subdir)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{sanitize_filename(member_name)}_{timestamp}.parquet"
    out_path = subdir / filename
    # write parquet
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, str(out_path), compression="snappy")
    except Exception:
        logger.exception("Erro escrevendo parquet para %s", out_path)
        return 0, 0
    # upload to HF
    repo_path = f"{sanitize_filename(torrent_name)}/{sanitize_filename(tar_name)}/{filename}"
    success = hf_upload_file(api, token, repo_id, out_path, repo_path, commit_message=f"Add {repo_path}")
    if success:
        # mark uploaded rows (based on emails in df)
        emails_list = df["email"].tolist()
        mark_uploaded_for_rows(conn_sqlite, emails_list)
        return len(df), len(df)
    else:
        return len(df), 0

# ---------- Helpers ----------
def sanitize_filename(s: str):
    # make safe filenames for exports and repo paths
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in s)[:200].replace(" ", "_")

def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---------- Signals ----------
_stop_requested = False
def handle_sig(signum, frame):
    global _stop_requested
    logger.warning(f"Signal {signum} received; will stop after current item.")
    _stop_requested = True

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

# ---------- Main orchestration ----------
def main():
    global HF_TOKEN
    HF_TOKEN = os.getenv("HF_TOKEN", HF_TOKEN)  # env overrides default
    if not HF_TOKEN:
        logger.error("HF_TOKEN não configurado. Defina a variável HF_TOKEN ou altere HF_TOKEN_DEFAULT no código.")
        sys.exit(1)

    # Prepare HF API and repo
    api, repo_id, hf_user = hf_api_login_and_prepare_repo(HF_TOKEN, HF_DATASET_NAME)

    # Init sqlite
    conn_sqlite = init_sqlite(SQLITE_DB)

    # Load checkpoint
    checkpoint = load_checkpoint()

    overall_report = {}

    for magnet_item in MAGNETS:
        if _stop_requested:
            break
        torrent_name = magnet_item.get("name")
        magnet_link = magnet_item.get("magnet")
        targets = magnet_item.get("targets", [])
        logger.info(f"[blue]Iniciando torrent:[/blue] {torrent_name}")

        # start libtorrent session and add torrent
        ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
        params = lt.parse_magnet_uri(magnet_link)
        params.save_path = str(SAVE_PATH)
        handle = ses.add_torrent(params)
        logger.info("🔗 Magnet adicionado. Aguardando metadata...")
        # wait metadata
        while not handle.has_metadata():
            s = handle.status()
            logger.info(f"⏳ metadata: peers={s.num_peers} state={s.state}")
            if _stop_requested:
                break
            time.sleep(POLL_INTERVAL)
        if _stop_requested:
            break
        info = handle.get_torrent_info()
        file_size_map = build_torrent_file_size_map(info)

        # priorizar arquivos alvo
        nfiles = info.num_files()
        prioritized = 0
        for i in range(nfiles):
            path = info.files().at(i).path
            for t in targets:
                if t.lower() in path.lower() or os.path.basename(path).lower() == t.lower():
                    handle.file_priority(i, 7)
                    prioritized += 1
                    break
        logger.info(f"Priorizei {prioritized} arquivos no torrent (se existirem matching).")

        # wait until target files exist and are complete
        logger.info("⌛ Aguardando arquivos alvo aparecerem e completarem o download...")
        retries = 0
        found_files = []
        while True:
            if _stop_requested:
                break
            found_files = find_local_target_files(SAVE_PATH, targets)
            s = handle.status()
            logger.info(f"↓ Download global: {s.progress*100:.2f}% peers={s.num_peers} downrate={s.download_rate} B/s found={len(found_files)}")
            if found_files:
                # check completeness
                incomplete = []
                for f in found_files:
                    expected = expected_size_for_local_path(f, file_size_map)
                    actual = f.stat().st_size if f.exists() else 0
                    if expected and actual < expected:
                        incomplete.append((f, actual, expected))
                        logger.info(f"Arquivo {f} incompleto: {actual}/{expected} ({actual/expected*100:.1f}%)")
                    else:
                        logger.info(f"Arquivo completo detectado: {f} ({actual} bytes)")
                if not incomplete:
                    logger.info(f"[green]Arquivos alvo completos encontrados:[/green] {', '.join(str(x) for x in found_files)}")
                    break
                else:
                    # wait and retry
                    retries += 1
                    if retries > WAIT_RETRIES:
                        logger.warning("Excedeu retries de espera por arquivos completos; iremos processar os que estiverem completos e pular os incompletos.")
                        # filter only complete
                        found_files = [f for f in found_files if expected_size_for_local_path(f, file_size_map) is None or f.stat().st_size >= expected_size_for_local_path(f, file_size_map)]
                        break
            if s.progress >= 1.0:
                logger.info("Torrent parece completo (progress 100%). Checando arquivos locais...")
                # allow one extra loop to check
            time.sleep(POLL_INTERVAL)

        # process each found file (tar.gz)
        report_for_torrent = {"files_processed": 0, "members_processed": 0, "emails_found_per_file": {}, "emails_saved_per_file": {}}

        for tar_path in found_files:
            if _stop_requested:
                break
            tar_name = os.path.basename(tar_path)
            logger.info(f"[blue]Abrindo tar:[/blue] {tar_path}")
            if not tar_path.exists():
                logger.warning(f"Arquivo não existe (pular): {tar_path}")
                continue

            # check expected size again and skip if incomplete
            expected = expected_size_for_local_path(tar_path, file_size_map)
            if expected and tar_path.stat().st_size < expected:
                logger.warning(f"Arquivo {tar_path} ainda incompleto (pular).")
                continue

            # open tar in random-access mode 'r:*' for robustness
            try:
                with tarfile.open(tar_path, "r:*") as t:
                    # iterate members
                    for member in t:
                        if _stop_requested:
                            break
                        if not member.isfile():
                            continue
                        if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                            continue
                        # checkpoint key
                        key = f"{torrent_name}||{tar_name}||{member.name}"
                        status = checkpoint.get(key)
                        if status == "done":
                            logger.info(f"⏭️  Já processado: {member.name}")
                            continue
                        logger.info(f"[cyan]Processando member:[/cyan] {member.name}")
                        # mark processing
                        checkpoint[key] = "processing"
                        save_checkpoint(checkpoint)
                        # process stream and insert into sqlite
                        extracted, inserted = process_member_stream_and_store(conn_sqlite, t, member, member.name, None)
                        report_for_torrent["members_processed"] += 1
                        report_for_torrent["files_processed"] = report_for_torrent.get("files_processed", 0) + 0  # per tar counted below
                        # after processing member, attempt export/upload of new rows (batch)
                        exported, uploaded = export_new_rows_to_parquet_and_upload(
                            conn_sqlite, api, HF_TOKEN, repo_id, hf_user, HF_DATASET_NAME, torrent_name, tar_name, member.name
                        )
                        # update report
                        report_for_torrent["emails_found_per_file"].setdefault(tar_name, 0)
                        report_for_torrent["emails_saved_per_file"].setdefault(tar_name, 0)
                        report_for_torrent["emails_found_per_file"][tar_name] += extracted
                        report_for_torrent["emails_saved_per_file"][tar_name] += uploaded
                        # mark done checkpoint
                        checkpoint[key] = "done"
                        save_checkpoint(checkpoint)
                        logger.info(f"[green]Member finalizado:[/green] {member.name} extraídos={extracted} inseridos_local={inserted} exported={exported} uploaded={uploaded}")
                    # after finishing tar
                    report_for_torrent["files_processed"] += 1
            except tarfile.ReadError as e:
                logger.exception("ReadError no tar %s: %s", tar_path, e)
                continue
            except Exception as e:
                logger.exception("Erro ao processar tar %s: %s", tar_path, e)
                continue

        # aggregate overall for this torrent
        overall_report[torrent_name] = report_for_torrent

    # final report: aggregate from sqlite
    cur = conn_sqlite.cursor()
    cur.execute("SELECT COUNT(*) FROM emails")
    total_processed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM emails WHERE uploaded=1")
    total_uploaded = cur.fetchone()[0]

    # print nice report
    console.rule("[bold green]RELATÓRIO FINAL[/bold green]")
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
    console.print(f"TOTAL UNIQUE EMAILS (SQLite): {total_processed:,}")
    console.print(f"TOTAL UPLOADED TO HF: {total_uploaded:,}")
    console.print(f"Export files location: {EXPORT_DIR}")
    console.rule("[bold green]FIM[/bold green]")

    conn_sqlite.close()

if __name__ == "__main__":
    main()
