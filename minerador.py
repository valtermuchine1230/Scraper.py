#!/usr/bin/env python3
"""
Minerador robusto: baixa arquivos selecionados de um magnet/torrent via libtorrent,
faz streaming dos .tar.gz, extrai e-mails por regex e injeta no Neon (Postgres)
usando COPY -> INSERT ... ON CONFLICT DO NOTHING. Checkpoints por member e estatísticas.
Configure DB_URL e MAGNET_LINK via environment variables (secrets no Actions).
"""
import os
import re
import tarfile
import time
import logging
import signal
import sys
import libtorrent as lt
import psycopg
from psycopg.rows import class_row
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# Configurações
DB_URL = os.getenv("DB_URL", "")  # Ex.: postgresql://user:pass@host:port/db
MAGNET_LINK = os.getenv("MAGNET_LINK", "")
# Ajuste a lista de arquivos alvo conforme necessário (strings parciais ou nomes base)
ARQUIVOS_ALVO = [
    "Collection #2_New combo cloud_Trading Collection.tar.gz",
    "Collection #4_BTC combos.tar.gz",
]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "8"))  # segundos entre logs/checagens
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))     # não usado como lista, mas parâmetro de referência
SAVE_PATH = os.getenv("SAVE_PATH", ".")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Regex de e-mail (bytes) - trabalha com linhas em bytes
EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

# Global control
_stop_requested = False

# Logging configurado bonito
logger = logging.getLogger("minerador")
handler = logging.StreamHandler(sys.stdout)
fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
handler.setFormatter(logging.Formatter(fmt))
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)


def add_sslmode_if_needed(dsn: str) -> str:
    """Se DB_URL não tem sslmode, adiciona sslmode=require (muito comum para Neon)."""
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
        logger.debug("Não consegui parsear DSN para adicionar sslmode; usando original.")
    return dsn


def setup_db(conn):
    """Cria tabelas necessárias se não existirem."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            email text PRIMARY KEY,
            nome text,
            dominio text,
            origem text,
            criado_at timestamptz DEFAULT now()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS leads_staging (
            email text,
            nome text,
            dominio text,
            origem text
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processing_checkpoints (
            torrent text,
            tar_name text,
            member_name text,
            status text,
            last_updated timestamptz DEFAULT now(),
            error text,
            PRIMARY KEY (torrent, tar_name, member_name)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processing_stats (
            id bigserial PRIMARY KEY,
            tar_name text,
            member_name text,
            emails_extraidos bigint,
            emails_inseridos bigint,
            inicio timestamptz,
            fim timestamptz
        );
        """)
        conn.commit()
    logger.info("✅ Esquema DB garantido.")


def mark_checkpoint(conn, torrent, tar_name, member_name, status, error=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO processing_checkpoints(torrent, tar_name, member_name, status, last_updated, error)
        VALUES (%s,%s,%s,%s,now(),%s)
        ON CONFLICT (torrent, tar_name, member_name) DO UPDATE
           SET status = EXCLUDED.status, last_updated = now(), error = EXCLUDED.error
        """, (torrent, tar_name, member_name, status, error))
        conn.commit()


def record_stats(conn, tar_name, member_name, extracted, inserted, inicio, fim):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO processing_stats(tar_name, member_name, emails_extraidos, emails_inseridos, inicio, fim)
        VALUES (%s,%s,%s,%s,%s,%s)
        """, (tar_name, member_name, extracted, inserted, inicio, fim))
        conn.commit()


def find_local_target_files(root="."):
    """Procura por arquivos em disco que correspondem a ARQUIVOS_ALVO.
    Matching é case-insensitive e tenta substring no path e equality na basename."""
    matches = []
    lower_targets = [t.lower() for t in ARQUIVOS_ALVO]
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            full_lower = full.lower()
            basename_lower = fn.lower()
            for t in lower_targets:
                if t in full_lower or t == basename_lower:
                    matches.append(full)
                    break
    return sorted(set(matches))


def graceful_shutdown(signum, frame):
    global _stop_requested
    logger.warning("Sinal de parada recebido (%s). Irei encerrar após a iteração corrente.", signum)
    _stop_requested = True


def download_and_wait(magnet: str, save_path: str):
    """Adiciona torrent e espera até que os arquivos alvo comecem a existir no disco.
    Faz logs frequentes de progresso para indicar que está 'vivo'."""
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(magnet)
    params.save_path = save_path
    handle = ses.add_torrent(params)
    logger.info("🔗 Magnet adicionado. Waiting metadata...")
    # Espera metadata
    while not handle.has_metadata():
        s = handle.status()
        logger.info("⏳ Sem metadata ainda - peers: %d, estado: %s", s.num_peers, s.state)
        if _stop_requested:
            raise KeyboardInterrupt()
        time.sleep(POLL_INTERVAL)
    logger.info("📦 Metadata obtido.")
    info = handle.get_torrent_info()
    # Priorizar arquivos alvo
    n_files = info.num_files()
    logger.info("Torrent contém %d arquivos. Aplicando prioridades conforme ARQUIVOS_ALVO...", n_files)
    prioritized_count = 0
    for i in range(n_files):
        path = info.files().at(i).path  # path dentro do torrent
        path_lower = path.lower()
        setprio = 0
        for t in ARQUIVOS_ALVO:
            if t.lower() in path_lower or os.path.basename(path).lower() == t.lower():
                setprio = 7
                prioritized_count += 1
                break
        handle.file_priority(i, setprio)
    logger.info("Priorizei %d arquivos (se houverem correspondências no torrent).", prioritized_count)

    # Espera até os arquivos aparecerem no disco ou até completarem (de forma segura)
    start = time.time()
    logger.info("⌛ Aguardando os arquivos alvo aparecerem em disco (pode requerer alguns minutos)...")
    last_log = 0
    while True:
        if _stop_requested:
            raise KeyboardInterrupt()
        found = find_local_target_files(save_path)
        # Logging de progresso do torrent
        s = handle.status()
        now = time.time()
        if now - last_log > 10:
            logger.info("↓ Download: %.2f%%, peers: %d, download_rate: %d B/s, arquivos_localizados: %d",
                        s.progress * 100, s.num_peers, s.download_rate, len(found))
            last_log = now
        if found:
            logger.info("✅ Encontrados %d arquivo(s) alvo em disco: %s", len(found), ", ".join(found))
            return handle, found
        # se o torrent terminou por completo, tenta novamente um listing
        if s.progress >= 1.0:
            logger.info("Torrent completo (progress 100%%). Verificando arquivos alvo no disco...")
            found = find_local_target_files(save_path)
            if found:
                return handle, found
            else:
                logger.warning("Torrent completo mas nenhum dos ARQUIVOS_ALVO foi encontrado no path. Verifique nomes em ARQUIVOS_ALVO.")
                return handle, []
        # evita loop apertado
        time.sleep(POLL_INTERVAL)


def process_tarfile_member(conn, tar_path, member, torrent_id):
    """Processa um member do tar em streaming, faz COPY para staging e move para leads final."""
    tar_name = os.path.basename(tar_path)
    member_name = member.name
    logger.info("📂 Iniciando processamento do member: %s (dentro de %s)", member_name, tar_name)

    # Verifica checkpoint
    with conn.cursor() as cur:
        cur.execute("""
        SELECT status FROM processing_checkpoints
        WHERE torrent=%s AND tar_name=%s AND member_name=%s
        """, (torrent_id, tar_name, member_name))
        row = cur.fetchone()
        if row and row[0] == 'done':
            logger.info("⏭️  Já processado (checkpoint done): %s", member_name)
            return

    mark_checkpoint(conn, torrent_id, tar_name, member_name, 'processing')

    inicio = datetime.utcnow()
    extracted_count = 0
    inserted_count = 0
    try:
        with conn.cursor() as cur:
            # COPY streaming para leads_staging (persistente)
            copy_sql = "COPY leads_staging(email,nome,dominio,origem) FROM STDIN"
            with cur.copy(copy_sql) as copy:
                # extrai o member em streaming
                f = tar.extractfile(member)
                if f is None:
                    raise RuntimeError("Não consegui extrair member %s" % member_name)
                for raw_line in f:
                    if _stop_requested:
                        raise KeyboardInterrupt()
                    # raw_line é bytes; usa regex em bytes
                    for email_b in EMAIL_REGEX.findall(raw_line):
                        try:
                            email = email_b.decode('utf8', 'ignore').strip().lower()
                        except Exception:
                            email = email_b.decode('latin1', 'ignore').strip().lower()
                        if not email:
                            continue
                        dominio = email.split('@', 1)[1] if '@' in email else 'n/a'
                        copy.write_row((email, "Trader Lead", dominio, member_name))
                        extracted_count += 1
                # copy context fecha aqui
            # Mover para final com deduplicação
            insert_sql = """
            INSERT INTO leads(email,nome,dominio,origem)
            SELECT email,nome,dominio,origem FROM leads_staging
            ON CONFLICT (email) DO NOTHING
            RETURNING email
            """
            cur.execute(insert_sql)
            # rowcount deve indicar quantos foram inseridos
            inserted_count = cur.rowcount if cur.rowcount is not None else 0
            # estatística de quantos extraímos total (antes do TRUNCATE)
            cur.execute("SELECT count(*) FROM leads_staging")
            total_staged = cur.fetchone()[0]
            # limpa staging
            cur.execute("TRUNCATE leads_staging")
            conn.commit()
            fim = datetime.utcnow()
            record_stats(conn, tar_name, member_name, total_staged, inserted_count, inicio, fim)
            mark_checkpoint(conn, torrent_id, tar_name, member_name, 'done')
            logger.info("✅ Finalizado member %s - extraídos=%d, inseridos=%d", member_name, total_staged, inserted_count)
    except KeyboardInterrupt:
        logger.warning("⛔ Interrompido pelo usuário durante member %s. Marcando como failed temporariamente.", member_name)
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error='interrupted')
        raise
    except Exception as e:
        logger.exception("❌ Erro ao processar member %s: %s", member_name, e)
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error=str(e))


def process_files(conn, files_list, torrent_id):
    """Para cada arquivo .tar.gz localizado, itera pelos members e processa os .txt/.csv."""
    for tar_path in files_list:
        logger.info("🗂️  Abrindo tar.gz: %s", tar_path)
        if not os.path.exists(tar_path):
            logger.warning("Arquivo esperado não existe (pulando): %s", tar_path)
            continue
        try:
            # modo streaming - r|gz
            with tarfile.open(tar_path, "r|gz") as t:
                for member in t:
                    if _stop_requested:
                        raise KeyboardInterrupt()
                    if not member.isfile():
                        continue
                    # filtra por extensões relevantes
                    if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                        continue
                    logger.info("  ➤ Arquivo interno: %s", member.name)
                    process_tarfile_member(conn, tar_path, member, torrent_id)
        except tarfile.ReadError as e:
            logger.exception("Erro lendo tar %s: %s", tar_path, e)
        except Exception:
            logger.exception("Erro geral ao processar %s", tar_path)


def main():
    global DB_URL
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    if not DB_URL:
        logger.error("DB_URL não definido. Configure a variável de ambiente DB_URL (Neon DSN).")
        sys.exit(1)
    if not MAGNET_LINK:
        logger.error("MAGNET_LINK não definido. Configure a variável de ambiente MAGNET_LINK.")
        sys.exit(1)

    DB_URL = add_sslmode_if_needed(DB_URL)
    logger.info("Conectando ao banco de dados (Neon)...")
    # psycopg.connect aceita DSN com sslmode
    try:
        conn = psycopg.connect(DB_URL, autocommit=False)
    except Exception as e:
        logger.exception("Falha ao conectar ao DB: %s", e)
        raise

    try:
        setup_db(conn)
        handle, found_files = download_and_wait(MAGNET_LINK, SAVE_PATH)
        if not found_files:
            logger.warning("Nenhum arquivo alvo encontrado; saindo.")
            return
        # Usa o magnet link como id de torrent simples (pode usar info.hash se preferir)
        torrent_id = MAGNET_LINK
        process_files(conn, found_files, torrent_id)
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário. Saindo...")
    except Exception:
        logger.exception("Erro fatal no processo principal.")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        logger.info("Processo finalizado. Conexão DB encerrada.")


if __name__ == "__main__":
    main()
