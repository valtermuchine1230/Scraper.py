#!/usr/bin/env python3
"""
Minerador robusto: baixa arquivos selecionados de um magnet/torrent via libtorrent,
faz streaming dos .tar.gz, extrai e-mails por regex e injeta no Neon (Postgres)
usando COPY -> INSERT ... ON CONFLICT DO NOTHING. Checkpoints por member e estatísticas.
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
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import traceback

# Configurações
DB_URL = "postgresql://neondb_owner:npg_cumTqS9n5ABR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVOS_ALVO = [
    "Collection #2_New combo cloud_Trading Collection.tar.gz",
    "Collection #4_BTC combos.tar.gz",
]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "8"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
SAVE_PATH = os.getenv("SAVE_PATH", ".")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

_stop_requested = False

logger = logging.getLogger("minerador")
handler = logging.StreamHandler(sys.stdout)
fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
handler.setFormatter(logging.Formatter(fmt))
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)


def add_sslmode_if_needed(dsn: str) -> str:
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
        logger.debug("Não consegui parsear DSN; usando original.")
    return dsn


def setup_db(conn):
    """Cria as tabelas se possível. Se o usuário não tiver permissão, lance a exceção para o chamador tratar."""
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
    """Marca checkpoint; usa sua própria transação/commit."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO processing_checkpoints(torrent, tar_name, member_name, status, last_updated, error)
            VALUES (%s,%s,%s,%s,now(),%s)
            ON CONFLICT (torrent, tar_name, member_name) DO UPDATE
               SET status = EXCLUDED.status, last_updated = now(), error = EXCLUDED.error
            """, (torrent, tar_name, member_name, status, error))
            conn.commit()
    except Exception:
        # Se houve um estado de transação abortada no conn, tenta rollback e repetir
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO processing_checkpoints(torrent, tar_name, member_name, status, last_updated, error)
                VALUES (%s,%s,%s,%s,now(),%s)
                ON CONFLICT (torrent, tar_name, member_name) DO UPDATE
                   SET status = EXCLUDED.status, last_updated = now(), error = EXCLUDED.error
                """, (torrent, tar_name, member_name, status, error))
                conn.commit()
        except Exception as e:
            logger.exception("Falha ao marcar checkpoint mesmo após rollback: %s", e)


def record_stats(conn, tar_name, member_name, extracted, inserted, inicio, fim):
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO processing_stats(tar_name, member_name, emails_extraidos, emails_inseridos, inicio, fim)
            VALUES (%s,%s,%s,%s,%s,%s)
            """, (tar_name, member_name, extracted, inserted, inicio, fim))
            conn.commit()
    except Exception:
        logger.exception("Falha ao gravar stats; tentando rollback e regravar.")
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("""
                INSERT INTO processing_stats(tar_name, member_name, emails_extraidos, emails_inseridos, inicio, fim)
                VALUES (%s,%s,%s,%s,%s,%s)
                """, (tar_name, member_name, extracted, inserted, inicio, fim))
                conn.commit()
        except Exception as e:
            logger.exception("Ainda falhou ao gravar stats: %s", e)


def find_local_target_files(root="."):
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
    logger.warning("Sinal de parada recebido (%s). Encerrando após a iteração corrente.", signum)
    _stop_requested = True


def download_and_wait(magnet: str, save_path: str):
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
    n_files = info.num_files()
    logger.info("Torrent contém %d arquivos. Aplicando prioridades conforme ARQUIVOS_ALVO...", n_files)
    prioritized_count = 0
    for i in range(n_files):
        path = info.files().at(i).path
        path_lower = path.lower()
        setprio = 0
        for t in ARQUIVOS_ALVO:
            if t.lower() in path_lower or os.path.basename(path).lower() == t.lower():
                setprio = 7
                prioritized_count += 1
                break
        handle.file_priority(i, setprio)
    logger.info("Priorizei %d arquivos (se houver correspondências no torrent).", prioritized_count)

    # Espera os arquivos alvo aparecerem em disco
    logger.info("⌛ Aguardando os arquivos alvo aparecerem em disco (pode requerer alguns minutos)...")
    last_log = 0
    while True:
        if _stop_requested:
            raise KeyboardInterrupt()
        found = find_local_target_files(save_path)
        s = handle.status()
        now_ts = time.time()
        if now_ts - last_log > 10:
            logger.info("↓ Download: %.2f%%, peers: %d, download_rate: %d B/s, arquivos_localizados: %d",
                        s.progress * 100, s.num_peers, s.download_rate, len(found))
            last_log = now_ts
        if found:
            logger.info("✅ Encontrados %d arquivo(s) alvo em disco: %s", len(found), ", ".join(found))
            return handle, found
        if s.progress >= 1.0:
            logger.info("Torrent completo (100%%). Verificando arquivos alvo no disco...")
            found = find_local_target_files(save_path)
            if found:
                return handle, found
            else:
                logger.warning("Torrent completo mas nenhum dos ARQUIVOS_ALVO foi encontrado no path.")
                return handle, []
        time.sleep(POLL_INTERVAL)


def process_tarfile_member(conn, tar_path, member, torrent_id, tar_obj):
    tar_name = os.path.basename(tar_path)
    member_name = member.name
    logger.info("📂 Iniciando processamento do member: %s (dentro de %s)", member_name, tar_name)

    # Verifica checkpoint
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT status FROM processing_checkpoints
            WHERE torrent=%s AND tar_name=%s AND member_name=%s
            """, (torrent_id, tar_name, member_name))
            row = cur.fetchone()
            if row and row[0] == 'done':
                logger.info("⏭️  Já processado (checkpoint done): %s", member_name)
                return
    except Exception:
        # Se houver problema (ex: transação anterior abortada), tenta rollback e re-tentar a verificação
        logger.debug("Erro ao verificar checkpoint — tentando rollback e re-tentar.")
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("""
                SELECT status FROM processing_checkpoints
                WHERE torrent=%s AND tar_name=%s AND member_name=%s
                """, (torrent_id, tar_name, member_name))
                row = cur.fetchone()
                if row and row[0] == 'done':
                    logger.info("⏭️  Já processado (checkpoint done): %s", member_name)
                    return
        except Exception:
            logger.exception("Não consegui verificar checkpoint após rollback; prosseguindo e tentando processar.")

    mark_checkpoint(conn, torrent_id, tar_name, member_name, 'processing')

    inicio = datetime.now(timezone.utc)
    extracted_count = 0
    inserted_count = 0
    try:
        with conn.cursor() as cur:
            copy_sql = "COPY leads_staging(email,nome,dominio,origem) FROM STDIN"
            with cur.copy(copy_sql) as copy:
                f = tar_obj.extractfile(member)
                if f is None:
                    raise RuntimeError(f"Não consegui extrair member {member_name}")
                for raw_line in f:
                    if _stop_requested:
                        raise KeyboardInterrupt()
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
            # Após fechar o COPY, mover para leads
            insert_sql = """
            INSERT INTO leads(email,nome,dominio,origem)
            SELECT email,nome,dominio,origem FROM leads_staging
            ON CONFLICT (email) DO NOTHING
            """
            cur.execute(insert_sql)
            # número aproximado inserido:
            try:
                inserted_count = cur.rowcount if cur.rowcount is not None else 0
            except Exception:
                inserted_count = 0
            # Conta staging antes do TRUNCATE
            cur.execute("SELECT count(*) FROM leads_staging")
            total_staged = cur.fetchone()[0]
            cur.execute("TRUNCATE leads_staging")
            conn.commit()
            fim = datetime.now(timezone.utc)
            record_stats(conn, tar_name, member_name, total_staged, inserted_count, inicio, fim)
            mark_checkpoint(conn, torrent_id, tar_name, member_name, 'done')
            logger.info("✅ Finalizado member %s - extraídos=%d, inseridos=%d", member_name, total_staged, inserted_count)
    except KeyboardInterrupt:
        logger.warning("⛔ Interrompido pelo usuário durante member %s. Marcando como failed.", member_name)
        try:
            conn.rollback()
        except Exception:
            pass
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error='interrupted')
        raise
    except Exception as e:
        logger.exception("❌ Erro ao processar member %s: %s", member_name, e)
        # Se a transação foi abortada, precisa dar rollback antes de executar outros comandos
        try:
            conn.rollback()
        except Exception:
            logger.debug("Rollback também falhou ou não necessário.")
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error=str(e))
        # registra stats com 0 inseridos/extrados se for apropriado
        fim = datetime.now(timezone.utc)
        try:
            record_stats(conn, tar_name, member_name, extracted_count, 0, inicio, fim)
        except Exception:
            logger.debug("Falha ao gravar stats de erro.")
        # não relançamos — permitimos que o loop continue com o próximo member
        return


def process_files(conn, files_list, torrent_id):
    for tar_path in files_list:
        logger.info("🗂️  Abrindo tar.gz: %s", tar_path)
        if not os.path.exists(tar_path):
            logger.warning("Arquivo esperado não existe (pulando): %s", tar_path)
            continue
        try:
            with tarfile.open(tar_path, "r|gz") as t:
                for member in t:
                    if _stop_requested:
                        raise KeyboardInterrupt()
                    if not member.isfile():
                        continue
                    if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                        continue
                    logger.info("  ➤ Arquivo interno: %s", member.name)
                    process_tarfile_member(conn, tar_path, member, torrent_id, t)
        except KeyboardInterrupt:
            logger.warning("Interrompido pelo usuário durante processamento de %s", tar_path)
            raise
        except Exception as e:
            logger.exception("Erro geral ao processar %s: %s", tar_path, e)
            # continuar com próximos arquivos


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
    try:
        conn = psycopg.connect(DB_URL, autocommit=False)
    except Exception as e:
        logger.exception("Falha ao conectar ao DB: %s", e)
        raise

    try:
        setup_db(conn)
    except Exception as e:
        logger.exception("setup_db falhou (ver permissões). Trace:\n%s", traceback.format_exc())
        # Se preferir, sem permissões, podemos continuar assumindo que as tabelas já existem:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("Continuando sem criar esquema - assuma que tabelas já existem.")

    try:
        handle, found_files = download_and_wait(MAGNET_LINK, SAVE_PATH)
        if not found_files:
            logger.warning("Nenhum arquivo alvo encontrado; saindo.")
            return
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
