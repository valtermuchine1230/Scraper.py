#!/usr/bin/env python3
"""
Minerador robusto (versão corrigida):

- Espera o arquivo .tar.gz alvo estar totalmente baixado (size == expected size no torrent).
- Abre o tar em modo aleatório ("r:*") para processar members robustamente.
- Streaming para COPY (psycopg.copy) sem acumular em RAM.
- Checkpoints por member, stats por member, logs detalhados.
- Relatório final com totais por tar e geral.
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

# ---------- Configurações ----------
DB_URL = "postgresql://neondb_owner:npg_cumTqS9n5ABR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVOS_ALVO = [
    "Collection #2_New combo cloud_Trading Collection.tar.gz",
    "Collection #4_BTC combos.tar.gz",
]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "8"))
SAVE_PATH = os.getenv("SAVE_PATH", ".")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
BATCH_REF = int(os.getenv("BATCH_REF", "2000"))  # referência informativa

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)
_stop_requested = False

# ---------- Logging ----------
logger = logging.getLogger("minerador")
handler = logging.StreamHandler(sys.stdout)
fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
handler.setFormatter(logging.Formatter(fmt))
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)

# ---------- Helpers DB ----------
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
    """Tenta criar esquema; se sem permissões, o chamador decide."""
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
    logger.info("✅ Esquema DB garantido (se permissão disponível).")

def safe_exec_with_rollback(conn, fn, *args, **kwargs):
    """Executa fn(cur, *args) com rollback automático se transação abortar."""
    try:
        with conn.cursor() as cur:
            return fn(cur, *args, **kwargs)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        with conn.cursor() as cur:
            return fn(cur, *args, **kwargs)

def mark_checkpoint(conn, torrent, tar_name, member_name, status, error=None):
    def _fn(cur, torrent, tar_name, member_name, status, error):
        cur.execute("""
        INSERT INTO processing_checkpoints(torrent, tar_name, member_name, status, last_updated, error)
        VALUES (%s,%s,%s,%s,now(),%s)
        ON CONFLICT (torrent, tar_name, member_name) DO UPDATE
           SET status = EXCLUDED.status, last_updated = now(), error = EXCLUDED.error
        """, (torrent, tar_name, member_name, status, error))
        cur.connection.commit()
    safe_exec_with_rollback(conn, _fn, torrent, tar_name, member_name, status, error)

def record_stats(conn, tar_name, member_name, extracted, inserted, inicio, fim):
    def _fn(cur, tar_name, member_name, extracted, inserted, inicio, fim):
        cur.execute("""
        INSERT INTO processing_stats(tar_name, member_name, emails_extraidos, emails_inseridos, inicio, fim)
        VALUES (%s,%s,%s,%s,%s,%s)
        """, (tar_name, member_name, extracted, inserted, inicio, fim))
        cur.connection.commit()
    safe_exec_with_rollback(conn, _fn, tar_name, member_name, extracted, inserted, inicio, fim)

# ---------- Torrent / arquivos ----------
def find_local_target_files(root="."):
    matches = []
    lower_targets = [t.lower() for t in ARQUIVOS_ALVO]
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            f_lower = full.lower()
            base_lower = fn.lower()
            for t in lower_targets:
                if t in f_lower or t == base_lower:
                    matches.append(full)
                    break
    return sorted(set(matches))

def build_torrent_file_size_map(torrent_info):
    """Constrói mapa path -> size do metadata do torrent."""
    mapping = {}
    n = torrent_info.num_files()
    for i in range(n):
        p = torrent_info.files().at(i).path  # path dentro do torrent
        s = torrent_info.files().at(i).size
        mapping[p] = s
        mapping[os.path.basename(p)] = s  # ajudar matching por basename
    return mapping

def expected_size_for_local_path(local_path, file_size_map):
    local_lower = local_path.replace("\\", "/").lower()
    # tenta match por sufixo do path no metadata (prefer preferências por caminhos longos)
    candidates = []
    for meta_path, size in file_size_map.items():
        meta_norm = meta_path.replace("\\", "/").lower()
        if local_lower.endswith(meta_norm):
            candidates.append((len(meta_norm), size))
    if candidates:
        # pega o melhor (maior comprimento de correspondência)
        candidates.sort(reverse=True)
        return candidates[0][1]
    # fallback por basename
    base = os.path.basename(local_path).lower()
    return file_size_map.get(base)

def graceful_shutdown(signum, frame):
    global _stop_requested
    logger.warning("Sinal de parada recebido (%s). Irei encerrar após a iteração corrente.", signum)
    _stop_requested = True

# ---------- Download / verificação ----------
def download_and_wait(magnet: str, save_path: str):
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(magnet)
    params.save_path = save_path
    handle = ses.add_torrent(params)
    logger.info("🔗 Magnet adicionado. Aguardando metadata...")
    # espera metadata
    while not handle.has_metadata():
        s = handle.status()
        logger.info("⏳ Sem metadata - peers:%d estado:%s", s.num_peers, s.state)
        if _stop_requested:
            raise KeyboardInterrupt()
        time.sleep(POLL_INTERVAL)
    logger.info("📦 Metadata obtido.")
    info = handle.get_torrent_info()
    file_size_map = build_torrent_file_size_map(info)
    # priorizar arquivos alvo
    n_files = info.num_files()
    logger.info("Torrent contém %d arquivos. Aplicando prioridades...", n_files)
    prioritized = 0
    for i in range(n_files):
        path = info.files().at(i).path
        setprio = 0
        for t in ARQUIVOS_ALVO:
            if t.lower() in path.lower() or os.path.basename(path).lower() == t.lower():
                setprio = 7
                prioritized += 1
                break
        handle.file_priority(i, setprio)
    logger.info("Priorizei %d arquivos.", prioritized)

    # esperar até os arquivos alvo aparecerem e COMPLETOS (size local >= expected size)
    logger.info("⌛ Aguardando arquivos alvo aparecerem e serem totalmente baixados...")
    last_log = 0
    while True:
        if _stop_requested:
            raise KeyboardInterrupt()
        found = find_local_target_files(save_path)
        s = handle.status()
        now_ts = time.time()
        if now_ts - last_log > 10:
            logger.info("↓ Download global: %.2f%% peers:%d rate:%d B/s arquivos_localizados:%d",
                        s.progress * 100, s.num_peers, s.download_rate, len(found))
            last_log = now_ts
        if found:
            # Verifica se cada encontrado está completo conforme metadata
            incomplete = []
            for f in found:
                exp = expected_size_for_local_path(f, file_size_map)
                if exp is None:
                    # se não sabemos o tamanho esperado, consideramos presente, mas logamos
                    logger.warning("Tamanho esperado ausente no metadata para %s; prosseguiremos com prudência.", f)
                    continue
                try:
                    actual = os.path.getsize(f)
                except OSError:
                    actual = 0
                pct = actual / exp * 100 if exp > 0 else 100.0
                logger.info("Arquivo detectado: %s (%.1f%% %d/%d bytes)", f, pct, actual, exp)
                if actual < exp:
                    incomplete.append((f, actual, exp))
            if not incomplete:
                logger.info("✅ Todos os arquivos alvo encontrados e completos: %s", ", ".join(found))
                return handle, found, file_size_map
            else:
                for f, a, e in incomplete:
                    logger.info("⏳ Arquivo incompleto: %s (%.1f%%). Aguardando...", f, a / e * 100 if e else 0)
        # se torrent completo e ainda não encontramos/confirmamos, podemos sair ou tentar mais logs
        if s.progress >= 1.0:
            logger.info("Torrent completo (100%%). Verificando arquivos no disco...")
            found = find_local_target_files(save_path)
            if found:
                # re-executa a verificação acima na próxima iteração
                time.sleep(POLL_INTERVAL)
                continue
            else:
                logger.warning("Torrent completo mas nenhum ARQUIVOS_ALVO encontrado no path.")
                return handle, [], file_size_map
        time.sleep(POLL_INTERVAL)

# ---------- Processamento ----------
def process_tarfile_member(conn, tar_path, member, torrent_id, tar_obj):
    tar_name = os.path.basename(tar_path)
    member_name = member.name
    logger.info("📂 Iniciando member: %s (dentro %s)", member_name, tar_name)

    # verificar checkpoint
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT status FROM processing_checkpoints
            WHERE torrent=%s AND tar_name=%s AND member_name=%s
            """, (torrent_id, tar_name, member_name))
            r = cur.fetchone()
            if r and r[0] == 'done':
                logger.info("⏭️  Já processado: %s", member_name)
                return 0, 0
    except Exception:
        logger.debug("Erro verificando checkpoint; rollback e seguir.")
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("""
                SELECT status FROM processing_checkpoints
                WHERE torrent=%s AND tar_name=%s AND member_name=%s
                """, (torrent_id, tar_name, member_name))
                r = cur.fetchone()
                if r and r[0] == 'done':
                    logger.info("⏭️  Já processado (após rollback): %s", member_name)
                    return 0, 0
        except Exception:
            logger.debug("Ainda não foi possível verificar checkpoint; tentando processar.")

    mark_checkpoint(conn, torrent_id, tar_name, member_name, 'processing')
    inicio = datetime.now(timezone.utc)
    extracted = 0
    inserted = 0
    try:
        with conn.cursor() as cur:
            with cur.copy("COPY leads_staging(email,nome,dominio,origem) FROM STDIN") as copy:
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
                        extracted += 1
            # move para leads com deduplicação
            cur.execute("""
            INSERT INTO leads(email,nome,dominio,origem)
            SELECT email,nome,dominio,origem FROM leads_staging
            ON CONFLICT (email) DO NOTHING
            """)
            try:
                inserted = cur.rowcount if cur.rowcount is not None else 0
            except Exception:
                inserted = 0
            # total staged
            cur.execute("SELECT count(*) FROM leads_staging")
            total_staged = cur.fetchone()[0]
            cur.execute("TRUNCATE leads_staging")
            conn.commit()
            fim = datetime.now(timezone.utc)
            record_stats(conn, tar_name, member_name, total_staged, inserted, inicio, fim)
            mark_checkpoint(conn, torrent_id, tar_name, member_name, 'done')
            logger.info("✅ Done member %s — extraídos=%d inseridos=%d", member_name, total_staged, inserted)
            return total_staged, inserted
    except KeyboardInterrupt:
        logger.warning("Interrompido pelo usuário durante member %s", member_name)
        try:
            conn.rollback()
        except Exception:
            pass
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error='interrupted')
        raise
    except tarfile.ReadError as e:
        logger.exception("Erro de leitura (tar): %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error=str(e))
        record_stats(conn, tar_name, member_name, extracted, 0, inicio, datetime.now(timezone.utc))
        return extracted, 0
    except Exception as e:
        logger.exception("Erro processando member %s: %s", member_name, e)
        try:
            conn.rollback()
        except Exception:
            pass
        mark_checkpoint(conn, torrent_id, tar_name, member_name, 'failed', error=str(e))
        record_stats(conn, tar_name, member_name, extracted, 0, inicio, datetime.now(timezone.utc))
        return extracted, 0

def process_files(conn, files_list, torrent_id, file_size_map):
    totals = {
        "per_tar": {},
        "overall_extracted": 0,
        "overall_inserted": 0
    }
    for tar_path in files_list:
        tar_name = os.path.basename(tar_path)
        totals["per_tar"].setdefault(tar_name, {"extracted": 0, "inserted": 0})
        logger.info("🗂️  Abrindo tar.gz: %s", tar_path)
        if not os.path.exists(tar_path):
            logger.warning("Arquivo não existe (pular): %s", tar_path)
            continue
        # Verifica tamanho esperado e local antes de abrir
        expected = expected_size_for_local_path(tar_path, file_size_map)
        if expected:
            actual = os.path.getsize(tar_path)
            logger.info("Tamanho local: %d bytes / esperado: %d bytes (%.1f%%)", actual, expected, actual / expected * 100 if expected else 100.0)
            if actual < expected:
                logger.warning("Arquivo ainda não está completamente escrito em disco: %s (%.1f%%). Pulando por segurança.", tar_path, actual / expected * 100)
                continue
        else:
            logger.warning("Tamanho esperado não encontrado no metadata para %s; prosseguindo com cuidado.", tar_path)
        # Abrir em modo random-access (mais robusto) 'r:*' detecta compressão
        try:
            with tarfile.open(tar_path, "r:*") as t:
                for member in t:
                    if _stop_requested:
                        raise KeyboardInterrupt()
                    if not member.isfile():
                        continue
                    if not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                        continue
                    logger.info("  ➤ Member: %s", member.name)
                    extracted, inserted = process_tarfile_member(conn, tar_path, member, torrent_id, t)
                    totals["per_tar"][tar_name]["extracted"] += extracted
                    totals["per_tar"][tar_name]["inserted"] += inserted
                    totals["overall_extracted"] += extracted
                    totals["overall_inserted"] += inserted
        except tarfile.ReadError as e:
            logger.exception("Erro lendo tar %s: %s. Marcando tar como problemático e seguindo.", tar_path, e)
            continue
        except KeyboardInterrupt:
            logger.warning("Interrompido pelo usuário durante processamento de %s", tar_path)
            raise
        except Exception as e:
            logger.exception("Erro geral ao processar %s: %s", tar_path, e)
            continue
    # relatório final parcial
    logger.info("----- Relatório Parcial -----")
    for tar_name, vals in totals["per_tar"].items():
        logger.info("Tar: %s — extraídos=%d inseridos=%d", tar_name, vals["extracted"], vals["inserted"])
    logger.info("Totais até agora — extraídos: %d inseridos: %d", totals["overall_extracted"], totals["overall_inserted"])
    return totals

# ---------- main ----------
def main():
    global DB_URL
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    if not DB_URL:
        logger.error("DB_URL não definido. Configure DB_URL (Neon DSN).")
        sys.exit(1)
    if not MAGNET_LINK:
        logger.error("MAGNET_LINK não definido. Configure MAGNET_LINK.")
        sys.exit(1)

    DB_URL = add_sslmode_if_needed(DB_URL)
    logger.info("Conectando ao Neon/Postgres...")
    try:
        conn = psycopg.connect(DB_URL, autocommit=False)
    except Exception as e:
        logger.exception("Falha conexão DB: %s", e)
        raise

    try:
        try:
            setup_db(conn)
        except Exception:
            logger.warning("setup_db falhou (provavelmente permissões). Continuarei assumindo que as tabelas existem.")
            try:
                conn.rollback()
            except Exception:
                pass

        handle, found_files, file_size_map = download_and_wait(MAGNET_LINK, SAVE_PATH)
        if not found_files:
            logger.warning("Nenhum arquivo alvo encontrado/completo; encerrando.")
            return
        torrent_id = MAGNET_LINK  # pode ser substituído por info.hash
        totals = process_files(conn, found_files, torrent_id, file_size_map)
        # relatório final
        logger.info("===== Relatório Final =====")
        for tar_name, vals in totals["per_tar"].items():
            logger.info("Tar: %s — extraídos=%d inseridos=%d", tar_name, vals["extracted"], vals["inserted"])
        logger.info("Totais finais — extraídos: %d | inseridos: %d", totals["overall_extracted"], totals["overall_inserted"])
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário.")
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
