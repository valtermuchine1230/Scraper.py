import re
import tarfile
import os
import sys
import time
import logging
from typing import Set, List, Tuple
import libtorrent as lt
import psycopg

# ============ CONFIGURAÇÃO ============
DB_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]

CHUNK_SIZE = 16 * 1024 * 1024
TAMANHO_LOTE = 10000
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(
    rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
    re.IGNORECASE
)

# ============ FUNÇÕES ============

def validar_magnet(magnet: str) -> bool:
    if not magnet.startswith("magnet:?"):
        logger.error("❌ Magnet link inválido")
        return False
    if "xt=urn:btih:" not in magnet:
        logger.error("❌ Sem xt=urn:btih:")
        return False
    return True

def deduzir_nome(email_bytes: bytes) -> str:
    try:
        username = email_bytes.split(b"@")[0].decode('utf-8', 'ignore')
        username = re.sub(r'\d+', '', username)
        username = re.sub(r'[_.-]+', ' ', username).strip()
        return username.title() or "Trader Lead"
    except:
        return "Trader Lead"

def validar_email(email: str) -> bool:
    return (len(email) <= 254 and '@' in email and email.count('@') == 1 and '.' in email.split('@')[1])

def baixar_torrent_seletivo() -> bool:
    logger.info("📡 DOWNLOAD SELETIVO (APENAS ARQUIVOS ALVO)...")
    if not validar_magnet(MAGNET_LINK): return False
    try:
        ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
        params = lt.parse_magnet_uri(MAGNET_LINK)
        params.save_path = '.'
        handle = ses.add_torrent(params)
        
        logger.info("⏳ Aguardando metadados...")
        while not handle.status().has_metadata: time.sleep(1)
        
        info = handle.get_torrent_info()
        tamanho_total = 0
        
        for i in range(info.num_files()):
            arquivo = info.file_path(i)
            if any(alvo in arquivo for alvo in ARQUIVOS_ALVO):
                handle.file_priority(i, 7)
                tamanho_total += info.file_size(i)
                logger.info(f"  ✅ {arquivo} - MARCADO PARA DOWNLOAD")
            else:
                handle.file_priority(i, 0)
        
        logger.info(f"📊 TAMANHO A BAIXAR: {tamanho_total/1024/1024:.2f}MB")
        while not handle.status().is_seeding:
            status = handle.status()
            print(f"📥 {status.progress*100:.1f}% | Vel: {status.download_rate/1024/1024:.2f}MB/s", end="\r")
            time.sleep(5)
        return True
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        return False

def conectar_db():
    try:
        return psycopg.connect(DB_URL, timeout=10)
    except:
        return None

def inserir_lote_otimizado(conn, buffer_lote: List[Tuple]):
    try:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO leads (email, nome, dominio, origem) VALUES (%s, %s, %s, %s) ON CONFLICT (email) DO NOTHING", buffer_lote)
        conn.commit()
    except:
        conn.rollback()

def processar_arquivo_otimizado(conn, filepath: str, cache_local: Set[str]) -> Tuple[int, int]:
    emails_novos, emails_duplicados = 0, 0
    buffer_lote = []
    try:
        with tarfile.open(filepath, "r|gz") as tar:
            for member in tar:
                if not member.isfile() or not member.name.endswith(('.txt', '.csv')): continue
                f = tar.extractfile(member)
                for line in f:
                    emails = EMAIL_REGEX.findall(line)
                    for email_bytes in emails:
                        email = email_bytes.decode('utf-8', 'ignore').lower().strip()
                        if validar_email(email) and email not in cache_local:
                            cache_local.add(email)
                            buffer_lote.append((email, deduzir_nome(email_bytes), email.split('@')[1], member.name))
                            emails_novos += 1
                            if len(buffer_lote) >= TAMANHO_LOTE:
                                inserir_lote_otimizado(conn, buffer_lote)
                                buffer_lote = []
        if buffer_lote: inserir_lote_otimizado(conn, buffer_lote)
        return emails_novos, emails_duplicados
    except:
        return 0, 0

def processar():
    if not baixar_torrent_seletivo(): sys.exit(1)
    conn = conectar_db()
    if not conn: sys.exit(1)
    cache = set()
    for arquivo in ARQUIVOS_ALVO:
        if os.path.exists(arquivo):
            processar_arquivo_otimizado(conn, arquivo, cache)
    conn.close()

if __name__ == "__main__":
    processar()
