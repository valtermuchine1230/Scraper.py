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
ARQUIVOS_ALVO = ["Collection #2_New combo cloud_Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]

CHUNK_SIZE = 16 * 1024 * 1024
TAMANHO_LOTE = 5000
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

def deduzir_nome(email_bytes: bytes) -> str:
    try:
        username = email_bytes.split(b"@")[0].decode('utf-8', 'ignore')
        username = re.sub(r'\d+', '', username)
        username = re.sub(r'[_.\-]+', ' ', username)
        return ' '.join(word.capitalize() for word in username.split()).strip() or "Trader Lead"
    except: return "Trader Lead"

def baixar_torrent_otimizado():
    logger.info("📡 Iniciando download...")
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(MAGNET_LINK)
    params.save_path = '.'
    handle = ses.add_torrent(params)
    while not handle.status().has_metadata: time.sleep(1)
    
    info = handle.get_torrent_info()
    for i in range(info.num_files()):
        handle.file_priority(i, 7 if any(a in info.files().at(i).path for a in ARQUIVOS_ALVO) else 0)
    
    while handle.status().progress < 1.0:
        logger.info(f"📥 Progresso: {handle.status().progress*100:.2f}%")
        time.sleep(10)
    handle.pause()
    logger.info("✅ Download completo e pausado.")

def processar_arquivo(conn, filepath, cache):
    logger.info(f"⛏️ Processando: {filepath}")
    batch = []
    with tarfile.open(filepath, "r|gz") as tar:
        for member in tar:
            if member.isfile() and member.name.endswith(('.txt', '.csv')):
                f = tar.extractfile(member)
                for line in f:
                    for email_b in EMAIL_REGEX.findall(line):
                        email = email_b.decode('utf-8', 'ignore').lower().strip()
                        if email not in cache:
                            cache.add(email)
                            batch.append((email, deduzir_nome(email_b), email.split('@')[1], member.name))
                            if len(batch) >= TAMANHO_LOTE:
                                with conn.cursor() as cur:
                                    cur.executemany("INSERT INTO leads (email, nome, dominio, origem) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", batch)
                                conn.commit()
                                batch = []
    if batch:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO leads (email, nome, dominio, origem) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", batch)
        conn.commit()

if __name__ == "__main__":
    baixar_torrent_otimizado()
    conn = psycopg.connect(DB_URL)
    cache = set()
    for arq in ARQUIVOS_ALVO:
        if os.path.exists(arq): processar_arquivo(conn, arq, cache)
    conn.close()
    logger.info("🚀 TAREFA CONCLUÍDA.")
