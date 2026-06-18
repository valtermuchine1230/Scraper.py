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
ARQUIVOS_ALVO = [
    "Collection #2_New combo cloud_Trading Collection.tar.gz",
    "Collection #4_BTC combos.tar.gz"
]

CHUNK_SIZE = 16 * 1024 * 1024
TAMANHO_LOTE = 10000

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

def baixar_torrent_otimizado() -> bool:
    logger.info("📡 INICIANDO DOWNLOAD OTIMIZADO...")
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(MAGNET_LINK)
    params.save_path = '.'
    handle = ses.add_torrent(params)
    
    logger.info("⏳ Aguardando metadados...")
    while not handle.status().has_metadata: time.sleep(1)
    
    # Priorizar apenas arquivos alvo
    info = handle.get_torrent_info()
    for i in range(info.num_files()):
        handle.file_priority(i, 7 if any(a in info.files().at(i).path for a in ARQUIVOS_ALVO) else 0)
    
    # Download agressivo
    while not handle.status().is_seeding:
        status = handle.status()
        if status.progress >= 1.0: break
        logger.info(f"📥 {status.progress*100:.2f}% | Vel: {status.download_rate/1024/1024:.2f}MB/s")
        time.sleep(5)
    
    handle.pause()
    logger.info("✅ DOWNLOAD CONCLUÍDO E PAUSADO.")
    return True

def processar_arquivo(conn, filepath: str, cache: Set[str]):
    logger.info(f"⛏️ Processando: {filepath}")
    buffer = []
    with tarfile.open(filepath, "r|gz") as tar:
        for member in tar:
            if member.isfile() and member.name.endswith(('.txt', '.csv')):
                f = tar.extractfile(member)
                for line in f:
                    for email in EMAIL_REGEX.findall(line):
                        email_str = email.decode('utf-8', 'ignore').lower().strip()
                        if email_str not in cache:
                            cache.add(email_str)
                            buffer.append((email_str, "Trader", email_str.split('@')[1], member.name))
                            if len(buffer) >= TAMANHO_LOTE:
                                with conn.cursor() as cur:
                                    cur.executemany("INSERT INTO leads (email, nome, dominio, origem) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", buffer)
                                conn.commit()
                                buffer = []
    if buffer:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO leads (email, nome, dominio, origem) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", buffer)
            conn.commit()

def run():
    if not baixar_torrent_otimizado(): return
    conn = psycopg.connect(DB_URL)
    cache = set()
    for arq in ARQUIVOS_ALVO:
        if os.path.exists(arq): processar_arquivo(conn, arq, cache)
    conn.close()
    logger.info("🚀 TAREFA FINALIZADA.")

if __name__ == "__main__":
    run()
