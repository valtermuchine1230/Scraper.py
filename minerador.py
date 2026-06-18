import re
import tarfile
import os
import sys
import time
import logging
from typing import Set, List, Tuple
import libtorrent as lt
import psycopg
from psycopg import sql

# ============ CONFIGURAÇÃO ============
DB_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVO_TORRENT = None 
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]

# ============ OTIMIZAÇÕES PARA VELOCIDADE ============
CHUNK_SIZE = 16 * 1024 * 1024  # 16MB
TAMANHO_LOTE = 10000
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Regex melhorado
EMAIL_REGEX = re.compile(
    rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
    re.IGNORECASE
)

# ============ FUNÇÕES ============

def validar_magnet(magnet: str) -> bool:
    if not magnet.startswith("magnet:?"):
        logger.error("❌ Magnet link não começa com 'magnet:?'")
        return False
    if "xt=urn:btih:" not in magnet:
        logger.error("❌ Magnet link não contém 'xt=urn:btih:'")
        return False
    try:
        hash_part = magnet.split("xt=urn:btih:")[1].split("&")[0]
        logger.info(f"✅ Magnet link válido (hash: {hash_part[:16]}...)")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao validar magnet: {e}")
        return False

def deduzir_nome(email_bytes: bytes) -> str:
    try:
        username = email_bytes.split(b"@")[0].decode('utf-8', 'ignore')
        username = re.sub(r'\d+', '', username)
        username = re.sub(r'[_.-]+', ' ', username).strip()
        return username.title() or "Trader Lead"
    except Exception:
        return "Trader Lead"

def validar_email(email: str) -> bool:
    return (len(email) <= 254 and '@' in email and email.count('@') == 1 and '.' in email.split('@')[1])

def baixar_torrent_magnet() -> bool:
    logger.info("📡 INICIANDO DOWNLOAD VIA MAGNET...")
    if not validar_magnet(MAGNET_LINK): return False
    try:
        settings = {'listen_interfaces': '0.0.0.0:6881,0.0.0.0:6889', 'connections_limit': 1000}
        ses = lt.session(settings)
        params = lt.parse_magnet_uri(MAGNET_LINK)
        params.save_path = '.'
        handle = ses.add_torrent(params)
        
        logger.info("⏳ Aguardando metadados...")
        timeout = 0
        while not handle.status().has_metadata and timeout < 300:
            time.sleep(1)
            timeout += 1
            
        if not handle.status().has_metadata: return False
        
        while not handle.status().is_seeding:
            status = handle.status()
            logger.info(f"📥 {status.progress*100:6.2f}% | Vel: {status.download_rate/1024/1024:7.2f}MB/s | Peers: {status.num_peers}")
            time.sleep(10)
        return True
    except Exception as e:
        logger.error(f"❌ Erro no download: {e}")
        return False

def conectar_db(tentativa=0):
    try:
        conn = psycopg.connect(DB_URL, timeout=10)
        logger.info("✅ Conectado ao banco de dados")
        return conn
    except Exception as e:
        if tentativa < RETRY_ATTEMPTS:
            time.sleep(RETRY_DELAY)
            return conectar_db(tentativa + 1)
        return None

def inserir_lote_otimizado(conn, buffer_lote: List[Tuple]) -> bool:
    for tentativa in range(RETRY_ATTEMPTS):
        try:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS leads (id SERIAL PRIMARY KEY, email VARCHAR(254) UNIQUE, nome VARCHAR(255), dominio VARCHAR(255), origem VARCHAR(500), criado_em TIMESTAMP DEFAULT NOW())""")
                cur.executemany("INSERT INTO leads (email, nome, dominio, origem) VALUES (%s, %s, %s, %s) ON CONFLICT (email) DO NOTHING", buffer_lote)
            conn.commit()
            logger.info(f"🚀 {len(buffer_lote)} leads inseridos")
            return True
        except Exception:
            conn.rollback()
            time.sleep(RETRY_DELAY)
    return False

def processar_arquivo_otimizado(conn, filepath: str, cache_local: Set[str]) -> Tuple[int, int]:
    emails_novos = 0
    emails_duplicados = 0
    buffer_lote = []
    buffer_restante = b''
    
    try:
        logger.info(f"⛏️ Processando: {filepath}")
        with tarfile.open(filepath, "r|gz") as tar:
            for member in tar:
                if not member.isfile() or not any(member.name.endswith(ext) for ext in ['.txt', '.csv']): continue
                f = tar.extractfile(member)
                while True:
                    bloco = f.read(CHUNK_SIZE)
                    if not bloco: break
                    dados = buffer_restante + bloco
                    ultimo_newline = dados.rfind(b'\n')
                    processar = dados[:ultimo_newline + 1] if ultimo_newline != -1 else dados
                    buffer_restante = dados[ultimo_newline + 1:] if ultimo_newline != -1 else b''
                    
                    for email_bytes in EMAIL_REGEX.findall(processar):
                        email = email_bytes.decode('utf-8', 'ignore').lower().strip()
                        if not validar_email(email): continue
                        if email not in cache_local:
                            cache_local.add(email)
                            buffer_lote.append((email, deduzir_nome(email_bytes), email.split('@')[1], member.name))
                            emails_novos += 1
                            if len(buffer_lote) >= TAMANHO_LOTE:
                                inserir_lote_otimizado(conn, buffer_lote)
                                buffer_lote = []
                        else:
                            emails_duplicados += 1
        if buffer_lote: inserir_lote_otimizado(conn, buffer_lote)
        return emails_novos, emails_duplicados
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        return 0, 0

def processar():
    if not baixar_torrent_magnet(): sys.exit(1)
    conn = conectar_db()
    if not conn: sys.exit(1)
    
    cache_local = set()
    for arquivo in ARQUIVOS_ALVO:
        if os.path.exists(arquivo):
            processar_arquivo_otimizado(conn, arquivo, cache_local)
    conn.close()

if __name__ == "__main__":
    processar()
