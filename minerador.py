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
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]

CHUNK_SIZE = 8 * 1024 * 1024 # 8MB
TAMANHO_LOTE = 5000
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Regex melhorado para emails
EMAIL_REGEX = re.compile(
    rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
    re.IGNORECASE
)

# ============ FUNÇÕES ============

def deduzir_nome(email_bytes: bytes) -> str:
    """Extrai nome do email"""
    try:
        username = email_bytes.split(b"@")[0].decode('utf-8', 'ignore')
        username = re.sub(r'\d+', '', username)
        username = re.sub(r'[_.-]+', ' ', username).strip()
        return username.title() or "Trader Lead"
    except Exception as e:
        logger.warning(f"Erro ao deduzir nome: {e}")
        return "Trader Lead"

def validar_email(email: str) -> bool:
    """Valida formato básico de email"""
    return (
        len(email) <= 254 and
        '@' in email and
        email.count('@') == 1 and
        '.' in email.split('@')[1]
    )

def baixar_torrent() -> bool:
    """Download do torrent com feedback"""
    logger.info("📡 Iniciando download do torrent...")
    try:
        ses = lt.session({
            'listen_interfaces': '0.0.0.0:6881',
            'connections_limit': 500,
            'download_rate_limit': 10000000 # 10MB/s
        })
        params = lt.parse_magnet_uri(MAGNET_LINK)
        params.save_path = '.'
        handle = ses.add_torrent(params)
        
        # Esperar metadados
        timeout = 0
        while not handle.status().has_metadata and timeout < 300:
            time.sleep(1)
            timeout += 1
        if not handle.status().has_metadata:
            logger.error("❌ Timeout aguardando metadados do torrent")
            return False
            
        # Download
        while not handle.status().is_seeding:
            status = handle.status()
            logger.info(f"📥 Download: {status.progress*100:.1f}% | "
                        f"Vel: {status.download_rate/1024/1024:.1f}MB/s | "
                        f"Peers: {status.num_peers}")
            time.sleep(10)
        logger.info("✅ Download do torrent concluído")
        return True
    except Exception as e:
        logger.error(f"❌ Erro no download: {e}")
        return False

def conectar_db(tentativa=0):
    """Conexão com retry"""
    try:
        conn = psycopg.connect(DB_URL)
        logger.info("✅ Conectado ao banco de dados")
        return conn
    except Exception as e:
        if tentativa < RETRY_ATTEMPTS:
            logger.warning(f"⚠️ Falha conexão (tentativa {tentativa+1}): {e}")
            time.sleep(RETRY_DELAY)
            return conectar_db(tentativa + 1)
        logger.error(f"❌ Falha permanente: {e}")
        return None

def inserir_lote(conn, buffer_lote: List[Tuple]) -> bool:
    """Insere lote no banco com retry"""
    for tentativa in range(RETRY_ATTEMPTS):
        try:
            with conn.cursor() as cur:
                # Criar tabela se não existir
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS leads (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(254) UNIQUE,
                        nome VARCHAR(255),
                        dominio VARCHAR(255),
                        origem VARCHAR(500),
                        criado_em TIMESTAMP DEFAULT NOW()
                    )
                """)
                # Insert com ON CONFLICT
                cur.executemany(
                    """
                    INSERT INTO leads (email, nome, dominio, origem)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    buffer_lote
                )
                conn.commit()
                logger.info(f"🚀 {len(buffer_lote)} leads inseridos com sucesso")
                return True
        except Exception as e:
            logger.warning(f"⚠️ Erro na inserção (tentativa {tentativa+1}): {e}")
            conn.rollback()
            if tentativa < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"❌ Falha permanente na inserção")
                return False
    return False

def processar_arquivo(conn, filepath: str, cache_local: Set[str]) -> Tuple[int, int]:
    """Processa arquivo tar.gz e retorna (emails_novos, emails_duplicados)"""
    emails_novos = 0
    emails_duplicados = 0
    buffer_lote = []
    buffer_restante = b''
    try:
        logger.info(f"⛏️ Processando: {filepath}")
        with tarfile.open(filepath, "r|gz") as tar:
            for member in tar:
                if not member.isfile(): continue
                if not any(member.name.endswith(ext) for ext in ['.txt', '.csv']): continue
                
                logger.info(f" 📄 {member.name} ({member.size/1024/1024:.1f}MB)")
                f = tar.extractfile(member)
                while True:
                    bloco = f.read(CHUNK_SIZE)
                    if not bloco: break
                    
                    dados = buffer_restante + bloco
                    ultimo_newline = dados.rfind(b'\n')
                    if ultimo_newline != -1:
                        processar = dados[:ultimo_newline + 1]
                        buffer_restante = dados[ultimo_newline + 1:]
                    else:
                        processar = dados
                        buffer_restante = b''
                    
                    emails = EMAIL_REGEX.findall(processar)
                    for email_bytes in emails:
                        email = email_bytes.decode('utf-8', 'ignore').lower().strip()
                        if not validar_email(email): continue
                        
                        if email not in cache_local:
                            cache_local.add(email)
                            nome = deduzir_nome(email_bytes)
                            dominio = email.split('@')[1]
                            buffer_lote.append((email, nome, dominio, member.name))
                            emails_novos += 1
                            
                            if len(buffer_lote) >= TAMANHO_LOTE:
                                inserir_lote(conn, buffer_lote)
                                buffer_lote = []
                        else:
                            emails_duplicados += 1
                
                # Processar restante do buffer
                if buffer_restante and EMAIL_REGEX.search(buffer_restante):
                    emails = EMAIL_REGEX.findall(buffer_restante)
                    for email_bytes in emails:
                        email = email_bytes.decode('utf-8', 'ignore').lower().strip()
                        if validar_email(email) and email not in cache_local:
                            cache_local.add(email)
                            buffer_lote.append((email, deduzir_nome(email_bytes), email.split('@')[1], member.name))
                            emails_novos += 1
                            buffer_restante = b''
                            
            if buffer_lote:
                inserir_lote(conn, buffer_lote)
            logger.info(f" ✅ {filepath} concluído")
            return emails_novos, emails_duplicados
    except Exception as e:
        logger.error(f"❌ Erro processando {filepath}: {e}")
        return 0, 0

def processar():
    """Função principal"""
    logger.info("=" * 50)
    logger.info("🚀 INICIANDO PROCESSAMENTO DE EMAILS")
    logger.info("=" * 50)
    inicio = time.time()
    
    if not baixar_torrent():
        logger.error("❌ Falha no download do torrent")
        sys.exit(1)
        
    conn = conectar_db()
    if not conn:
        logger.error("❌ Falha na conexão com banco de dados")
        sys.exit(1)
        
    cache_local = set()
    total_novos = 0
    total_duplicados = 0
    
    try:
        for arquivo in ARQUIVOS_ALVO:
            if not os.path.exists(arquivo):
                logger.warning(f"⚠️ Arquivo não encontrado: {arquivo}")
                continue
            novos, duplicados = processar_arquivo(conn, arquivo, cache_local)
            total_novos += novos
            total_duplicados += duplicados
            
        duracao = time.time() - inicio
        logger.info("=" * 50)
        logger.info(f"✅ PROCESSAMENTO CONCLUÍDO")
        logger.info(f" 📊 Emails novos: {total_novos}")
        logger.info(f" 🔄 Emails duplicados: {total_duplicados}")
        logger.info(f" 💾 Cache final: {len(cache_local)}")
        logger.info(f" ⏱️ Tempo total: {duracao/60:.1f}min")
        logger.info("=" * 50)
    except Exception as e:
        logger.error(f"❌ Erro geral: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    processar()
