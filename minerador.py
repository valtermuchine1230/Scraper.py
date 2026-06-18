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
MAGNET_LINK = "magnet:?xt=urn:btih:..." 
ARQUIVOS_ALVO = ["seu_arquivo.tar.gz"]

# ============ OTIMIZAÇÕES PARA VELOCIDADE ============
CHUNK_SIZE = 16 * 1024 * 1024  # 16MB (aumentado)
TAMANHO_LOTE = 10000  # Aumentado para menos commits
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

def baixar_torrent_otimizado() -> bool:
    """Download do torrent com máxima velocidade"""
    logger.info("📡 INICIANDO DOWNLOAD COM VELOCIDADE MÁXIMA...")
    
    try:
        # Configuração agressiva para máxima velocidade
        settings = {
            'listen_interfaces': '0.0.0.0:6881,0.0.0.0:6889',
            'connections_limit': 1000,  # Máximo de conexões
            'connection_speed': 100,  # Conexões por segundo
            'request_timeout': 3,
            'peer_connect_timeout': 3,
            'download_rate_limit': 0,  # Sem limite de velocidade
            'upload_rate_limit': 50000000,  # 50MB/s para upload
            'active_downloads': 100,
            'active_seeds': 100,
            'active_dht_limit': 600,
            'max_metadata_size': 16777216,
            'max_piece_size': 16777216,
            'max_out_request_queue': 500,
            'whole_pieces_threshold': 2097152,  # 2MB
        }
        
        ses = lt.session(settings)
        
        # Permitir conexões IPv4 e IPv6
        ses.add_dht_router("router.utorrent.com", 6881)
        ses.add_dht_router("dht.transmissionbt.com", 6881)
        ses.add_dht_router("router.bittorrent.com", 6881)
        
        # Parse magnet
        params = lt.parse_magnet_uri(MAGNET_LINK)
        params.save_path = '.'
        params.flags |= lt.torrent_flags.sequential_download  # Download sequencial
        
        handle = ses.add_torrent(params)
        
        # Esperar metadados
        logger.info("⏳ Aguardando metadados do torrent...")
        timeout = 0
        while not handle.status().has_metadata and timeout < 300:
            time.sleep(0.5)
            timeout += 0.5
            if timeout % 10 == 0:
                logger.info(f"  ⏳ {timeout:.0f}s aguardando metadados...")
        
        if not handle.status().has_metadata:
            logger.error("❌ Timeout aguardando metadados")
            return False
        
        logger.info("✅ Metadados recebidos!")
        
        # Desabilitar upload enquanto baixa
        ses.set_settings({'upload_rate_limit': 0})
        
        # Download com feedback contínuo
        inicio = time.time()
        ultimo_reporte = 0
        velocidade_pico = 0
        
        while not handle.status().is_seeding:
            status = handle.status()
            progress = status.progress * 100
            vel_mb = status.download_rate / 1024 / 1024
            
            if vel_mb > velocidade_pico:
                velocidade_pico = vel_mb
            
            tempo_atual = time.time() - inicio
            if tempo_atual - ultimo_reporte >= 5:  # Reporte a cada 5 segundos
                logger.info(
                    f"📥 {progress:6.2f}% | "
                    f"Vel: {vel_mb:7.2f}MB/s | "
                    f"Pico: {velocidade_pico:7.2f}MB/s | "
                    f"Peers: {status.num_peers:4d} | "
                    f"Seeds: {status.num_seeds:3d}"
                )
                ultimo_reporte = tempo_atual
            
            time.sleep(1)
        
        tempo_total = time.time() - inicio
        logger.info("=" * 70)
        logger.info(f"✅ DOWNLOAD CONCLUÍDO COM SUCESSO!")
        logger.info(f"  📊 Velocidade pico: {velocidade_pico:.2f}MB/s")
        logger.info(f"  ⏱️ Tempo total: {tempo_total/60:.1f} minutos")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no download: {e}")
        return False

def conectar_db(tentativa=0):
    """Conexão com retry"""
    try:
        conn = psycopg.connect(DB_URL, timeout=10)
        logger.info("✅ Conectado ao banco de dados")
        return conn
    except Exception as e:
        if tentativa < RETRY_ATTEMPTS:
            logger.warning(f"⚠️ Falha conexão (tentativa {tentativa+1}): {e}")
            time.sleep(RETRY_DELAY)
            return conectar_db(tentativa + 1)
        logger.error(f"❌ Falha permanente: {e}")
        return None

def inserir_lote_otimizado(conn, buffer_lote: List[Tuple]) -> bool:
    """Insere lote com máxima performance"""
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
                
                # Criar índices para performance
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_leads_email 
                    ON leads(email)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_leads_dominio 
                    ON leads(dominio)
                """)
                
                # Batch insert otimizado
                cur.executemany(
                    """
                    INSERT INTO leads (email, nome, dominio, origem)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    buffer_lote
                )
            
            conn.commit()
            logger.info(f"🚀 {len(buffer_lote)} leads inseridos | Cache atual")
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

def processar_arquivo_otimizado(conn, filepath: str, cache_local: Set[str]) -> Tuple[int, int]:
    """Processa arquivo tar.gz com velocidade otimizada"""
    emails_novos = 0
    emails_duplicados = 0
    buffer_lote = []
    buffer_restante = b''
    
    try:
        logger.info(f"⛏️ Processando: {filepath}")
        
        with tarfile.open(filepath, "r|gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                
                if not any(member.name.endswith(ext) for ext in ['.txt', '.csv']):
                    continue
                
                logger.info(f"  📄 {member.name} ({member.size/1024/1024:.1f}MB)")
                f = tar.extractfile(member)
                
                while True:
                    bloco = f.read(CHUNK_SIZE)
                    if not bloco:
                        break
                    
                    dados = buffer_restante + bloco
                    
                    # Encontrar último newline
                    ultimo_newline = dados.rfind(b'\n')
                    if ultimo_newline != -1:
                        processar = dados[:ultimo_newline + 1]
                        buffer_restante = dados[ultimo_newline + 1:]
                    else:
                        processar = dados
                        buffer_restante = b''
                    
                    # Extrair emails em batch
                    emails = EMAIL_REGEX.findall(processar)
                    
                    for email_bytes in emails:
                        email = email_bytes.decode('utf-8', 'ignore').lower().strip()
                        
                        if not validar_email(email):
                            continue
                        
                        if email not in cache_local:
                            cache_local.add(email)
                            nome = deduzir_nome(email_bytes)
                            dominio = email.split('@')[1]
                            
                            buffer_lote.append((email, nome, dominio, member.name))
                            emails_novos += 1
                            
                            # Inserir quando atingir limite
                            if len(buffer_lote) >= TAMANHO_LOTE:
                                inserir_lote_otimizado(conn, buffer_lote)
                                buffer_lote = []
                        else:
                            emails_duplicados += 1
                
                # Processar restante
                if buffer_restante and EMAIL_REGEX.search(buffer_restante):
                    emails = EMAIL_REGEX.findall(buffer_restante)
                    for email_bytes in emails:
                        email = email_bytes.decode('utf-8', 'ignore').lower().strip()
                        if validar_email(email) and email not in cache_local:
                            cache_local.add(email)
                            buffer_lote.append((
                                email,
                                deduzir_nome(email_bytes),
                                email.split('@')[1],
                                member.name
                            ))
                            emails_novos += 1
                    buffer_restante = b''
        
        # Inserir lote restante
        if buffer_lote:
            inserir_lote_otimizado(conn, buffer_lote)
        
        logger.info(f"  ✅ {filepath} concluído")
        return emails_novos, emails_duplicados
        
    except Exception as e:
        logger.error(f"❌ Erro processando {filepath}: {e}")
        return 0, 0

def processar():
    """Função principal otimizada"""
    logger.info("=" * 70)
    logger.info("🚀 PROCESSAMENTO DE EMAILS - MODO VELOCIDADE MÁXIMA")
    logger.info("=" * 70)
    
    inicio = time.time()
    
    # Download
    if not baixar_torrent_otimizado():
        logger.error("❌ Falha no download do torrent")
        sys.exit(1)
    
    # Conexão
    conn = conectar_db()
    if not conn:
        logger.error("❌ Falha na conexão com banco de dados")
        sys.exit(1)
    
    cache_local = set()
    total_novos = 0
    total_duplicados = 0
    
    try:
        # Processar arquivos
        for arquivo in ARQUIVOS_ALVO:
            if not os.path.exists(arquivo):
                logger.warning(f"⚠️ Arquivo não encontrado: {arquivo}")
                continue
            
            novos, duplicados = processar_arquivo_otimizado(conn, arquivo, cache_local)
            total_novos += novos
            total_duplicados += duplicados
        
        # Estatísticas finais
        duracao = time.time() - inicio
        logger.info("=" * 70)
        logger.info(f"✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        logger.info(f"  📊 Emails novos: {total_novos:,}")
        logger.info(f"  🔄 Emails duplicados: {total_duplicados:,}")
        logger.info(f"  💾 Cache final: {len(cache_local):,}")
        logger.info(f"  ⏱️ Tempo total: {duracao/60:.1f}min ({duracao/3600:.2f}h)")
        logger.info(f"  📈 Velocidade média: {total_novos/(duracao/60):.0f} emails/min")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"❌ Erro geral: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    processar()
