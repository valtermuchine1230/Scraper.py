import re
import tarfile
import os
import sys
import time
import libtorrent as lt
import psycopg

# Configurações de Conexão e Alvos
DB_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]

# Constantes de Performance
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB
TAMANHO_LOTE = 5000
EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)

def deduzir_nome(email_bytes):
    username = email_bytes.split(b"@")[0]
    username = re.sub(rb"\d+", b"", username)
    username = re.sub(rb"[_.-]+", b" ", username).strip()
    return username.decode('utf-8', 'ignore').title() or "Trader Lead"

def baixar_torrent():
    print("📡 Iniciando Torrent...", flush=True)
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(MAGNET_LINK)
    params.save_path = '.'
    handle = ses.add_torrent(params)
    while not handle.status().has_metadata: time.sleep(1)
    while not handle.status().is_seeding:
        print(f"📥 Download: {handle.status().progress*100:.1f}%", end="\r", flush=True)
        time.sleep(10)
    print("\n✅ Download concluído.", flush=True)

def processar():
    baixar_torrent()
    cache_local = set()
    buffer_lote = []
    
    with psycopg.connect(DB_URL) as conn:
        for file in ARQUIVOS_ALVO:
            if not os.path.exists(file): continue
            with tarfile.open(file, "r|gz") as tar:
                for member in tar:
                    if not member.isfile() or not member.name.endswith(('.txt', '.csv')): continue
                    f = tar.extractfile(member)
                    print(f"⛏️ Processando: {member.name}", flush=True)
                    
                    buffer_restante = b''
                    while True:
                        bloco = f.read(CHUNK_SIZE)
                        if not bloco: break
                        
                        dados = buffer_restante + bloco
                        emails = EMAIL_REGEX.findall(dados)
                        buffer_restante = dados[-1024:]
                        
                        for e in emails:
                            e_str = e.decode('utf-8', 'ignore').lower()
                            if e_str not in cache_local:
                                cache_local.add(e_str)
                                buffer_lote.append((e_str, deduzir_nome(e), e_str.split('@')[1], member.name))
                                
                                if len(buffer_lote) >= TAMANHO_LOTE:
                                    try:
                                        with conn.cursor() as cur:
                                            with cur.copy("COPY leads (email, nome, dominio, origem) FROM STDIN") as copy:
                                                for row in buffer_lote: copy.write_row(row)
                                        conn.commit()
                                        print(f"🚀 {len(buffer_lote)} enviados. Cache: {len(cache_local)}")
                                        buffer_lote = []
                                    except Exception as e:
                                        print(f"⚠️ Erro no COPY, revertendo lote: {e}")
                                        conn.rollback()

if __name__ == "__main__":
    processar()
