import re, tarfile, os, sys, time, libtorrent as lt, psycopg

# Forçar logs em tempo real
sys.stdout.reconfigure(line_buffering=True)

# CONFIGURAÇÃO NEON
DATABASE_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)

def deduzir_nome(email):
    username = re.sub(r"[_.-]+", " ", re.sub(r"\d+", "", email.split("@")[0])).strip().title()
    return username if username else "Trader Lead"

def enviar_ao_neon(lote):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                with cur.copy("COPY leads_traders (email, nome, dominio, origem) FROM STDIN") as copy:
                    for row in lote: copy.write_row(row)
                conn.commit()
    except Exception as e:
        print(f"Erro no envio ao Neon: {e}")

def minerar():
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    handle = lt.add_magnet_uri(ses, MAGNET_LINK, {'save_path': '.'})
    
    print("📡 Aguardando metadados do torrent...", flush=True)
    while not handle.has_metadata(): time.sleep(1)
    
    print("🚀 Início da mineração em Stream...", flush=True)
    lote = []
    
    # Loop de monitorização e mineração
    while not handle.is_seed():
        s = handle.status()
        print(f"📥 Progresso: {s.progress*100:.1f}% | Velocidade: {s.download_rate/1024:.0f} KB/s | Peers: {s.num_peers}", flush=True)
        
        # Processar arquivos se existirem
        for arquivo in ARQUIVOS_ALVO:
            if os.path.exists(arquivo):
                try:
                    with tarfile.open(arquivo, "r|gz") as tar:
                        for member in tar:
                            f = tar.extractfile(member)
                            if not f: continue
                            for line in f:
                                for em_b in EMAIL_REGEX.findall(line):
                                    email = em_b.decode("utf-8", errors="ignore").lower().strip()
                                    lote.append((email, deduzir_nome(email), email.split('@')[1], arquivo))
                                    if len(lote) >= 5000:
                                        enviar_ao_neon(lote)
                                        lote = []
                except Exception as e:
                    pass # Ignora erros de leitura de arquivo incompleto
        time.sleep(15)

if __name__ == "__main__":
    minerar()
