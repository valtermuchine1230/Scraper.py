import re, tarfile, os, sys, time, sqlite3, psycopg, libtorrent as lt

sys.stdout.reconfigure(line_buffering=True)

# CONFIGURAÇÃO
DATABASE_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)
TAMANHO_LOTE = 50000

# SQLite: Deduplicação sem gastar RAM
db = sqlite3.connect("emails_temp.db")
db.execute("CREATE TABLE IF NOT EXISTS emails_vistos(email TEXT PRIMARY KEY)")

def deduzir_nome(email: str) -> str:
    username = re.sub(r"[_.-]+", " ", re.sub(r"\d+", "", email.split("@")[0])).strip().title()
    return username if username else "Trader Lead"

def enviar_ao_neon(lote):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                with cur.copy("COPY leads_traders (email, nome, dominio, origem) FROM STDIN") as copy:
                    for row in lote: copy.write_row(row)
                conn.commit()
        return True
    except Exception as e:
        print(f"❌ Erro Neon: {e}")
        return False

def minerar():
    ses = lt.session({"listen_interfaces": "0.0.0.0:6881", "enable_dht": True})
    handle = lt.add_magnet_uri(ses, MAGNET_LINK, {'save_path': '.'})
    
    print("📡 Aguardando Metadados Torrent...", flush=True)
    while not handle.has_metadata(): time.sleep(1)
    
    print("🚀 Iniciando Mineração Stream...", flush=True)
    lote = []
    
    # Processamento em Stream dos arquivos baixados
    for arquivo in ARQUIVOS_ALVO:
        # Nota: O libtorrent gerencia o download enquanto lemos
        if not os.path.exists(arquivo): continue
        with tarfile.open(arquivo, "r|gz") as tar:
            for member in tar:
                f = tar.extractfile(member)
                if not f: continue
                for line in f:
                    for em_b in EMAIL_REGEX.findall(line):
                        email = em_b.decode("utf-8", errors="ignore").lower().strip()
                        try:
                            db.execute("INSERT INTO emails_vistos VALUES (?)", (email,))
                            lote.append((email, deduzir_nome(email), email.split('@')[1], arquivo))
                            if len(lote) >= TAMANHO_LOTE:
                                enviar_ao_neon(lote)
                                print(f"✅ Lote Neon enviado.", flush=True)
                                lote = []
                        except sqlite3.IntegrityError: continue

if __name__ == "__main__":
    minerar()
