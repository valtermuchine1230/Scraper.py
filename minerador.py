import re, tarfile, os, sqlite3, psycopg, time, sys

# CONFIGURAÇÃO
DATABASE_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)
TAMANHO_LOTE_COPY = 50000 

# SQLite para deduplicação em disco (Zero RAM usage)
db = sqlite3.connect("emails_temp.db")
db.execute("CREATE TABLE IF NOT EXISTS emails_vistos(email TEXT PRIMARY KEY)")

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
        return True
    except Exception as e:
        print(f"❌ Erro Neon: {e}")
        return False

def minerar():
    arquivos = [f for f in os.listdir(".") if any(alvo in f for alvo in ARQUIVOS_ALVO)]
    if not arquivos:
        print("❌ Nenhum arquivo encontrado. O download falhou?")
        return

    for arquivo in arquivos:
        print(f"📦 Processando: {arquivo}")
        with tarfile.open(arquivo, "r|gz") as tar:
            lote = []
            for member in tar:
                f = tar.extractfile(member)
                if not f: continue
                for line in f:
                    for em_b in EMAIL_REGEX.findall(line):
                        email = em_b.decode("utf-8", errors="ignore").lower().strip()
                        try:
                            db.execute("INSERT INTO emails_vistos VALUES (?)", (email,))
                            lote.append((email, deduzir_nome(email), email.split('@')[1], arquivo))
                            if len(lote) >= TAMANHO_LOTE_COPY:
                                if enviar_ao_neon(lote):
                                    print(f"✅ Lote de {len(lote)} enviado.")
                                lote = []
                        except sqlite3.IntegrityError: continue
        os.remove(arquivo) # Libera espaço no disco após processar

if __name__ == "__main__":
    minerar()
