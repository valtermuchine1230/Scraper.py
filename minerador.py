import re, tarfile, os, sys, time, sqlite3, psycopg
from psycopg import sql

# CONEXÃO DIRETA (Conforme solicitado)
DATABASE_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)
TAMANHO_LOTE_COPY = 50000 # Lote gigante para alta performance

# SQLite para deduplicação local (Eficiente e rápido)
db_local = sqlite3.connect("emails_temp.db")
db_local.execute("CREATE TABLE IF NOT EXISTS emails_vistos(email TEXT PRIMARY KEY)")

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def enviar_lote_copy(lote):
    for tentativa in range(5):
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    with cur.copy("COPY leads_traders (email, nome, dominio, origem) FROM STDIN") as copy:
                        for row in lote:
                            copy.write_row(row)
                    conn.commit()
            return True
        except Exception as e:
            log(f"⚠️ Erro no COPY (Tentativa {tentativa+1}): {e}")
            time.sleep(5 * (tentativa + 1))
    return False

def minerar():
    log("🚀 Iniciando Motor de Alta Performance...")
    lote, total, unicos = [], 0, 0
    
    for alvo in ARQUIVOS_ALVO:
        if not os.path.exists(alvo): continue
        log(f"📦 Extraindo arquivo: {alvo}")
        with tarfile.open(alvo, "r|gz") as tar:
            for member in tar:
                f = tar.extractfile(member)
                if not f: continue
                for line in f:
                    for em_b in EMAIL_REGEX.findall(line):
                        total += 1
                        email = em_b.decode("utf-8", errors="ignore").lower().strip()
                        try:
                            db_local.execute("INSERT INTO emails_vistos VALUES (?)", (email,))
                            lote.append((email, email.split('@')[0].title(), email.split('@')[1], alvo))
                            unicos += 1
                            if len(lote) >= TAMANHO_LOTE_COPY:
                                if enviar_lote_copy(lote):
                                    log(f"✅ Lote enviado! | Total processado: {total} | Únicos: {unicos}")
                                lote = []
                        except sqlite3.IntegrityError: pass
        os.remove(alvo)
        log(f"🗑️ {alvo} limpo para economizar espaço.")

if __name__ == "__main__":
    minerar()
