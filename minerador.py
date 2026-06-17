import re, tarfile, os, psycopg, time

# CONFIGURAÇÃO
DATABASE_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)
TAMANHO_LOTE_COPY = 50000

def deduzir_nome(email):
    # Regex para limpar nome do email
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

def processar():
    for arquivo in os.listdir("."):
        if any(alvo in arquivo for alvo in ARQUIVOS_ALVO):
            print(f"📦 Iniciando processamento de: {arquivo}")
            total_arquivo = 0
            with tarfile.open(arquivo, "r|gz") as tar:
                lote = []
                for member in tar:
                    f = tar.extractfile(member)
                    if not f: continue
                    for line in f:
                        for em_b in EMAIL_REGEX.findall(line):
                            email = em_b.decode("utf-8", errors="ignore").lower().strip()
                            lote.append((email, deduzir_nome(email), email.split('@')[1], arquivo))
                            total_arquivo += 1
                            
                            if len(lote) >= TAMANHO_LOTE_COPY:
                                if enviar_ao_neon(lote):
                                    print(f"✅ Lote enviado | Total neste arquivo: {total_arquivo}")
                                lote = []
                # Enviar restante
                if lote and enviar_ao_neon(lote):
                    print(f"🏁 Fim do arquivo {arquivo}. Total: {total_arquivo}")
            
            # Remove após processar para liberar disco
            os.remove(arquivo)

if __name__ == "__main__":
    processar()
