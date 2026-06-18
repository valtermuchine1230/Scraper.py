import re
import tarfile
import io
import time
import libtorrent as lt
import psycopg
import os
import logging

# CONFIGURAÇÃO FIXA
DB_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVOS_ALVO = ["Collection #2_New combo cloud_Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
EMAIL_REGEX = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE)

def deduzir_nome(email_str):
    username = email_str.split("@")[0]
    username = re.sub(r'\d+', '', username)
    username = re.sub(r'[_.-]+', ' ', username)
    return " ".join(word.capitalize() for word in username.split()) or "Trader Lead"

def carregar_no_banco(conn, batch):
    if not batch: return
    # Usa COPY para performance máxima
    f = io.StringIO()
    for item in batch:
        f.write(f"{item[0]}\t{item[1]}\t{item[2]}\t{item[3]}\n")
    f.seek(0)
    with conn.cursor() as cur:
        # ON CONFLICT DO NOTHING requer que a tabela tenha UNIQUE index em 'email'
        cur.copy_expert("COPY leads(email, nome, dominio, origem) FROM STDIN WITH (FORMAT csv, DELIMITER '\t')", f)
    conn.commit()

def processar():
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(MAGNET_LINK)
    params.save_path = "."
    handle = ses.add_torrent(params)
    
    logging.info("⏳ Aguardando metadados...")
    while not handle.status().has_metadata: time.sleep(1)
    
    # Define prioridade apenas para os arquivos alvo
    info = handle.get_torrent_info()
    for i in range(info.num_files()):
        handle.file_priority(i, 7 if any(a in info.files().at(i).path for a in ARQUIVOS_ALVO) else 0)
    
    logging.info("📥 Baixando...")
    while handle.status().progress < 1.0: time.sleep(10)
    
    conn = psycopg.connect(DB_URL)
    batch = []
    
    for arq in ARQUIVOS_ALVO:
        if not os.path.exists(arq): continue
        with tarfile.open(arq, "r|gz") as tar:
            for member in tar:
                if member.isfile() and member.name.endswith(('.txt', '.csv')):
                    f = tar.extractfile(member)
                    for line in f:
                        for email_b in EMAIL_REGEX.findall(line):
                            email = email_b.decode('utf8', 'ignore').lower()
                            batch.append((email, deduzir_nome(email), email.split('@')[1], member.name))
                            if len(batch) >= 10000:
                                carregar_no_banco(conn, batch)
                                batch.clear()
    carregar_no_banco(conn, batch)
    conn.close()
    logging.info("🚀 Sucesso total.")

if __name__ == "__main__": processar()
