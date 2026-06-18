import re
import tarfile
import io
import os
import logging
import psycopg
import libtorrent as lt

# Configurações
DB_URL = os.getenv("DB_URL")
MAGNET_LINK = "SEU_MAGNET_LINK"
ARQUIVOS_ALVO = ["arquivo1.tar.gz", "arquivo2.tar.gz"]
BATCH_SIZE = 50000  # Aumentado para o COPY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def deduzir_nome(email):
    username = email.split("@")[0]
    username = re.sub(r'\d+', '', username)
    username = re.sub(r'[_.-]+', ' ', username)
    return " ".join(word.capitalize() for word in username.split()) or "Trader Lead"

def inserir_via_copy(conn, batch):
    """Insere dados usando a API COPY do Postgres (performance máxima)"""
    if not batch: return
    
    with conn.cursor() as cur:
        with cur.copy("COPY leads (email, nome, dominio, origem) FROM STDIN") as copy:
            for row in batch:
                # O formato do copy precisa de tabs ou CSV
                copy.write_row(row)
    conn.commit()

def processar_torrent():
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(MAGNET_LINK)
    params.save_path = "."
    handle = ses.add_torrent(params)
    
    logger.info("⏳ Aguardando metadados...")
    while not handle.status().has_metadata: pass
    
    info = handle.get_torrent_info()
    for i in range(info.num_files()):
        handle.file_priority(i, 7 if any(a in info.files().at(i).path for a in ARQUIVOS_ALVO) else 0)
    
    while handle.status().progress < 1.0: pass
    logger.info("✅ Download concluído.")

def main():
    processar_torrent()
    conn = psycopg.connect(DB_URL)
    regex = re.compile(rb'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b')
    
    for arq in ARQUIVOS_ALVO:
        batch = []
        with tarfile.open(arq, "r|gz") as tar:
            for member in tar:
                if member.isfile() and member.name.endswith(('.txt', '.csv')):
                    f = tar.extractfile(member)
                    for line in f:
                        for match in regex.findall(line):
                            email = match.decode('utf-8', 'ignore').lower().strip()
                            batch.append((email, deduzir_nome(email), email.split('@')[1], member.name))
                            
                            if len(batch) >= BATCH_SIZE:
                                inserir_via_copy(conn, batch)
                                batch = []
        inserir_via_copy(conn, batch) # Restante
    conn.close()

if __name__ == "__main__":
    main()
