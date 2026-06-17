import re
import tarfile
import os
import sys
import time
import io
import libtorrent as lt
import psycopg
from psycopg import sql

# Configurações
DB_URL = "postgresql://authenticator:npg_kIH5FMhy9EcR@ep-delicate-heart-ad6by8cm-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"
ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
TAMANHO_LOTE = 5000 

EMAIL_REGEX = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)

def deduzir_nome(email):
    username = email.split(b"@")[0]
    username = re.sub(rb"\d+", b"", username)
    username = re.sub(rb"[_.-]+", b" ", username).strip()
    return username.decode('utf-8', 'ignore').title() or "Trader Lead"

def inserir_lote_copy(conn, buffer_lote):
    """Insere dados usando COPY, muito mais rápido que INSERTs comuns."""
    try:
        with conn.cursor() as cur:
            with cur.copy("COPY leads (email, nome, dominio, origem) FROM STDIN") as copy:
                for row in buffer_lote:
                    copy.write_row(row)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Erro no COPY: {e}")
        return False

def baixar_torrent():
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    handle = lt.add_magnet_uri(ses, MAGNET_LINK, {'save_path': '.'})
    while not handle.has_metadata(): time.sleep(1)
    print("Download iniciado...")
    while not handle.is_seed():
        print(f"Progresso: {handle.status().progress*100:.2f}%", end="\r")
        time.sleep(5)

def minerar():
    baixar_torrent()
    with psycopg.connect(DB_URL) as conn:
        buffer_lote = []
        cache_local = set() # Deduplicação rápida em RAM
        
        for file in ARQUIVOS_ALVO:
            with tarfile.open(file, "r|gz") as tar:
                for member in tar:
                    if not member.isfile() or not member.name.endswith(('.txt', '.csv')): continue
                    f = tar.extractfile(member)
                    
                    print(f"Minerando: {member.name}")
                    for line in f:
                        emails = EMAIL_REGEX.findall(line)
                        for e in emails:
                            email_str = e.decode('utf-8').lower()
                            if email_str not in cache_local:
                                cache_local.add(email_str)
                                buffer_lote.append((email_str, deduzir_nome(e), email_str.split('@')[1], member.name))
                                
                                if len(buffer_lote) >= TAMANHO_LOTE:
                                    if inserir_lote_copy(conn, buffer_lote):
                                        print(f"Lote enviado. Total RAM: {len(cache_local)}")
                                        buffer_lote = []

if __name__ == "__main__":
    minerar()
