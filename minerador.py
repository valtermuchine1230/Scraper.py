import re
import tarfile
import requests
import os
import sys
import time
import libtorrent as lt

# Força o Python a cuspir os logs essenciais imediatamente
sys.stdout.reconfigure(line_buffering=True)

SUPABASE_URL = "https://rbgbwqossenorypfrzln.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJiZ2J3cW9zc2Vub3J5cGZyemxuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg1NjA0NDcsImV4cCI6MjA3NDEzNjQ0N30.hD-pkTPJCM7ZmwQIyMpoBNJv-Hx6S1AvO9KGPZVzQjs"

MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"

ARQUIVOS_ALVO = [
    "Trading Collection.tar.gz",
    "Collection #4_BTC combos.tar.gz"
]

EMAIL_REGEX_BYTES = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)

# Calibrações de segurança para performance e estabilidade
TAMANHO_LOTE_UPLOAD = 1000     
TAMANHO_SINC_DOWNLOAD = 100000  # Puxa blocos maiores de cada vez para reduzir chamadas HTTP
CHUNK_SIZE = 8 * 1024 * 1024    

http_session = requests.Session()
http_session.headers.update({
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
})

def carregar_emails_existentes_via_id():
    print("🔄 [Cache] A carregar base de dados remota para a RAM...", flush=True)
    emails_cache = set()
    last_id = 0
    total_baixado = 0
    url = f"{SUPABASE_URL}/rest/v1/rpc/get_emails_batch_id"
    
    ult_print = 0
    while True:
        payload = {"last_id": last_id, "batch_size": TAMANHO_SINC_DOWNLOAD}
        try:
            r = http_session.post(url, json=payload, timeout=60.0)
            if r.status_code != 200:
                time.sleep(10)
                continue
            dados = r.json()
            if not dados:
                break
            for item in dados:
                if item.get("email"):
                    emails_cache.add(item["email"].strip().lower())
            total_baixado += len(dados)
            last_id = dados[-1]["id"]
            
            # PROTEÇÃO DE DISCO: Só escreve no log a cada 200.000 registos salvando espaço
            if total_baixado - ult_print >= 200000:
                print(f"📥 [Cache-RAM] Total sincronizado: {total_baixado:,} e-mails.", flush=True)
                ult_print = total_baixado
                
        except Exception:
            time.sleep(15)
            
    print(f"✅ Sincronização Concluída! Cache RAM com {len(emails_cache):,} registos prontos.", flush=True)
    return emails_cache

def salvar_checkpoint_supabase(member_name, bytes_processed):
    url = f"{SUPABASE_URL}/rest/v1/minerador_status?id=eq.1"
    try:
        http_session.patch(url, json={"member_name": member_name, "line_number": bytes_processed}, timeout=10.0)
    except Exception:
        pass

def carregar_checkpoint_supabase():
    url = f"{SUPABASE_URL}/rest/v1/minerador_status?id=eq.1&select=member_name,line_number"
    try:
        r = http_session.get(url, timeout=10.0)
        if r.status_code == 200 and r.json() and r.json()[0]["member_name"]:
            return r.json()[0]["member_name"], int(r.json()[0]["line_number"])
    except Exception:
        pass
    return None, 0

def deduzir_nome(email: str) -> str:
    username = email.split("@")[0]
    username = re.sub(r"\d+", "", username)
    username = re.sub(r"[_.-]+", " ", username).strip()
    return username.title() if username else "Trader Lead"

def enviar_lote_final_supabase(lote):
    if not lote:
        return
    url = f"{SUPABASE_URL}/rest/v1/rpc/importar_leads_flash"
    
    for tentativa in range(5):
        try:
            r = http_session.post(url, json={"lote_dados": lote}, timeout=60.0)
            if r.status_code == 200:
                return
            time.sleep(3 * (tentativa + 1))
        except Exception:
            time.sleep(5)

def baixar_torrent_seletivo(arquivos_alvo):
    print("📡 Conectando ao Enxame BitTorrent...", flush=True)
    ses = lt.session({"listen_interfaces": "0.0.0.0:6881", "enable_dht": True})
    handle = lt.add_magnet_uri(ses, MAGNET_LINK, {'save_path': '.'})
    while not handle.has_metadata():
        time.sleep(2)
    tor_info = handle.get_torrent_info()
    for index, f in enumerate(tor_info.files()):
        handle.file_priority(index, 7 if any(alvo in f.path for alvo in arquivos_alvo) else 0)
        
    ult_progresso = -1.0
    while not handle.is_seed():
        s = handle.status()
        progresso_atual = s.progress * 100
        
        # Reduz os logs do Torrent para imprimir apenas quando mudar 1% inteiro
        if progresso_atual - ult_progresso >= 1.0:
            print(f"📥 Download: {progresso_atual:.1f}% | Velocidade: {s.download_rate/1024/1024:.2f} MB/s | Peers: {s.num_peers}", flush=True)
            ult_progresso = progresso_atual
            
        completos = sum(1 for idx, f in enumerate(tor_info.files()) if any(alvo in f.path for alvo in arquivos_alvo) and handle.file_progress()[idx] >= f.size)
        if completos == len(arquivos_alvo):
            break
        time.sleep(10)
    print("✅ Alvos guardados localmente.", flush=True)
    return True

def processar_arquivos_tar(emails_cache):
    checkpoint_member, checkpoint_bytes = carregar_checkpoint_supabase()
    skip_mode = checkpoint_member is not None
    buffer_supabase = []
    total_encontrados_global = 0

    for root, dirs, files in os.walk("."):
        for file in files:
            if not (file.endswith(".tar.gz") and any(alvo in file for alvo in ARQUIVOS_ALVO)):
                continue
                
            alvo_tar = os.path.join(root, file)
            print(f"\n📦 [Streaming Ativo] Analisando: {alvo_tar}", flush=True)
            
            with tarfile.open(alvo_tar, "r|gz", errorlevel=0) as tar:
                for member in tar:
                    if not member.isfile() or not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                        continue
                    
                    if skip_mode:
                        if member.name != checkpoint_member:
                            continue
                        skip_mode = False

                    f = tar.extractfile(member)
                    if not f:
                        continue
                    
                    print(f"⛏️ [Mineração] Ficheiro Atual: {member.name}", flush=True)
                    bytes_lidos = 0
                    
                    if checkpoint_bytes > 0:
                        bytes_saltados = 0
                        while bytes_saltados < checkpoint_bytes:
                            tamanho_proximo_bloco = min(CHUNK_SIZE, checkpoint_bytes - bytes_saltados)
                            descarte = f.read(tamanho_proximo_bloco)
                            if not descarte:
                                break
                            bytes_saltados += len(descarte)
                        bytes_lidos = bytes_saltados
                        print(f"🎯 Checkpoint alinhado em {bytes_lidos / 1024 / 1024:.2f} MB.", flush=True)
                    
                    buffer_restante = b''
                    ult_print_mb = 0.0
                    
                    while True:
                        bloco = f.read(CHUNK_SIZE)
                        if not bloco:
                            break
                            
                        bytes_lidos += len(bloco)
                        dados = buffer_restante + bloco
                        
                        encontrados = EMAIL_REGEX_BYTES.findall(dados)
                        total_encontrados_global += len(encontrados)
                        buffer_restante = dados[-4096:]
                        
                        mb = bytes_lidos / 1024 / 1024
                        # Controla logs de processamento para reportar apenas de 40MB em 40MB
                        if mb - ult_print_mb >= 40.0:
                            print(f"📂 {mb:.1f} MB lidos | Total de e-mails capturados: {total_encontrados_global:,}", flush=True)
                            ult_print_mb = mb
                        
                        for email_b in encontrados:
                            try:
                                email = email_b.decode("utf-8", errors="ignore").lower().strip()
                                if email in emails_cache:
                                    continue
                                
                                emails_cache.add(email)
                                buffer_supabase.append({
                                    "email_p": email,
                                    "nome_p": deduzir_nome(email),
                                    "origem_p": f"{file}/{member.name}"
                                })
                                
                                if len(buffer_supabase) >= TAMANHO_LOTE_UPLOAD:
                                    enviar_lote_final_supabase(buffer_supabase)
                                    salvar_checkpoint_supabase(member.name, bytes_processed=bytes_lidos)
                                    buffer_supabase = []
                            except Exception:
                                continue
                                
                    checkpoint_bytes = 0
                    
    if buffer_supabase:
        enviar_lote_final_supabase(buffer_supabase)
        salvar_checkpoint_supabase(member.name, bytes_processed=bytes_lidos)
    print("🏁 [Fim da Execução] Processamento concluído com sucesso.", flush=True)

if __name__ == "__main__":
    try:
        cache_ram = carregar_emails_existentes_via_id()
        if baixar_torrent_seletivo(ARQUIVOS_ALVO):
            processar_arquivos_tar(cache_ram)
    except Exception as e:
        print(f"💥 Erro Fatal: {e}", flush=True)
        raise
