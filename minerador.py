import re
import tarfile
import requests
import os
import sys
import time
import libtorrent as lt

# Garante o flush imediato dos logs para monitoramento em tempo real no GitHub Actions
sys.stdout.reconfigure(line_buffering=True)

SUPABASE_URL = "https://rbgbwqossenorypfrzln.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJiZ2J3cW9zc2Vub3J5cGZyemxuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg1NjA0NDcsImV4cCI6MjA3NDEzNjQ0N30.hD-pkTPJCM7ZmwQIyMpoBNJv-Hx6S1AvO9KGPZVzQjs"

MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"

ARQUIVOS_ALVO = [
    "Trading Collection.tar.gz",
    "Collection #4_BTC combos.tar.gz"
]

# REGEX EM BYTES: Processamento direto na memória binária para velocidade máxima (Ultra-Fast)
EMAIL_REGEX_BYTES = re.compile(rb"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

TAMANHO_LOTE_UPLOAD = 50000    
TAMANHO_SINC_DOWNLOAD = 50000  

http_session = requests.Session()
http_session.headers.update({
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
})

def carregar_emails_existentes_via_id():
    """Consome o banco via Keyset por ID numérico e joga as chaves na RAM para deduplicação instantânea."""
    print("🔄 [Memory-Cache] Baixando base indexada do Supabase via ID...")
    emails_cache = set()
    last_id = 0
    total_baixado = 0
    
    url = f"{SUPABASE_URL}/rest/v1/rpc/get_emails_batch_id"
    
    while True:
        payload = {"last_id": last_id, "batch_size": TAMANHO_SINC_DOWNLOAD}
        try:
            r = http_session.post(url, json=payload, timeout=60.0)
            if r.status_code != 200:
                print(f"⚠️ Instabilidade na leitura ({r.status_code}). Re-tentando em 10s...")
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
            print(f"📥 Sincronizados {total_baixado} e-mails em memória RAM (ID atual: {last_id})...")
            
        except Exception as e:
            print(f"⚠️ Erro na conexão de sync: {e}. Aguardando 15s...")
            time.sleep(15)
            
    print(f"✅ Cache em RAM pronto! Total de chaves ativas para Dedupe: {len(emails_cache)}")
    return emails_cache

def salvar_checkpoint_supabase(member_name, line_number):
    url = f"{SUPABASE_URL}/rest/v1/minerador_status?id=eq.1"
    try:
        http_session.patch(url, json={"member_name": member_name, "line_number": line_number}, timeout=15.0)
    except Exception:
        pass

def carregar_checkpoint_supabase():
    url = f"{SUPABASE_URL}/rest/v1/minerador_status?id=eq.1&select=member_name,line_number"
    try:
        r = http_session.get(url, timeout=15.0)
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
            r = http_session.post(url, json={"lote_dados": lote}, timeout=120.0)
            if r.status_code == 200:
                print(f"🚀 [RPC Push] +{len(lote)} e-mails processados e sincronizados com o servidor.")
                return
            if r.status_code == 409:
                return
            time.sleep(10 * (tentativa + 1))
        except Exception:
            time.sleep(15)
    raise RuntimeError("❌ Supabase recusou o lote repetidamente.")

def baixar_torrent_seletivo(arquivos_alvo):
    print("📡 Inicializando Sessão Libtorrent...")
    ses = lt.session({"listen_interfaces": "0.0.0.0:6881", "enable_dht": True})
    handle = lt.add_magnet_uri(ses, MAGNET_LINK, {'save_path': '.'})
    
    while not handle.has_metadata():
        time.sleep(5)
        
    tor_info = handle.get_torrent_info()
    for index, f in enumerate(tor_info.files()):
        handle.file_priority(index, 7 if any(alvo in f.path for alvo in arquivos_alvo) else 0)

    while not handle.is_seed():
        s = handle.status()
        print(f"📥 Download Geral: {s.progress*100:.2f}% | Velocidade: {s.download_rate/1024/1024:.2f} MB/s")
        
        completos = sum(1 for idx, f in enumerate(tor_info.files()) if any(alvo in f.path for alvo in arquivos_alvo) and handle.file_progress()[idx] >= f.size)
        if completos == len(arquivos_alvo):
            break
        time.sleep(20)
    print("✅ Download dos arquivos compactados concluído com sucesso!")
    return True

def processar_arquivos_tar(emails_cache):
    checkpoint_member, checkpoint_line = carregar_checkpoint_supabase()
    skip_mode = checkpoint_member is not None
    buffer_supabase = []

    for root, dirs, files in os.walk("."):
        for file in files:
            if not (file.endswith(".tar.gz") and any(alvo in file for alvo in ARQUIVOS_ALVO)):
                continue
                
            alvo_tar = os.path.join(root, file)
            print(f"\n📦 Abrindo stream de extração no arquivo gigante: {alvo_tar}")
            
            with tarfile.open(alvo_tar, "r:gz", errorlevel=0) as tar:
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
                    
                    print(f"⛏️ Varrendo linhas de: {member.name}")
                    numero_da_linha_atual = 0
                    
                    # Leitura direta em Bytes para performance extrema
                    for line_bytes in f:
                        numero_da_linha_atual += 1
                        if checkpoint_line > 0 and numero_da_linha_atual <= checkpoint_line:
                            continue
                        
                        try:
                            # Corta delimitadores comuns diretamente nos bytes antes de decodificar
                            line_cleaned = line_bytes.strip()
                            if b":" in line_cleaned:
                                partes = line_cleaned.split(b":")
                            elif b";" in line_cleaned:
                                partes = line_cleaned.split(b";")
                            else:
                                partes = [line_cleaned]
                                
                            email_bytes = partes[0].strip()
                            
                            # Validação ultrarápida com regex em bytes
                            if not EMAIL_REGEX_BYTES.match(email_bytes):
                                continue
                                
                            # Conversão para string apenas após validação de formato bem-sucedida
                            email = email_bytes.decode("utf-8", errors="ignore").lower().strip()
                            
                            if email in emails_cache:
                                continue
                            
                            # Alimenta cache local dinâmico e insere no buffer de persistência
                            emails_cache.add(email)
                            buffer_supabase.append({
                                "email_p": email,
                                "nome_p": deduzir_nome(email),
                                "origem_p": f"{file}/{member.name}"
                            })
                            
                            if len(buffer_supabase) >= TAMANHO_LOTE_UPLOAD:
                                enviar_lote_final_supabase(buffer_supabase)
                                salvar_checkpoint_supabase(member.name, numero_da_linha_atual)
                                buffer_supabase = []
                                
                        except Exception:
                            continue
                    checkpoint_line = 0
                    
    # Descarrega qualquer lead remanescente no fim do processamento
    if buffer_supabase:
        print(f"📥 Descarregando lote final residual de {len(buffer_supabase)} leads...")
        enviar_lote_final_supabase(buffer_supabase)
        salvar_checkpoint_supabase(member.name, numero_da_linha_atual)
    print("🏁 Processamento e extração concluídos globalmente sem dados pendentes!")

if __name__ == "__main__":
    try:
        cache_ram = carregar_emails_existentes_via_id()
        if baixar_torrent_seletivo(ARQUIVOS_ALVO):
            processar_arquivos_tar(cache_ram)
    except Exception as e:
        print(f"💥 Erro fatal no ciclo do script: {e}")
        raise
