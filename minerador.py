import re
import tarfile
import requests
import os
import sys
import time
import libtorrent as lt

sys.stdout.reconfigure(line_buffering=True)

SUPABASE_URL = "https://rbgbwqossenorypfrzln.supabase.co"
# Chave mantida diretamente no código conforme as suas diretrizes de configuração
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJiZ2J3cW9zc2Vub3J5cGZyemxuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg1NjA0NDcsImV4cCI6MjA3NDEzNjQ0N30.hD-pkTPJCM7ZmwQIyMpoBNJv-Hx6S1AvO9KGPZVzQjs"

MAGNET_LINK = "magnet:?xt=urn:btih:D136B1ADDE531F38311FBF43FB96FC26DF1A34CD&dn=Collection%20%232-%235%20%26%20Antipublic&tr=udp%3a%2f%2ftracker.coppersurfer.tk%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.leechers-paradise.org%3a6969%2fannounce&tr=http%3a%2f%2ft.nyaatracker.com%3a80%2fannounce&tr=http%3a%2f%2fopentracker.xyz%3a80%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce&tr=udp%3a%2f%2fopentracker.i2p.rocks%3a6969%2fannounce&tr=udp%3a%2f%2ftracker.openbittorrent.com%3a6969%2fannounce&tr=udp%3a%2f%2fexodus.desync.com%3a6969%2fannounce"

ARQUIVOS_ALVO = ["Trading Collection.tar.gz", "Collection #4_BTC combos.tar.gz"]
EMAIL_REGEX_BYTES = re.compile(rb'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE)

TAMANHO_SINC_DOWNLOAD = 150000  
CHUNK_SIZE = 16 * 1024 * 1024   
ARQUIVO_SAIDA_LOCAL = "emails_minerados_finais.txt"
TAMANHO_LOTE_UPLOAD = 2000 # Tamanho seguro para não engasgar o Supabase

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
                time.sleep(5)
                continue
            dados = r.json()
            if not dados:
                break
            for item in dados:
                if item.get("email"):
                    emails_cache.add(item["email"].strip().lower())
            total_baixado += len(dados)
            last_id = dados[-1]["id"]
            
            if total_baixado - ult_print >= 300000:
                print(f"📥 [Cache-RAM] Total sincronizado: {total_baixado:,} e-mails.", flush=True)
                ult_print = total_baixado
                
        except Exception:
            time.sleep(10)
            
    print(f"✅ Sincronização Concluída! Cache RAM com {len(emails_cache):,} registos prontos.", flush=True)
    return emails_cache

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
        if progresso_atual - ult_progresso >= 1.0:
            print(f"📥 Download: {progresso_atual:.1f}% | Velocidade: {s.download_rate/1024/1024:.2f} MB/s", flush=True)
            ult_progresso = progresso_atual
        completos = sum(1 for idx, f in enumerate(tor_info.files()) if any(alvo in f.path for alvo in arquivos_alvo) and handle.file_progress()[idx] >= f.size)
        if completos == len(arquivos_alvo):
            break
        time.sleep(10)
    print("✅ Alvos guardados localmente.", flush=True)
    return True

def deduzir_nome(email: str) -> str:
    username = email.split("@")[0]
    username = re.sub(r"\d+", "", username)
    username = re.sub(r"[_.-]+", " ", username).strip()
    return username.title() if username else "Trader Lead"

def processar_arquivos_tar(emails_cache):
    total_encontrados_global = 0
    total_novos_salvos = 0
    
    with open(ARQUIVO_SAIDA_LOCAL, "a", encoding="utf-8") as f_out:
        for root, dirs, files in os.walk("."):
            for file in files:
                if not (file.endswith(".tar.gz") and any(alvo in file for alvo in ARQUIVOS_ALVO)):
                    continue
                alvo_tar = os.path.join(root, file)
                print(f"\n📦 [Fase 1: Extração Extrema] Analisando: {alvo_tar}", flush=True)
                
                with tarfile.open(alvo_tar, "r|gz", errorlevel=0) as tar:
                    for member in tar:
                        if not member.isfile() or not (member.name.endswith(".txt") or member.name.endswith(".csv")):
                            continue
                        f = tar.extractfile(member)
                        if not f:
                            continue
                        
                        bytes_lidos = 0
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
                            if mb - ult_print_mb >= 100.0:
                                print(f"📂 {mb:.1f} MB lidos | Novos únicos salvos: {total_novos_salvos:,}", flush=True)
                                ult_print_mb = mb
                            
                            for email_b in encontrados:
                                try:
                                    email = email_b.decode("utf-8", errors="ignore").lower().strip()
                                    if email not in emails_cache:
                                        emails_cache.add(email)
                                        # Guarda e-mail e origem separados por vírgula
                                        f_out.write(f"{email},{file}/{member.name}\n")
                                        total_novos_salvos += 1
                                except Exception:
                                    continue
    print(f"🏁 [Fim da Fase 1] Extração concluída. Salvos localmente: {total_novos_salvos:,}", flush=True)

def upload_em_massa_supabase():
    if not os.path.exists(ARQUIVO_SAIDA_LOCAL):
        print("Nenhum ficheiro novo para enviar.", flush=True)
        return

    print("\n🚀 [Fase 2: Sincronização Final] A iniciar envio para o Supabase...", flush=True)
    url = f"{SUPABASE_URL}/rest/v1/rpc/importar_leads_flash"
    lote_atual = []
    total_enviados = 0

    with open(ARQUIVO_SAIDA_LOCAL, "r", encoding="utf-8") as f_in:
        linhas = f_in.readlines()

    for linha in linhas:
        partes = linha.strip().split(',')
        if len(partes) >= 2:
            email = partes[0]
            origem = partes[1]
            lote_atual.append({
                "email_p": email,
                "nome_p": deduzir_nome(email),
                "origem_p": origem
            })

        if len(lote_atual) >= TAMANHO_LOTE_UPLOAD:
            _enviar_lote_com_retry(url, lote_atual)
            total_enviados += len(lote_atual)
            print(f"📤 Uploaded: {total_enviados:,} / {len(linhas):,} leads...", flush=True)
            lote_atual = []

    # Envia o restante que ficou no último lote
    if lote_atual:
        _enviar_lote_com_retry(url, lote_atual)
        total_enviados += len(lote_atual)
        
    print(f"✅ [Sucesso] Sincronização concluída! {total_enviados} leads injetados no Supabase.", flush=True)

def _enviar_lote_com_retry(url, lote):
    tentativas_falhas = 0
    while True:
        try:
            r = http_session.post(url, json={"lote_dados": lote}, timeout=60.0)
            if r.status_code == 200:
                return # Lote enviado com sucesso, sai do loop
            
            print(f"⚠️ Aviso da API (Status {r.status_code}). Aguardando para tentar novamente...", flush=True)
            tentativas_falhas += 1
            time.sleep(10 * tentativas_falhas) # Espera progressiva (10s, 20s, 30s)
            
        except Exception as e:
            tentativas_falhas += 1
            print(f"⚠️ Erro de rede: {e}. Retentando...", flush=True)
            time.sleep(10 * tentativas_falhas)

if __name__ == "__main__":
    try:
        cache_ram = carregar_emails_existentes_via_id()
        if baixar_torrent_seletivo(ARQUIVOS_ALVO):
            processar_arquivos_tar(cache_ram)
            upload_em_massa_supabase() # Dispara a Fase 2 automaticamente após a Fase 1
    except Exception as e:
        print(f"💥 Erro Fatal: {e}", flush=True)
        raise
