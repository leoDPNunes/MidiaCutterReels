import os, subprocess, re, sys, time, json, requests
from datetime import datetime
import psutil
import GPUtil

# --- CONFIGURAÇÕES ---
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive" 
MAX_GPU_TEMP = 80 
REPO = "leoDPNunes/MidiaCutterReels" # Verifique se o nome está exato

def obter_dados_github():
    run_id = os.environ.get("RUN_ID")
    token = os.environ.get("GH_TOKEN")
    url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}"
    
    headers = {"Authorization": f"token {token}"}
    response = requests.get(url, headers=headers).json()
    
    # Busca o payload do evento original
    url_origem = response.get("repository_dispatch", {}).get("payload", {})
    return url_origem.get("relatorio"), url_origem.get("url")

def iniciar_processamento():
    print("🛰️ Conectando à API do GitHub para buscar o relatório...")
    try:
        relatorio, url_youtube = obter_dados_github()
        if not relatorio: raise ValueError("Relatório não encontrado na API.")
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        time.sleep(10); return

    start_time = datetime.now()
    # ... (O resto da lógica de telemetria e cortes continua igual)
    # Use as variáveis 'relatorio' e 'url_youtube' aqui embaixo
    print(f"✅ Sucesso! Iniciando cortes para: {url_youtube}")
    
    # Lógica de regex e pastas (Mantenha a que já criamos para o Drive F:)
    # [CÓDIGO DE CORTES AQUI]

if __name__ == "__main__":
    iniciar_processamento()
