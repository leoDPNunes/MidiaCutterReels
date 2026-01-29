import os, subprocess, re, sys, time, json
from datetime import datetime
import psutil
import GPUtil

# --- CONFIGURAÇÕES ---
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive" 
MAX_GPU_TEMP = 80  

def obter_telemetria():
    cpu = psutil.cpu_percent()
    gpu = GPUtil.getGPUs()[0] if GPUtil.getGPUs() else None
    return cpu, (gpu.temperature if gpu else 0)

# ... (funções criar_caminho_hierarquico e realizar_corte permanecem iguais)

def iniciar_processamento():
    # Lendo dos arquivos temporários criados pelo GitHub
    try:
        with open("url_temp.txt", "r") as f: url_youtube = f.read().strip()
        with open("relatorio_temp.txt", "r", encoding="utf-8") as f: relatorio = f.read()
    except Exception as e:
        print(f"❌ Erro ao ler arquivos temporários: {e}")
        return

    start_time = datetime.now()
    log_name = f"historico_{start_time.strftime('%d_%m_%Y_%H_%M_%S')}.md"
    # ... (resto da lógica de processamento e rclone copy)

if __name__ == "__main__":
    iniciar_processamento()
