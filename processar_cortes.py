import os, subprocess, re, sys, time, json
from datetime import datetime
import psutil
import GPUtil

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive" 

# --- LIMITES TÉRMICOS (CONTROLE RJ) ---
MAX_CPU_TEMP = 85  
MAX_GPU_TEMP = 80  
COOL_DOWN_TIME = 10 

def obter_telemetria():
    cpu_usage = psutil.cpu_percent()
    gpu = GPUtil.getGPUs()[0] if GPUtil.getGPUs() else None
    gpu_temp = gpu.temperature if gpu else 0
    return cpu_usage, gpu_temp

def criar_caminho_hierarquico(data_video, titulo_video):
    ano = str(data_video.year)
    mes = data_video.strftime("%m_%B")
    categoria = "EBD" if any(x in titulo_video.upper() for x in ["EBD", "AULA"]) else "Culto"
    return os.path.join(ano, mes, categoria).replace("\\", "/")

def realizar_corte(url, inicio, duracao, nome_saida, destino_local):
    if not os.path.exists(destino_local): os.makedirs(destino_local)
    caminho_arquivo = os.path.join(destino_local, f"{nome_saida}.mp4")
    
    cmd_url = f'yt-dlp -g -f "bestvideo+bestaudio/best" "{url}"'
    urls = subprocess.check_output(cmd_url, shell=True).decode().split('\n')
    
    ffmpeg_cmd = [
        'ffmpeg', '-y', '-ss', inicio, '-t', duracao, '-i', urls[0].strip(),
        '-ss', inicio, '-t', duracao, '-i', urls[1].strip(),
        '-map', '0:v', '-map', '1:a', '-c', 'copy', caminho_arquivo
    ]
    subprocess.run(ffmpeg_cmd, check=True)

def iniciar_processamento(relatorio, url_youtube):
    start_time = datetime.now()
    log_name = f"historico_{start_time.strftime('%d_%m_%Y_%H_%M_%S')}.md"
    if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
    log_path = os.path.join(LOG_DIR, log_name)

    print(f"🔍 Analisando vídeo: {url_youtube}")
    info_raw = subprocess.check_output(f'yt-dlp --dump-json "{url_youtube}"', shell=True)
    video_info = json.loads(info_raw)
    data_upload = datetime.strptime(video_info['upload_date'], '%Y%m%d')
    titulo_video = video_info['title']
    
    rel_path = criar_caminho_hierarquico(data_upload, titulo_video)
    pasta_local_final = os.path.join(BASE_PATH, rel_path)
    pasta_drive_final = f"{DRIVE_NAME}:/Cortes_Midia_Igreja/{rel_path}"

    padrao = r"\[(\d{2}:\d{2}:\d{2})\].*?Duração:\s(\d{2}:\d{2})\)\nHook:\s\"(.*?)\""
    matches = re.findall(padrao, relatorio, re.DOTALL)
    total_tarefas = len(matches)

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"# Relatório: {titulo_video}\n- **Início:** {start_time}\n- **Total:** {total_tarefas}\n\n")
        log.write("| # | Corte | Status | CPU | GPU Temp |\n|---|---|---|---|---|\n")

        for i, (inicio, duracao, titulo) in enumerate(matches, 1):
            cpu, g_temp = obter_telemetria()
            if g_temp > MAX_GPU_TEMP:
                print(f"🌡️ Resfriando GPU ({g_temp}°C)...")
                time.sleep(30)

            nome_slug = re.sub(r'[^\w\s-]', '', titulo).replace(' ', '_')[:40]
            nome_final = f"{nome_slug}__{inicio.replace(':', '-')}"
            print(f"[{ (i/total_tarefas)*100 :.1f}%] ({i}/{total_tarefas}) {titulo}")
            
            try:
                realizar_corte(url_youtube, inicio, f"00:{duracao}", nome_final, pasta_local_final)
                status = "✅ OK"
            except Exception as e:
                status = "❌ Erro"

            log.write(f"| {i} | {titulo} | {status} | {cpu}% | {g_temp}°C |\n")
            time.sleep(COOL_DOWN_TIME)

        print("\n☁️ Sincronizando com Google Drive...")
        subprocess.run(['rclone', 'copy', pasta_local_final, pasta_drive_final], check=True)

    print(f"\n✅ Concluído! Log: {log_name}")
    input("Pressione qualquer tecla para sair...")

if __name__ == "__main__":
    # Captura das variáveis enviadas pelo main.yml
    r_env = os.environ.get("RELATORIO_BRUTO")
    u_env = os.environ.get("URL_YOUTUBE")
    if r_env and u_env:
        iniciar_processamento(r_env, u_env)
    else:
        print("❌ Erro: Variáveis de ambiente ausentes.")
        time.sleep(10)
