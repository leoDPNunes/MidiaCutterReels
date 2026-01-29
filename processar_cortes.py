import os, subprocess, re, sys, time, json
from datetime import datetime
import psutil
import GPUtil

# --- CONFIGURAÇÕES DE DIRETÓRIO ---
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive" # Nome configurado no rclone config

# --- LIMITES TÉRMICOS (CONTROLE RJ) ---
MAX_CPU_TEMP = 85  
MAX_GPU_TEMP = 80  
COOL_DOWN_TIME = 10 

def obter_telemetria():
    cpu_usage = psutil.cpu_percent()
    gpu = GPUtil.getGPUs()[0] if GPUtil.getGPUs() else None
    gpu_temp = gpu.temperature if gpu else 0
    return cpu_usage, gpu_temp

def criar_caminho_hierarquico(data_video, tipo_video):
    ano = str(data_video.year)
    mes = data_video.strftime("%m_%B")
    categoria = "EBD" if "EBD" in tipo_video.upper() or "AULA" in tipo_video.upper() else "Culto"
    
    # Caminho relativo para usar tanto localmente quanto no Drive
    caminho_relativo = os.path.join(ano, mes, categoria).replace("\\", "/")
    return caminho_relativo

def realizar_corte(url, inicio, duracao, nome_saida, destino_local):
    if not os.path.exists(destino_local): os.makedirs(destino_local)
    caminho_arquivo = os.path.join(destino_local, f"{nome_saida}.mp4")
    
    cmd_url = f'yt-dlp -g -f "bestvideo+bestaudio/best" {url}'
    urls = subprocess.check_output(cmd_url, shell=True).decode().split('\n')
    
    ffmpeg_cmd = [
        'ffmpeg', '-y', '-ss', inicio, '-t', duracao, '-i', urls[0].strip(),
        '-ss', inicio, '-t', duracao, '-i', urls[1].strip(),
        '-map', '0:v', '-map', '1:a', '-c', 'copy', caminho_arquivo
    ]
    subprocess.run(ffmpeg_cmd, check=True)
    return caminho_arquivo

def iniciar_processamento(relatorio, url_youtube):
    start_time = datetime.now()
    log_name = f"historico_{start_time.strftime('%d_%m_%Y_%H_%M_%S')}.md"
    if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
    log_path = os.path.join(LOG_DIR, log_name)

    # Obter Metadados do YouTube
    print("🔍 Analisando metadados do vídeo...")
    info_raw = subprocess.check_output(f'yt-dlp --dump-json {url_youtube}', shell=True)
    video_info = json.loads(info_raw)
    data_upload = datetime.strptime(video_info['upload_date'], '%Y%m%d')
    titulo_video = video_info['title']
    
    # Gerar estrutura de pastas
    rel_path = criar_caminho_hierarquico(data_upload, titulo_video)
    pasta_local_final = os.path.join(BASE_PATH, rel_path)
    pasta_drive_final = f"{DRIVE_NAME}:/Cortes_Midia_Igreja/{rel_path}"

    padrao = r"\[(\d{2}:\d{2}:\d{2})\].*?Duração:\s(\d{2}:\d{2})\)\nHook:\s\"(.*?)\""
    matches = re.findall(padrao, relatorio, re.DOTALL)
    total_tarefas = len(matches)

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"# Relatório de Processamento\n")
        log.write(f"- **Data de Início:** {start_time.strftime('%d/%m/%Y %H:%M:%S')}\n")
        log.write(f"- **Vídeo Original:** {titulo_video} ({url_youtube})\n")
        log.write(f"- **Estrutura de Pastas:** {rel_path}\n")
        log.write(f"- **Total de Cortes:** {total_tarefas}\n\n")
        log.write("| # | Corte | Status Local | CPU | GPU Temp |\n|---|---|---|---|---|\n")

        print(f"\n🚀 {total_tarefas} cortes identificados para {rel_path}. Iniciando...")

        for i, (inicio, duracao, titulo) in enumerate(matches, 1):
            cpu, g_temp = obter_telemetria()
            
            # Gestão Térmica
            if g_temp > MAX_GPU_TEMP:
                print(f"🌡️ Alerta Térmico: {g_temp}°C. Pausando 30s...")
                time.sleep(30)

            perc = (i / total_tarefas) * 100
            nome_slug = re.sub(r'[^\w\s-]', '', titulo).replace(' ', '_')[:40]
            nome_final = f"{nome_slug}__{inicio.replace(':', '-')}"

            print(f"[{perc:.1f}%] ({i}/{total_tarefas}) Processando: {titulo}")
            
            try:
                realizar_corte(url_youtube, inicio, f"00:{duracao}", nome_final, pasta_local_final)
                status = "✅ OK"
            except Exception as e:
                status = f"❌ Erro"
                print(f"Erro no corte {i}: {e}")

            log.write(f"| {i} | {titulo} | {status} | {cpu}% | {g_temp}°C |\n")
            time.sleep(COOL_DOWN_TIME)

        # Sincronização com Google Drive
        print(f"\n☁️ Sincronizando com o Drive: {pasta_drive_final}...")
        log.write(f"\n## Sincronização Cloud\n")
        try:
            # Rclone copy cria as pastas automaticamente se não existirem
            subprocess.run(['rclone', 'copy', pasta_local_final, pasta_drive_final], check=True)
            log.write(f"- **Status Upload:** ✅ Concluído com sucesso.\n")
            print("✅ Upload finalizado!")
        except Exception as e:
            log.write(f"- **Status Upload:** ❌ Falha: {e}\n")
            print(f"❌ Erro no upload: {e}")

        end_time = datetime.now()
        log.write(f"\n- **Fim do Processo:** {end_time.strftime('%H:%M:%S')}\n")
        log.write(f"- **Duração Total:** {end_time - start_time}\n")

    print(f"\n✨ Tudo pronto! Log gerado: {log_name}")
    input("\nPressione qualquer tecla para encerrar...")

if __name__ == "__main__":
    # Agora buscamos das variáveis de ambiente configuradas no YAML
    relatorio_env = os.environ.get("RELATORIO_BRUTO")
    url_env = os.environ.get("URL_YOUTUBE")
    
    if relatorio_env and url_env:
        iniciar_processamento(relatorio_env, url_env)
    else:
        print("❌ Erro: Dados não recebidos corretamente do GitHub.")
        input("Pressione qualquer tecla para fechar...")
