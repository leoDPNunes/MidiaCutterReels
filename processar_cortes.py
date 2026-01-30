import os, subprocess, re, time, json, argparse
from datetime import datetime
import psutil
import GPUtil

# --- CONFIGURAÇÕES ---
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive"
MAX_GPU_TEMP = 80
COOL_DOWN_TIME = 10

def obter_telemetria():
    cpu = psutil.cpu_percent()
    gpu = GPUtil.getGPUs()[0] if GPUtil.getGPUs() else None
    return cpu, (gpu.temperature if gpu else 0)

def criar_caminho_hierarquico(data_video, titulo_video):
    ano = str(data_video.year)
    mes = data_video.strftime("%m_%B")
    categoria = "EBD" if any(x in titulo_video.upper() for x in ["EBD", "AULA"]) else "Culto"
    return os.path.join(ano, mes, categoria).replace("\\", "/")

def limpar_url(url: str) -> str:
    # Aceita url normal e também markdown do tipo: [http://x](http://x)
    m = re.search(r"\((https?://[^)]+)\)", url or "")
    if m:
        return m.group(1).strip()
    return (url or "").strip().strip('"').strip("'")

def ler_event_payload(event_path: str):
    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)

    payload = event.get("client_payload", {})
    url = limpar_url(payload.get("url"))
    relatorio = payload.get("relatorio") or ""
    return url, relatorio

def realizar_corte(url, inicio, duracao, nome_saida, destino_local):
    if not os.path.exists(destino_local):
        os.makedirs(destino_local)

    caminho_arquivo = os.path.join(destino_local, f"{nome_saida}.mp4")

    cmd_url = f'yt-dlp -g -f "bestvideo+bestaudio/best" "{url}"'
    urls = subprocess.check_output(cmd_url, shell=True).decode().split('\n')

    # yt-dlp -g geralmente retorna 2 linhas (video e audio) no caso bestvideo+bestaudio
    v_url = urls[0].strip()
    a_url = urls[1].strip() if len(urls) > 1 else ""

    if not v_url or not a_url:
        raise RuntimeError("yt-dlp não retornou URLs de vídeo/áudio como esperado.")

    ffmpeg_cmd = [
        'ffmpeg', '-y',
        '-ss', inicio, '-t', duracao, '-i', v_url,
        '-ss', inicio, '-t', duracao, '-i', a_url,
        '-map', '0:v', '-map', '1:a',
        '-c', 'copy',
        caminho_arquivo
    ]
    subprocess.run(ffmpeg_cmd, check=True)

def iniciar_processamento(event_path: str):
    # 1) Ler URL/relatorio do repository_dispatch via github.event_path [web:154][web:155]
    try:
        url_youtube, relatorio = ler_event_payload(event_path)
        if not url_youtube:
            raise ValueError("client_payload.url vazio")
        if not relatorio.strip():
            raise ValueError("client_payload.relatorio vazio")
    except Exception as e:
        print(f"❌ Erro ao ler payload do evento: {e}")
        time.sleep(10)
        return

    start_time = datetime.now()
    log_name = f"historico_{start_time.strftime('%d_%m_%Y_%H_%M_%S')}.md"
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_path = os.path.join(LOG_DIR, log_name)

    print(f"🔍 Analisando vídeo: {url_youtube}")
    info_raw = subprocess.check_output(f'yt-dlp --dump-json "{url_youtube}"', shell=True)
    video_info = json.loads(info_raw)

    data_upload = datetime.strptime(video_info['upload_date'], '%Y%m%d')
    titulo_video = video_info['title']

    rel_path = criar_caminho_hierarquico(data_upload, titulo_video)
    pasta_local_final = os.path.join(BASE_PATH, rel_path)
    pasta_drive_final = f"{DRIVE_NAME}:/Cortes_Midia_Igreja/{rel_path}"

    # 2) Regex adaptado ao seu relatório (uma entrada por linha)
    # Ex.: [00:08:34] até (Duração: 01:10) Hook: "A amargura é o veneno..."
    # Captura: inicio (HH:MM:SS), duracao (MM:SS), titulo (resto da linha)
    padrao = r'^\[(\d{2}:\d{2}:\d{2})\]\s+até\s+\(Duração:\s+(\d{2}:\d{2})\)\s*(?:Hook:\s*)?(.*)$'
    matches = re.findall(padrao, relatorio, flags=re.MULTILINE)  # MULTILINE faz ^/$ funcionarem por linha [web:154]
    total = len(matches)

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"# Relatório: {titulo_video}\n- Total: {total}\n\n| # | Corte | Status | CPU | GPU |\n|---|---|---|---|---|\n")

        for i, (inicio, duracao, titulo) in enumerate(matches, 1):
            cpu, g_temp = obter_telemetria()
            if g_temp > MAX_GPU_TEMP:
                print(f"🌡️ Resfriando GPU: {g_temp}°C...")
                time.sleep(30)

            # Mantém nome baseado no título do hook (como você pediu)
            titulo_limpo = (titulo or "").strip()
            if not titulo_limpo:
                titulo_limpo = f"corte_{i}"

            nome_slug = re.sub(r'[^\w\s-]', '', titulo_limpo).replace(' ', '_')[:40]
            nome_final = f"{nome_slug}__{inicio.replace(':', '-')}"
            print(f"[{ (i/total)*100 :.1f}%] ({i}/{total}) Cortando: {titulo_limpo}")

            try:
                realizar_corte(url_youtube, inicio, f"00:{duracao}", nome_final, pasta_local_final)
                status = "✅ OK"
            except Exception as e:
                status = "❌ Erro"

            log.write(f"| {i} | {titulo_limpo} | {status} | {cpu}% | {g_temp}°C |\n")
            time.sleep(COOL_DOWN_TIME)

        print("\n☁️ Sincronizando com Google Drive...")
        subprocess.run(['rclone', 'copy', pasta_local_final, pasta_drive_final], check=True)

    print(f"\n✅ Concluído! Log: {log_name}")
    # Não use input() em ambiente de CI senão pode travar aguardando stdin. [web:159]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", required=True)
    args = parser.parse_args()
    iniciar_processamento(args.event_path)
