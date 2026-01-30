import os, subprocess, re, time, json, argparse, hashlib
from datetime import datetime
import psutil
import GPUtil

# =========================
# CONFIG DO SEU PROJETO
# =========================
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive"
MAX_GPU_TEMP = 80
COOL_DOWN_TIME = 10

DOWNLOAD_CACHE_DIR = os.path.join(BASE_PATH, "_cache_downloads").replace("\\", "/")

# Se quiser reduzir peso: limite altura (ex: "bv*[height<=1080][ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best")
YTDLP_FORMAT_FULL = 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best'

# =========================
# CONFIG "TRAVADA" DO YT-DLP
# =========================
# Cookies: yt-dlp espera arquivo no formato Netscape (header "# Netscape HTTP Cookie File"). [web:469]
YTDLP_COOKIES_TXT_PATH = r"D:\secrets\yt_cookies.txt"
YTDLP_COOKIES_ARGS = ["--cookies", YTDLP_COOKIES_TXT_PATH]  # [web:347]

# EJS: habilita download do solver via GitHub quando necessário. [web:393]
YTDLP_EJS_ARGS = ["--remote-components", "ejs:github"]  # [web:393]

# JS runtime: aponta node explicitamente (necessário no seu setup com nvm). [web:325]
YTDLP_NODE_EXE = r"C:\nvm4w\nodejs\node.exe"
YTDLP_JS_ARGS = ["--js-runtimes", f"node:{YTDLP_NODE_EXE}"]  # [web:325]

# Rede: IPv4 + retries (fragment e backoff). [web:325]
YTDLP_NET_ARGS = [
    "-4",
    "--retries", "10",
    "--fragment-retries", "10",
    "--retry-sleep", "exp=1:20:2",
]  # [web:325]

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
    s = (url or "").strip().strip('"').strip("'")
    m = re.search(r"\((https?://[^)]+)\)", s)
    return m.group(1).strip() if m else s

def ler_event_payload(event_path: str):
    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)
    payload = event.get("client_payload", {})
    url = limpar_url(payload.get("url"))
    relatorio = payload.get("relatorio") or ""
    return url, relatorio

def extrair_cortes(relatorio: str):
    linhas = relatorio.splitlines()
    cortes = []
    re_tempo = re.compile(r"^\[(\d{2}:\d{2}:\d{2})\]\s+até\s+\(Duração:\s+(\d{2}:\d{2})\)\s*(.*)$")

    i = 0
    while i < len(linhas):
        m = re_tempo.match(linhas[i].strip())
        if not m:
            i += 1
            continue

        inicio = m.group(1)
        duracao = m.group(2)
        resto_mesma_linha = (m.group(3) or "").strip()

        titulo = ""
        if resto_mesma_linha:
            titulo = resto_mesma_linha
        else:
            j = i + 1
            while j < len(linhas):
                t = (linhas[j] or "").strip()
                if not t or t.lower().startswith("categoria:"):
                    j += 1
                    continue
                titulo = t
                break

        titulo = re.sub(r"^Hook:\s*", "", titulo, flags=re.IGNORECASE).strip()
        titulo = titulo.strip('"').strip("'").strip()
        if not titulo:
            titulo = f"corte_{len(cortes)+1}"

        cortes.append((inicio, duracao, titulo))
        i += 1

    return cortes

def hhmmss_to_seconds(hhmmss: str) -> int:
    h, m, s = hhmmss.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)

def mmss_to_seconds(mmss: str) -> int:
    m, s = mmss.split(":")
    return int(m) * 60 + int(s)

def seconds_to_hhmmss(sec: int) -> str:
    if sec < 0:
        sec = 0
    h = sec // 3600
    sec %= 3600
    m = sec // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def run_cmd(cmd, check=True):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=False)
    if check and p.returncode != 0:
        raise RuntimeError(p.stdout)
    return p.returncode, p.stdout

def is_403(output: str) -> bool:
    if not output:
        return False
    return ("HTTP Error 403" in output) or ("403 Forbidden" in output) or ("status code 403" in output)

def cache_key_for_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def ytdlp_base_cmd():
    # padrão pedido por você: sempre construir assim
    return ["yt-dlp", *YTDLP_COOKIES_ARGS, *YTDLP_EJS_ARGS, *YTDLP_JS_ARGS, *YTDLP_NET_ARGS]

def garantir_download_inteiro(url_youtube: str, log_fn=print) -> str:
    os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)
    key = cache_key_for_url(url_youtube)
    outtmpl = os.path.join(DOWNLOAD_CACHE_DIR, f"{key}.%(ext)s")
    mp4_path = os.path.join(DOWNLOAD_CACHE_DIR, f"{key}.mp4")

    if os.path.exists(mp4_path):
        log_fn(f"📦 Cache hit: usando vídeo já baixado ({mp4_path})")
        return mp4_path

    log_fn("⬇️ Baixando vídeo inteiro (cache) via yt-dlp...")
    cmd = [
        *ytdlp_base_cmd(),
        "-f", YTDLP_FORMAT_FULL,
        "--merge-output-format", "mp4",
        "-o", outtmpl,
        url_youtube
    ]
    rc, out = run_cmd(cmd, check=False)
    if rc != 0:
        raise RuntimeError(out)

    if not os.path.exists(mp4_path):
        for fn in os.listdir(DOWNLOAD_CACHE_DIR):
            if fn.startswith(key + "."):
                return os.path.join(DOWNLOAD_CACHE_DIR, fn)
        raise RuntimeError("Download inteiro finalizou, mas arquivo não encontrado no cache.")

    return mp4_path

def cortar_local(video_path: str, inicio: str, duracao_mmss: str, saida_path: str):
    duracao = f"00:{duracao_mmss}"
    os.makedirs(os.path.dirname(saida_path), exist_ok=True)

    cmd = [
        "ffmpeg", "-y",
        "-ss", inicio,
        "-t", duracao,
        "-i", video_path,
        "-c", "copy",
        saida_path
    ]
    rc, out = run_cmd(cmd, check=False)
    if rc != 0:
        raise RuntimeError(out)

def tentar_baixar_trecho(url_youtube: str, inicio_hhmmss: str, duracao_mmss: str, saida_path: str, log_fn=print):
    os.makedirs(os.path.dirname(saida_path), exist_ok=True)
    start = inicio_hhmmss
    end = seconds_to_hhmmss(hhmmss_to_seconds(inicio_hhmmss) + mmss_to_seconds(duracao_mmss))
    section = f"*{start}-{end}"

    cmd = [
        *ytdlp_base_cmd(),
        "-f", YTDLP_FORMAT_FULL,
        "--download-sections", section,
        "--force-keyframes-at-cuts",
        "--merge-output-format", "mp4",
        "-o", saida_path,
        url_youtube
    ]
    log_fn(f"🎯 Tentando baixar somente o trecho ({section})...")
    rc, out = run_cmd(cmd, check=False)

    if rc == 0 and os.path.exists(saida_path) and os.path.getsize(saida_path) > 0:
        return True, out
    return False, out

def realizar_corte(url_youtube, inicio, duracao_mmss, nome_saida, destino_local, log_fn=print):
    os.makedirs(destino_local, exist_ok=True)
    saida_path = os.path.join(destino_local, f"{nome_saida}.mp4")

    # B) baixar trecho
    ok, out_trecho = tentar_baixar_trecho(url_youtube, inicio, duracao_mmss, saida_path, log_fn=log_fn)
    if ok:
        return True, "B", out_trecho

    if is_403(out_trecho):
        log_fn("⚠️ Trecho falhou com 403: YouTube recusou/expirou/ bloqueou a requisição.")
    else:
        log_fn("⚠️ Trecho falhou (não-403).")

    # A) fallback: baixar inteiro e cortar local
    log_fn("🔁 Fallback: baixando vídeo inteiro (cache) e cortando localmente...")
    try:
        video_local = garantir_download_inteiro(url_youtube, log_fn=log_fn)
        cortar_local(video_local, inicio, duracao_mmss, saida_path)
        return True, "A", out_trecho
    except Exception as e:
        out_full = str(e)
        if is_403(out_trecho) and is_403(out_full):
            log_fn("❌ Ambos falharam com 403: provável recusa/bloqueio do YouTube ou assinatura/URL temporária expirada/recusada.")
        raise

def iniciar_processamento(event_path: str):
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
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, log_name)

    print(f"🔍 Analisando vídeo: {url_youtube}")

    # dump-json usando a mesma base cmd travada
    cmd_info = [*ytdlp_base_cmd(), "--dump-json", url_youtube]
    rc, out = run_cmd(cmd_info, check=False)
    if rc != 0:
        print(out)
        time.sleep(5)
        return
    video_info = json.loads(out)

    data_upload = datetime.strptime(video_info['upload_date'], '%Y%m%d')
    titulo_video = video_info['title']

    rel_path = criar_caminho_hierarquico(data_upload, titulo_video)
    pasta_local_final = os.path.join(BASE_PATH, rel_path)
    pasta_drive_final = f"{DRIVE_NAME}:/Cortes_Midia_Igreja/{rel_path}"

    cortes = extrair_cortes(relatorio)
    total = len(cortes)
    print(f"✂️ Cortes encontrados: {total}")

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"# Relatório: {titulo_video}\n- Total: {total}\n\n| # | Corte | Modo | Status | CPU | GPU |\n|---|---|---|---|---|---|\n")

        for idx, (inicio, duracao, titulo) in enumerate(cortes, 1):
            cpu, g_temp = obter_telemetria()
            if g_temp > MAX_GPU_TEMP:
                print(f"🌡️ Resfriando GPU: {g_temp}°C...")
                time.sleep(30)

            nome_slug = re.sub(r"[^\w\s-]", "", titulo).replace(" ", "_")[:40]
            nome_final = f"{nome_slug}__{inicio.replace(':', '-')}"
            print(f"[{(idx/total)*100:.1f}%] ({idx}/{total}) Cortando: {titulo}")

            modo = "-"
            status = "❌ Erro"
            ytdlp_tail = ""

            try:
                ok, modo, out_trecho = realizar_corte(
                    url_youtube=url_youtube,
                    inicio=inicio,
                    duracao_mmss=duracao,
                    nome_saida=nome_final,
                    destino_local=pasta_local_final,
                    log_fn=print
                )
                status = "✅ OK"
                ytdlp_tail = (out_trecho or "")[-2000:]
            except Exception as e:
                err = str(e)
                print(f"Erro no corte {idx}: {err}")
                ytdlp_tail = err[-2000:]

            log.write(f"| {idx} | {titulo} | {modo} | {status} | {cpu}% | {g_temp}°C |\n")
            if ytdlp_tail:
                log.write(f"\n<details><summary>Debug yt-dlp/ffmpeg corte #{idx}</summary>\n\n```\n{ytdlp_tail}\n```\n</details>\n\n")

            time.sleep(COOL_DOWN_TIME)

        print("\n☁️ Sincronizando com Google Drive...")
        subprocess.run(['rclone', 'copy', pasta_local_final, pasta_drive_final], check=True)

    print(f"\n✅ Concluído! Log: {log_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", required=True)
    args = parser.parse_args()
    iniciar_processamento(args.event_path)
