import os, subprocess, re, time, json, argparse, hashlib, glob
from datetime import datetime
import sys

# =========================
# FIX: stdout/stderr UTF-8 (evita UnicodeEncodeError cp1252 no runner)
# =========================
def force_utf8_stdio():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

force_utf8_stdio()

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

# Você pode trocar por um format que limite resolução se quiser
YTDLP_FORMAT_FULL = 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best'

# =========================
# CONFIG "TRAVADA" DO YT-DLP
# =========================
YTDLP_COOKIES_TXT_PATH = r"D:\secrets\yt_cookies.txt"
YTDLP_COOKIES_ARGS = ["--cookies", YTDLP_COOKIES_TXT_PATH]

YTDLP_EJS_ARGS = ["--remote-components", "ejs:github"]

YTDLP_NODE_EXE = r"C:\nvm4w\nodejs\node.exe"
YTDLP_JS_ARGS = ["--js-runtimes", f"node:{YTDLP_NODE_EXE}"]

YTDLP_NET_ARGS = [
    "-4",
    "--retries", "10",
    "--fragment-retries", "10",
    "--retry-sleep", "exp=1:20:2",
]

# =========================
# UPLOAD RETRY (EXTRA)
# =========================
UPLOAD_MAX_ATTEMPTS = 2          # 1 tentativa + 1 retry extra
UPLOAD_RETRY_SLEEP_SEC = 8       # espera entre tentativas (pra rate limit/intermitência)

# =========================
# LOG / STATUS
# =========================
def log_step(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def fmt_td(seconds: float) -> str:
    seconds = int(round(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"

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

def cache_key_for_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def ytdlp_base_cmd():
    return ["yt-dlp", *YTDLP_COOKIES_ARGS, *YTDLP_EJS_ARGS, *YTDLP_JS_ARGS, *YTDLP_NET_ARGS]

def garantir_download_inteiro(url_youtube: str) -> str:
    os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)
    key = cache_key_for_url(url_youtube)
    outtmpl = os.path.join(DOWNLOAD_CACHE_DIR, f"{key}.%(ext)s")
    mp4_path = os.path.join(DOWNLOAD_CACHE_DIR, f"{key}.mp4")

    if os.path.exists(mp4_path):
        log_step(f"Cache hit (vídeo inteiro): {mp4_path}")
        return mp4_path

    log_step("Baixando vídeo inteiro (cache) via yt-dlp...")
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

def tentar_baixar_trecho(url_youtube: str, inicio_hhmmss: str, duracao_mmss: str, saida_path: str):
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
    rc, out = run_cmd(cmd, check=False)

    if rc == 0 and os.path.exists(saida_path) and os.path.getsize(saida_path) > 0:
        return True, out, section
    return False, out, section

def realizar_corte(url_youtube, inicio, duracao_mmss, nome_saida, destino_local):
    os.makedirs(destino_local, exist_ok=True)
    saida_path = os.path.join(destino_local, f"{nome_saida}.mp4")

    ok, out_trecho, section = tentar_baixar_trecho(url_youtube, inicio, duracao_mmss, saida_path)
    if ok:
        return True, "B", out_trecho, saida_path, section

    # Fallback A
    video_local = garantir_download_inteiro(url_youtube)
    cortar_local(video_local, inicio, duracao_mmss, saida_path)
    return True, "A", out_trecho, saida_path, section

def listar_mp4(pasta_local_final: str):
    return sorted(glob.glob(os.path.join(pasta_local_final, "*.mp4")))

def _rclone_copyto_with_progress(src_path: str, dst_path: str):
    # --progress habilita stats contínuas; --stats 1s atualiza a cada 1s. [web:593]
    cmd = ["rclone", "copyto", src_path, dst_path, "--progress", "--stats", "1s"]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode, (p.stdout or "")

def upload_drive_arquivo_a_arquivo(pasta_local_final: str, pasta_drive_final: str):
    files = listar_mp4(pasta_local_final)
    total = len(files)

    ok_count = 0
    err_count = 0

    failed = []  # {name, dst, attempts, returncode, tail}

    log_step(f"Upload: iniciando rclone arquivo-a-arquivo. Total={total}")
    if total == 0:
        log_step("Upload: nenhum arquivo .mp4 encontrado para enviar.")
        return

    for i, fpath in enumerate(files, 1):
        fname = os.path.basename(fpath)
        dst = f"{pasta_drive_final}/{fname}"

        pct = (i / total) * 100.0
        log_step(f"Upload {i}/{total} ({pct:.1f}%) INICIO: {fname} | OK={ok_count} ERRO={err_count}")

        last_rc = None
        last_out = ""

        for attempt in range(1, UPLOAD_MAX_ATTEMPTS + 1):
            log_step(f"Upload {i}/{total}: tentativa {attempt}/{UPLOAD_MAX_ATTEMPTS} -> {fname}")

            rc, out = _rclone_copyto_with_progress(fpath, dst)
            last_rc, last_out = rc, out

            if out:
                print(out)

            if rc == 0:
                break

            if attempt < UPLOAD_MAX_ATTEMPTS:
                log_step(f"Upload {i}/{total}: falhou na tentativa {attempt}. Aguardando {UPLOAD_RETRY_SLEEP_SEC}s para retry...")
                time.sleep(UPLOAD_RETRY_SLEEP_SEC)

        done_pct = (i / total) * 100.0

        if last_rc == 0:
            ok_count += 1
            log_step(f"Upload {i}/{total} FIM: OK | {fname} | Geral={done_pct:.1f}% OK={ok_count} ERRO={err_count}")
        else:
            err_count += 1
            tail = last_out[-4000:] if last_out else "(sem saída do rclone)"
            failed.append({
                "name": fname,
                "dst": dst,
                "attempts": UPLOAD_MAX_ATTEMPTS,
                "returncode": last_rc,
                "tail": tail
            })
            log_step(f"Upload {i}/{total} FIM: ERRO | {fname} | Geral={done_pct:.1f}% OK={ok_count} ERRO={err_count} (vai continuar)")

    log_step(f"Upload: finalizado. OK={ok_count} ERRO={err_count} (Total={total})")

    if failed:
        log_step("Upload: ARQUIVOS QUE NÃO SUBIRAM (após retries):")
        for j, item in enumerate(failed, 1):
            log_step(f"{j}) {item['name']} -> {item['dst']} (tentativas={item['attempts']}, returncode={item['returncode']})")
            print(item["tail"])

        names = ", ".join([x["name"] for x in failed])
        raise RuntimeError(f"Upload falhou para {len(failed)}/{total} arquivo(s) após retries: {names}")

def iniciar_processamento(event_path: str):
    pipeline_start = datetime.now()

    url_youtube, relatorio = ler_event_payload(event_path)
    if not url_youtube:
        raise ValueError("client_payload.url vazio")
    if not relatorio.strip():
        raise ValueError("client_payload.relatorio vazio")

    log_step(f"Pipeline INICIO. URL={url_youtube}")

    # Info do vídeo
    cmd_info = [*ytdlp_base_cmd(), "--dump-json", url_youtube]
    rc, out = run_cmd(cmd_info, check=False)
    if rc != 0:
        raise RuntimeError(out)
    video_info = json.loads(out)

    data_upload = datetime.strptime(video_info["upload_date"], "%Y%m%d")
    titulo_video = video_info["title"]

    rel_path = criar_caminho_hierarquico(data_upload, titulo_video)
    pasta_local_final = os.path.join(BASE_PATH, rel_path)
    pasta_drive_final = f"{DRIVE_NAME}:/Cortes_Midia_Igreja/{rel_path}"

    cortes = extrair_cortes(relatorio)
    total = len(cortes)

    os.makedirs(LOG_DIR, exist_ok=True)
    log_name = f"historico_{pipeline_start.strftime('%d_%m_%Y_%H_%M_%S')}.md"
    log_path = os.path.join(LOG_DIR, log_name)

    log_step(f"Vídeo: {titulo_video}")
    log_step(f"Cortes detectados: {total}")
    log_step(f"Saída local: {pasta_local_final}")
    log_step(f"Destino Drive: {pasta_drive_final}")

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"# Relatório: {titulo_video}\n- Total: {total}\n\n| # | Corte | Modo | Status | Tempo | CPU | GPU |\n|---|---|---|---|---|---|---|\n")

        for idx, (inicio, duracao, titulo) in enumerate(cortes, 1):
            cpu, g_temp = obter_telemetria()
            if g_temp > MAX_GPU_TEMP:
                log_step(f"GPU quente ({g_temp}°C). Cooldown 30s...")
                time.sleep(30)

            pct = (idx / total) * 100 if total else 100
            nome_slug = re.sub(r"[^\w\s-]", "", titulo).replace(" ", "_")[:40]
            nome_final = f"{nome_slug}__{inicio.replace(':', '-')}"
            cut_start = datetime.now()

            log_step(f"Corte {idx}/{total} ({pct:.1f}%) INICIO: {titulo} [{inicio} + {duracao}]")

            modo = "-"
            status = "ERRO"
            saida_path = ""
            section = ""
            ytdlp_tail = ""

            try:
                _, modo, out_trecho, saida_path, section = realizar_corte(
                    url_youtube=url_youtube,
                    inicio=inicio,
                    duracao_mmss=duracao,
                    nome_saida=nome_final,
                    destino_local=pasta_local_final
                )
                status = "OK"
                ytdlp_tail = (out_trecho or "")[-2000:]
            except Exception as e:
                err = str(e)
                ytdlp_tail = err[-2000:]
                log_step(f"Corte {idx}/{total} FALHOU: {err}")

            cut_end = datetime.now()
            elapsed = (cut_end - cut_start).total_seconds()
            log_step(f"Corte {idx}/{total} FIM: status={status} modo={modo} tempo={fmt_td(elapsed)} section={section} arquivo={saida_path}")

            log.write(f"| {idx} | {titulo} | {modo} | {status} | {fmt_td(elapsed)} | {cpu}% | {g_temp}°C |\n")
            if ytdlp_tail:
                log.write(f"\n<details><summary>Debug corte #{idx}</summary>\n\n```\n{ytdlp_tail}\n```\n</details>\n\n")

            time.sleep(COOL_DOWN_TIME)

    # Upload
    log_step("Upload: INICIO")
    upload_drive_arquivo_a_arquivo(pasta_local_final, pasta_drive_final)
    log_step("Upload: FIM")

    pipeline_end = datetime.now()
    total_s = (pipeline_end - pipeline_start).total_seconds()
    log_step(f"Pipeline FIM. Tempo total: {fmt_td(total_s)}. Log: {log_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", required=True)
    args = parser.parse_args()
    iniciar_processamento(args.event_path)
