import os, subprocess, re, time, json, argparse, hashlib, glob
from datetime import datetime
import sys
import unicodedata

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
UPLOAD_MAX_ATTEMPTS = 2
UPLOAD_RETRY_SLEEP_SEC = 8

# =========================
# NOMES CONHECIDOS (CULTO)
# =========================
NOMES_CULTO_CONHECIDOS = [
    "quinta viva com cristo",
    "celebracao manha",
    "celebracao noite",
    "sunday night",
    "kids",
    "projeto familia",
    "homens",
    "mmr",
    "santa ceia manha",
    "santa ceia noite",
    "consagracao",
    "adola",
    "conferencia",
    "tarde teologica",
]

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

# =========================
# NORMALIZAÇÃO / SLUG
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))  # remove acentos [web:688]
    s = re.sub(r"\s+", " ", s)
    return s

def slugify(text: str) -> str:
    text = _norm(text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "sem_nome"

# =========================
# FOCO (tipocorte)
# =========================
def detectar_tipo_corte(relatorio: str) -> str:
    m = re.search(r'^Foco da Solicitação:\s*(.+)$', relatorio or "", re.MULTILINE | re.IGNORECASE)
    v = (m.group(1) if m else "").lower()

    if "louvor" in v:
        return "LOUVOR"
    if ("pregador" in v) or ("pregação" in v) or ("pastor" in v):
        return "PREGACAO"
    if ("oração" in v) or ("oracao" in v) or ("intercess" in v) or ("clamor" in v):
        return "ORACAO"
    if "testemun" in v or "relatos" in v or "experiên" in v or "experien" in v:
        return "TESTEMUNHO"
    if ("professor" in v) or ("professora" in v) or ("escola bíblica" in v) or ("ebd" in v) or ("aula" in v):
        return "EBD"

    return "OUTROS"

def detectar_grupo_evento(relatorio: str) -> str:
    # regra: se for EBD -> pasta EBD, senão -> CULTO
    return "EBD" if detectar_tipo_corte(relatorio) == "EBD" else "CULTO"

# =========================
# EXTRAÇÃO "NOME DO CULTO" / GRANULARIDADE EBD
# =========================
def extrair_tema_ebd(titulo_video: str) -> str:
    """
    Exemplo real:
    'AULA 04 - FERIDAS E AMARGURAS, FERIDAS DA REJEIÇÃO, PERDÃO | CLASSE GERAL | RECREIO | 25.01.26'
    -> 'aula_04_feridas_e_amarguras_feridas_da_rejeicao_perdao'
    """
    t = _norm(titulo_video)

    # remove partes após pipes (CLASSE GERAL | RECREIO | data)
    t = t.split("|")[0].strip()

    # remove sufixos óbvios e deixa só o tema da aula
    # mantém "aula 04" se existir
    t = re.sub(r"\s*-\s*", " - ", t).strip()

    # normaliza separadores
    t = t.replace(",", " ")
    t = re.sub(r"\s+", " ", t).strip()

    # slug final
    return slugify(t)

def extrair_nome_do_culto(titulo_video: str, grupo: str) -> str:
    """
    Para CULTO: tenta bater com tokens conhecidos dentro dos '|' ou por substring.
    Para EBD: retorna tema granular (aula + tema).
    """
    if grupo == "EBD":
        return extrair_tema_ebd(titulo_video)

    t = _norm(titulo_video)
    parts = [p.strip() for p in t.split("|") if p.strip()]

    cleaned = []
    for p in parts:
        if p in ("recreio", "classe geral"):
            continue
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", p):  # 25.01.26
            continue
        if p.startswith(("pr.", "pra.", "pastor", "pastora")):
            continue
        cleaned.append(p)

    # match exato por token
    for k in NOMES_CULTO_CONHECIDOS:
        for p in cleaned:
            if p == k:
                return k

    # match por substring no título todo
    for k in NOMES_CULTO_CONHECIDOS:
        if k in t:
            return k

    # fallback: primeiro bloco útil
    return cleaned[0] if cleaned else titulo_video

# =========================
# PATH NOVO (espelhado)
# =========================
def criar_caminho_hierarquico(data_video, titulo_video, relatorio):
    ano = str(data_video.year)
    mes = data_video.strftime("%m_%B")

    grupo = detectar_grupo_evento(relatorio)             # EBD ou CULTO
    tipocorte = detectar_tipo_corte(relatorio)           # LOUVOR/PREGACAO/ORACAO/TESTEMUNHO/EBD/...
    nomeculto = extrair_nome_do_culto(titulo_video, grupo)

    dia_mes_ano = data_video.strftime("%d_%m_%Y")
    pasta_execucao = f"{dia_mes_ano}_{nomeculto}_{tipocorte}"

    # slug final para segurança no Windows/Drive
    pasta_execucao = slugify(pasta_execucao)

    return os.path.join(ano, mes, grupo, pasta_execucao).replace("\\", "/")

# =========================
# EVENT PAYLOAD
# =========================
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

# =========================
# PARSER UNIFICADO DE CORTES
# =========================
def extrair_cortes(relatorio: str):
    import re

    def detect_focus(text: str) -> str:
        m = re.search(r'^Foco da Solicitação:\s*(.+)$', text, re.MULTILINE)
        if not m:
            return "unknown"
        v = m.group(1).lower()

        if "louvor" in v:
            return "louvor"
        if ("pregador" in v) or ("pregação" in v) or ("pastor" in v):
            return "pregacao"
        if ("oração" in v) or ("oracao" in v) or ("intercess" in v) or ("clamor" in v):
            return "oracao"
        if "testemun" in v or "relatos" in v or "experiên" in v or "experien" in v:
            return "testemunho"
        if ("professor" in v) or ("professora" in v) or ("escola bíblica" in v) or ("ebd" in v) or ("aula" in v):
            return "aula"
        return "unknown"

    def extrair_cortes_louvor(relatorio_louvor: str):
        linhas = relatorio_louvor.splitlines()
        cortes = []

        re_header = re.compile(r'^Louvor\s*(\d+)\s*[—-]\s*(.+?)\s*$')
        re_integral = re.compile(
            r'^Integral:\s*\[\[(\d{2}:\d{2}:\d{2})\]\]\s+até\s+\[\[(\d{2}:\d{2}:\d{2})\]\]\s*\(Duração:\s*(\d{2}:\d{2})\)\s*$'
        )
        re_ouro = re.compile(
            r'^Ouro\s*([A-D]):\s*\[\[(\d{2}:\d{2}:\d{2})\]\]\s+até\s+\[\[(\d{2}:\d{2}:\d{2})\]\]\s*\([^)]*\)\s*[—-]\s*(.*)$'
        )

        def to_seconds(hhmmss: str) -> int:
            h, m, s = map(int, hhmmss.split(":"))
            return h * 3600 + m * 60 + s

        def seconds_to_mmss(sec: int) -> str:
            if sec < 0:
                sec = 0
            m = sec // 60
            s = sec % 60
            return f"{m:02d}:{s:02d}"

        def sanitize(s: str, max_len: int = 60) -> str:
            s = (s or "").strip()
            s = re.sub(r'[\\/:*?"<>|]', '', s)
            s = re.sub(r'\s+', ' ', s)
            return s[:max_len].strip()

        cur = None
        for raw in linhas:
            line = (raw or "").strip()
            if not line:
                continue

            mh = re_header.match(line)
            if mh:
                cur = {"idx": int(mh.group(1)), "nome": mh.group(2).strip()}
                continue
            if cur is None:
                continue

            mi = re_integral.match(line)
            if mi:
                inicio, fim, dur = mi.group(1), mi.group(2), mi.group(3)
                titulo = f"LOUVOR_{cur['idx']:02d}_INTEGRAL__{sanitize(cur['nome'], 80)}"
                cortes.append((inicio, dur, titulo))
                continue

            mo = re_ouro.match(line)
            if mo:
                letra, inicio, fim, trecho = mo.group(1), mo.group(2), mo.group(3), mo.group(4)
                dur = seconds_to_mmss(to_seconds(fim) - to_seconds(inicio))
                titulo = f"LOUVOR_{cur['idx']:02d}_OURO_{letra}__{sanitize(cur['nome'], 60)}__{sanitize(trecho, 60)}"
                cortes.append((inicio, dur, titulo))
                continue

        return cortes

    def extrair_cortes_pregacao(relatorio_preg: str):
        linhas = relatorio_preg.splitlines()
        cortes = []

        re_md_dur = re.compile(
            r'^\[\[(\d{1,2}:\d{2}(?::\d{2})?)\]\([^)]+\)\]\s+até\s+\[\[(\d{1,2}:\d{2}(?::\d{2})?)\]\([^)]+\)\]\s*\(Duração:\s*(\d{2}:\d{2})\)\s*(.*)$'
        )

        def to_hhmmss(t: str) -> str:
            t = t.strip()
            if re.match(r'^\d{1,2}:\d{2}:\d{2}$', t):
                h, m, s = t.split(':')
                return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
            if re.match(r'^\d{1,2}:\d{2}$', t):
                m, s = t.split(':')
                return f"00:{int(m):02d}:{int(s):02d}"
            return t

        def sanitize_title(raw: str) -> str:
            raw = (raw or "").strip()
            raw = re.sub(r"^Hook:\s*", "", raw, flags=re.IGNORECASE).strip()
            return raw.strip('"').strip("'").strip()

        def guess_title_from_next(i: int) -> str:
            j = i + 1
            while j < len(linhas):
                t = (linhas[j] or "").strip()
                if not t or t.lower().startswith("categoria:"):
                    j += 1
                    continue
                if re_md_dur.match(t):
                    return ""
                return sanitize_title(t)
            return ""

        i = 0
        while i < len(linhas):
            line = (linhas[i] or "").strip()
            m = re_md_dur.match(line)
            if not m:
                i += 1
                continue

            inicio = to_hhmmss(m.group(1))
            dur = m.group(3)

            title_inline = sanitize_title(m.group(4))
            title_next = guess_title_from_next(i)
            titulo = title_inline or title_next or f"corte_{len(cortes)+1}"

            cortes.append((inicio, dur, titulo))
            i += 1

        return cortes

    def extrair_cortes_plain(relatorio_plain: str, prefix: str):
        linhas = relatorio_plain.splitlines()
        cortes = []

        re_plain = re.compile(
            r'^\[(\d{2}:\d{2}:\d{2})\]\s+até\s+\(Duração:\s*(\d{2}:\d{2})\)\s*(.*)$'
        )

        def sanitize_title(raw: str) -> str:
            raw = (raw or "").strip()
            raw = re.sub(r"^Assunto:\s*", "", raw, flags=re.IGNORECASE).strip()
            raw = re.sub(r"^Motivo:\s*", "", raw, flags=re.IGNORECASE).strip()
            raw = re.sub(r"^Hook:\s*", "", raw, flags=re.IGNORECASE).strip()
            raw = raw.strip('"').strip("'").strip()
            return raw

        for raw in linhas:
            line = (raw or "").strip()
            m = re_plain.match(line)
            if not m:
                continue
            inicio, dur, rest = m.group(1), m.group(2), m.group(3)
            titulo = sanitize_title(rest) or f"{prefix}_{len(cortes)+1}"
            cortes.append((inicio, dur, titulo))

        return cortes

    focus = detect_focus(relatorio)

    if focus == "louvor":
        return extrair_cortes_louvor(relatorio)
    if focus == "pregacao":
        return extrair_cortes_pregacao(relatorio)
    if focus == "testemunho":
        return extrair_cortes_plain(relatorio, prefix="testemunho")
    if focus == "oracao":
        return extrair_cortes_plain(relatorio, prefix="oracao")
    if focus == "aula":
        return extrair_cortes_plain(relatorio, prefix="aula_professor")

    raise ValueError("Não consegui detectar o foco do relatório (Foco da Solicitação).")

# =========================
# HELPERS TEMPO
# =========================
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

# =========================
# EXEC
# =========================
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

    cmd_info = [*ytdlp_base_cmd(), "--dump-json", url_youtube]
    rc, out = run_cmd(cmd_info, check=False)
    if rc != 0:
        raise RuntimeError(out)
    video_info = json.loads(out)

    data_upload = datetime.strptime(video_info["upload_date"], "%Y%m%d")
    titulo_video = video_info["title"]

    rel_path = criar_caminho_hierarquico(data_upload, titulo_video, relatorio)
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
            nome_slug = re.sub(r"[^\w\s-]", "", titulo).replace(" ", "_")[:80]
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
