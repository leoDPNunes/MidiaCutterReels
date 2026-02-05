import os, subprocess, re, time, json, argparse, hashlib, glob, sys, unicodedata
from datetime import datetime

import psutil
import GPUtil


# =========================
# UTF-8 robusto
# =========================
def force_utf8_stdio():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


force_utf8_stdio()


# =========================
# CONFIG
# =========================
BASE_PATH = "F:/Cortes_midia"
LOG_DIR = "D:/Coding/HTML/midia_cutter_reels/logs"
DRIVE_NAME = "meu_drive"

MAX_GPU_TEMP = 80
COOL_DOWN_TIME = 1

DOWNLOAD_CACHE_DIR = os.path.join(BASE_PATH, "_cache_downloads").replace("\\", "/")

YTDLP_COOKIES_TXT_PATH = r"D:\secrets\yt_cookies.txt"
YTDLP_NODE_EXE = r"C:\nvm4w\nodejs\node.exe"

YTDLP_FORMAT_FULL = "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best"
YTDLP_NET_ARGS = ["-4", "--retries", "10", "--fragment-retries", "10", "--retry-sleep", "exp=1:20:2"]

UPLOAD_MAX_ATTEMPTS = 2
UPLOAD_RETRY_SLEEP_SEC = 8

NOMES_CULTO_CONHECIDOS = [
    "quinta viva com cristo", "celebracao manha", "celebracao noite",
    "sunday night", "kids", "projeto familia", "homens", "mmr",
    "santa ceia manha", "santa ceia noite", "consagracao",
    "adola", "conferencia", "tarde teologica"
]


# =========================
# Utils
# =========================
def log_step(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def fmt_td(seconds: float) -> str:
    seconds = int(round(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"


def obter_telemetria():
    cpu = psutil.cpu_percent()
    gpu = GPUtil.getGPUs()[0] if GPUtil.getGPUs() else None
    return cpu, (gpu.temperature if gpu else 0)


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s


def slugify(text: str) -> str:
    text = _norm(text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "sem_nome"


def hhmmss_to_seconds(hhmmss: str) -> int:
    h, m, s = hhmmss.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def seconds_to_hhmmss(sec: int) -> str:
    if sec < 0:
        sec = 0
    h = sec // 3600
    sec %= 3600
    m = sec // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def mmss_from_seconds(sec: int) -> str:
    if sec < 1:
        sec = 1
    return f"{sec//60:02d}:{sec%60:02d}"


def limpar_url(url: str) -> str:
    s = (url or "").strip().strip('"').strip("'")
    m = re.search(r"\((https?://[^)]+)\)", s)
    return m.group(1).strip() if m else s


def sha1_12(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="replace")).hexdigest()[:12]


# =========================
# Payload (robusto)
# =========================
def _read_json_file_robust(path: str) -> dict:
    raw = None
    with open(path, "rb") as f:
        raw = f.read()
    try:
        txt = raw.decode("utf-8")
    except UnicodeDecodeError:
        txt = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(txt)
    except Exception as e:
        raise ValueError(f"Falha lendo JSON do event_path (len={len(raw)} sha1={hashlib.sha1(raw).hexdigest()[:12]}): {e}")


def ler_event_payload(event_path: str):
    event = _read_json_file_robust(event_path)
    payload = event.get("client_payload", {}) or {}
    url = limpar_url(payload.get("url") or "")
    relatorio = payload.get("relatorio") or ""
    return url, relatorio


# =========================
# Contrato do relatório (novo)
# =========================
ALLOWED_TIPOS = {"EBD", "CULTO"}
ALLOWED_FOCOS = {"LOUVOR", "PREGACAO", "EBD", "ORACAO", "TESTEMUNHO"}
ALLOWED_TAGS = ("Hook:", "Assunto:", "Motivo:", "Título:", "Titulo:")

RE_TS_LINE = re.compile(r'^\s*\[\[(\d{2}:\d{2}:\d{2})\]\]\s+\[\[(\d{2}:\d{2}:\d{2})\]\]\s*$')


def _parse_header_field(relatorio: str, field: str) -> str:
    m = re.search(rf'^{re.escape(field)}\s*(.+?)\s*$', relatorio, re.MULTILINE)
    return (m.group(1).strip() if m else "")


def detectar_foco_estrito(relatorio: str) -> str:
    foco = _parse_header_field(relatorio or "", "Foco da Solicitação:")
    foco_n = _norm(foco).upper()
    foco_n = foco_n.replace("Ç", "C")  # redundante, mas não dói
    foco_n = foco_n.replace(" ", "")
    foco_n = foco_n.replace("PREGACAO", "PREGACAO")
    foco_n = foco_n.replace("TESTEMUNHO", "TESTEMUNHO")
    foco_n = foco_n.replace("ORACAO", "ORACAO")
    foco_n = foco_n.replace("LOUVOR", "LOUVOR")
    foco_n = foco_n.replace("EBD", "EBD")

    # normalização “manual” pra lidar com acento
    foco_raw = _norm(foco)
    if foco_raw in ("louvor",):
        return "LOUVOR"
    if foco_raw in ("pregacao", "pregação"):
        return "PREGACAO"
    if foco_raw in ("ebd",):
        return "EBD"
    if foco_raw in ("oracao", "oração"):
        return "ORACAO"
    if foco_raw.startswith("testemun"):
        return "TESTEMUNHO"
    return ""


def detectar_tipo_conteudo(relatorio: str) -> str:
    tipo = _parse_header_field(relatorio or "", "Tipo de Conteúdo:")
    tipo_n = _norm(tipo).upper()
    if "EBD" == tipo_n:
        return "EBD"
    if "CULTO" == tipo_n:
        return "CULTO"
    return ""


def detectar_tipo_corte(relatorio: str) -> str:
    foco = detectar_foco_estrito(relatorio)
    if foco in ALLOWED_FOCOS:
        return foco

    # fallback heurístico temporário (formatos antigos)
    m = re.search(r"^Foco da Solicitação:\s*(.+)$", relatorio or "", re.MULTILINE | re.IGNORECASE)
    v = (m.group(1) if m else "").lower()
    if "louvor" in v:
        return "LOUVOR"
    if ("pregador" in v) or ("pregação" in v) or ("pregacao" in v) or ("pastor" in v) or ("mensagem" in v):
        return "PREGACAO"
    if ("oração" in v) or ("oracao" in v) or ("intercess" in v) or ("clamor" in v):
        return "ORACAO"
    if ("testemun" in v) or ("relatos" in v) or ("experiên" in v) or ("experien" in v):
        return "TESTEMUNHO"
    if ("ebd" in v) or ("escola bíblica" in v) or ("escola biblica" in v) or ("aula" in v) or ("professor" in v):
        return "EBD"
    return "OUTROS"


def detectar_grupo_evento(relatorio: str) -> str:
    return "EBD" if detectar_tipo_conteudo(relatorio) == "EBD" or detectar_tipo_corte(relatorio) == "EBD" else "CULTO"


def extrair_tema_ebd(titulo_video: str) -> str:
    t = _norm(titulo_video).split("|")[0].strip()
    t = re.sub(r"\s*-\s*", " - ", t).strip()
    t = t.replace(",", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return slugify(t)


def extrair_nome_do_culto(titulo_video: str, grupo: str) -> str:
    if grupo == "EBD":
        return extrair_tema_ebd(titulo_video)

    t = _norm(titulo_video)
    parts = [p.strip() for p in t.split("|") if p.strip()]
    cleaned = []
    for p in parts:
        if p in ("recreio", "classe geral"):
            continue
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", p):
            continue
        if p.startswith(("pr.", "pra.", "pastor", "pastora")):
            continue
        cleaned.append(p)

    for k in NOMES_CULTO_CONHECIDOS:
        for p in cleaned:
            if p == k:
                return k
    for k in NOMES_CULTO_CONHECIDOS:
        if k in t:
            return k
    return cleaned[0] if cleaned else titulo_video


def criar_caminho_hierarquico(data_video, titulo_video, relatorio):
    ano = str(data_video.year)
    mes = data_video.strftime("%m_%B")
    grupo = detectar_grupo_evento(relatorio)
    tipocorte = detectar_tipo_corte(relatorio)
    nomeculto = extrair_nome_do_culto(titulo_video, grupo)
    dia = data_video.strftime("%d_%m_%Y")
    pasta_execucao = slugify(f"{dia}_{nomeculto}_{tipocorte}")
    return os.path.join(ano, mes, grupo, pasta_execucao).replace("\\", "/")


def _sanitize_title(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r'^[A-Za-zÀ-ÿ\- ]{2,30}:\s*', '', s).strip()
    s = s.strip('"').strip("'").strip()
    s = re.sub(r'[\\/:*?"<>|]', '', s)
    s = re.sub(r"\s+", " ", s)
    return s


def _find_cortes_section_lines(relatorio: str) -> list[str]:
    lines = relatorio.splitlines()
    idx = None
    for i, raw in enumerate(lines):
        if (raw or "").strip() == "Cortes para Automação":
            idx = i
            break
    if idx is None:
        return []
    return lines[idx + 1 :]


def _validate_no_http_in_cortes_section(section_lines: list[str]):
    joined = "\n".join(section_lines)
    if "http" in joined.lower():
        raise ValueError("Relatório inválido: existe 'http' dentro da seção 'Cortes para Automação'.")


def _validate_no_triple_bracket(relatorio: str):
    if "[[[" in relatorio:
        raise ValueError("Relatório inválido: contém '[[[' (proibido).")


def extrair_cortes_formato_novo(relatorio: str, foco: str):
    """
    Formato novo:
      Cabeçalho com Foco da Solicitação
      Seção 'Cortes para Automação'
      Blocos de 2 linhas:
        [[HH:MM:SS]] [[HH:MM:SS]]
        Hook:/Assunto:/Motivo:/Título: ... (para LOUVOR deve conter INTEGRAL ou OURO 10/15/20/30)
    Retorna lista de dicts: {ini, fim, dur_mmss, titulo, kind, alvo}
    """
    _validate_no_triple_bracket(relatorio)

    section = _find_cortes_section_lines(relatorio)
    if not section:
        return []

    _validate_no_http_in_cortes_section(section)

    cortes = []
    i = 0
    while i < len(section):
        line1 = (section[i] or "").strip()
        if not line1:
            i += 1
            continue

        m = RE_TS_LINE.match(line1)
        if not m:
            raise ValueError(f"Relatório inválido na seção de cortes: linha de timestamp fora do padrão: '{line1}'")

        ini, fim = m.group(1), m.group(2)
        if hhmmss_to_seconds(fim) <= hhmmss_to_seconds(ini):
            raise ValueError(f"Relatório inválido: fim <= início no corte {ini} -> {fim}")

        # linha2 obrigatória (próxima não vazia)
        j = i + 1
        while j < len(section) and not (section[j] or "").strip():
            j += 1
        if j >= len(section):
            raise ValueError(f"Relatório inválido: faltou a linha 2 (descrição) após timestamps {ini} {fim}")

        line2 = (section[j] or "").strip()
        if not line2.startswith(ALLOWED_TAGS):
            raise ValueError(f"Relatório inválido: linha 2 deve começar com Hook:/Assunto:/Motivo:/Título:. Recebido: '{line2}'")

        desc = _sanitize_title(line2)

        kind = "CORTE"
        alvo = None

        if foco == "LOUVOR":
            up = _norm(desc).upper()
            if "INTEGRAL" in up:
                kind = "INTEGRAL"
            else:
                mo = re.search(r"\bOURO\s*(10|15|20|30)\b", up)
                if not mo:
                    raise ValueError("Relatório LOUVOR inválido: cada corte precisa indicar INTEGRAL ou OURO 10/15/20/30 na linha 2.")
                kind = "OURO"
                alvo = int(mo.group(1))

        dur_sec = hhmmss_to_seconds(fim) - hhmmss_to_seconds(ini)
        dur_mmss = mmss_from_seconds(dur_sec)

        cortes.append({
            "ini": ini,
            "fim": fim,
            "dur_mmss": dur_mmss,
            "desc": desc or f"corte_{len(cortes)+1}",
            "kind": kind,
            "alvo": alvo,
        })

        i = j + 1

    return cortes


# ========= fallbacks (antigos) =========
def extrair_cortes_louvor_gemini_antigo(relatorio: str):
    linhas = relatorio.splitlines()
    cortes = []

    re_header = re.compile(r'^Louvor\s*(\d+)\s*[—-]\s*(.+?)\s*$')
    re_integral = re.compile(r'^Integral:\s*\[\[(\d{2}:\d{2}:\d{2})\]\]\s+até\s+\[\[(\d{2}:\d{2}:\d{2})\]\]\s*\(Duração:\s*(\d{2}:\d{2})\)\s*$', re.IGNORECASE)
    re_ouro = re.compile(r'^Ouro\s*([A-D]):\s*\[\[(\d{2}:\d{2}:\d{2})\]\]\s+até\s+\[\[(\d{2}:\d{2}:\d{2})\]\]\s*\([^)]*\)\s*[—-]\s*(.*)$', re.IGNORECASE)

    def sanitize(s: str, max_len: int = 80) -> str:
        s = _sanitize_title(s)
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
            cortes.append({"ini": inicio, "fim": fim, "dur_mmss": dur, "desc": titulo, "kind": "INTEGRAL", "alvo": None})
            continue

        mo = re_ouro.match(line)
        if mo:
            letra, inicio, fim, trecho = mo.group(1), mo.group(2), mo.group(3), mo.group(4)
            dur_sec = max(1, hhmmss_to_seconds(fim) - hhmmss_to_seconds(inicio))
            dur = mmss_from_seconds(dur_sec)
            titulo = f"LOUVOR_{cur['idx']:02d}_OURO_{letra}__{sanitize(cur['nome'], 60)}__{sanitize(trecho, 60)}"
            cortes.append({"ini": inicio, "fim": fim, "dur_mmss": dur, "desc": titulo, "kind": "OURO", "alvo": None})
            continue

    return cortes


def extrair_cortes(relatorio: str):
    foco = detectar_tipo_corte(relatorio)

    # 1) formato novo (principal)
    cortes = extrair_cortes_formato_novo(relatorio, foco)
    if cortes:
        return foco, cortes

    # 2) fallback louvor antigo
    cortes2 = extrair_cortes_louvor_gemini_antigo(relatorio)
    if cortes2:
        return "LOUVOR", cortes2

    # 3) fallback "plain"
    linhas = relatorio.splitlines()
    cortes3 = []
    re_plain = re.compile(r'^\s*\[(\d{2}:\d{2}:\d{2})\]\s+até\s+\(Duração:\s*(\d{2}:\d{2})\)\s*(.*)$', re.IGNORECASE)
    for raw in linhas:
        line = (raw or "").strip()
        m = re_plain.match(line)
        if not m:
            continue
        ini, dur, rest = m.group(1), m.group(2), _sanitize_title(m.group(3))
        titulo = rest or f"corte_{len(cortes3)+1}"
        cortes3.append({"ini": ini, "fim": None, "dur_mmss": dur, "desc": titulo, "kind": "CORTE", "alvo": None})
    if cortes3:
        return foco, cortes3

    raise ValueError("Não encontrei cortes. Use o formato novo com 'Cortes para Automação' e blocos [[HH:MM:SS]] [[HH:MM:SS]] + linha 2.")


# =========================
# Execução (yt-dlp/ffmpeg)
# =========================
def run_cmd_live(cmd, check=True):
    log_step("CMD: " + " ".join(str(x) for x in cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    out_lines = []
    for line in p.stdout:
        print(line, end="", flush=True)
        out_lines.append(line)
    rc = p.wait()
    out = "".join(out_lines)
    if check and rc != 0:
        raise RuntimeError(out)
    return rc, out


def cache_key_for_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8", errors="replace")).hexdigest()[:16]


def ytdlp_base_cmd():
    return [
        "yt-dlp",
        "--cookies", YTDLP_COOKIES_TXT_PATH,
        "--remote-components", "ejs:github",
        "--js-runtimes", f"node:{YTDLP_NODE_EXE}",
        *YTDLP_NET_ARGS
    ]


def garantir_download_inteiro(url_youtube: str) -> str:
    os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)
    key = cache_key_for_url(url_youtube)
    outtmpl = os.path.join(DOWNLOAD_CACHE_DIR, f"{key}.%(ext)s")
    mp4_path = os.path.join(DOWNLOAD_CACHE_DIR, f"{key}.mp4")

    if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0:
        log_step(f"Cache hit (vídeo inteiro): {mp4_path}")
        return mp4_path

    log_step("Baixando vídeo inteiro (cache) via yt-dlp...")
    cmd = [*ytdlp_base_cmd(), "-f", YTDLP_FORMAT_FULL, "--merge-output-format", "mp4", "-o", outtmpl, url_youtube]
    rc, out = run_cmd_live(cmd, check=False)
    if rc != 0:
        raise RuntimeError(out)

    if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0:
        return mp4_path

    for fn in os.listdir(DOWNLOAD_CACHE_DIR):
        if fn.startswith(key + "."):
            fp = os.path.join(DOWNLOAD_CACHE_DIR, fn)
            if os.path.getsize(fp) > 0:
                return fp

    raise RuntimeError("Download inteiro finalizou, mas arquivo não encontrado no cache.")


def cortar_local_por_ini_fim(video_path: str, ini_hhmmss: str, fim_hhmmss: str, saida_path: str):
    os.makedirs(os.path.dirname(saida_path), exist_ok=True)
    # usa -to (fim absoluto) para casar com o relatório (snap já vem pronto)
    cmd = ["ffmpeg", "-y", "-hide_banner", "-ss", ini_hhmmss, "-to", fim_hhmmss, "-i", video_path, "-c", "copy", saida_path]
    rc, out = run_cmd_live(cmd, check=False)
    if rc != 0:
        raise RuntimeError(out)


def cortar_local_por_dur(video_path: str, ini_hhmmss: str, dur_mmss: str, saida_path: str):
    duracao = f"00:{dur_mmss}"
    os.makedirs(os.path.dirname(saida_path), exist_ok=True)
    cmd = ["ffmpeg", "-y", "-hide_banner", "-ss", ini_hhmmss, "-t", duracao, "-i", video_path, "-c", "copy", saida_path]
    rc, out = run_cmd_live(cmd, check=False)
    if rc != 0:
        raise RuntimeError(out)


def tentar_baixar_trecho(url_youtube: str, inicio_hhmmss: str, duracao_mmss: str, saida_path: str):
    end = seconds_to_hhmmss(hhmmss_to_seconds(inicio_hhmmss) + (int(duracao_mmss.split(":")[0]) * 60 + int(duracao_mmss.split(":")[1])))
    section = f"*{inicio_hhmmss}-{end}"
    cmd = [*ytdlp_base_cmd(), "-f", YTDLP_FORMAT_FULL,
           "--download-sections", section, "--force-keyframes-at-cuts",
           "--merge-output-format", "mp4", "-o", saida_path, url_youtube]
    rc, out = run_cmd_live(cmd, check=False)
    if rc == 0 and os.path.exists(saida_path) and os.path.getsize(saida_path) > 0:
        return True, out, section
    return False, out, section


def realizar_corte(url_youtube, corte: dict, nome_saida: str, destino_local: str, tipocorte: str):
    os.makedirs(destino_local, exist_ok=True)
    saida_path = os.path.join(destino_local, f"{nome_saida}.mp4")

    ini = corte["ini"]
    fim = corte.get("fim")
    dur = corte["dur_mmss"]

    # Anti-HLS para LOUVOR: sempre vídeo inteiro + corte local
    if tipocorte == "LOUVOR":
        video_local = garantir_download_inteiro(url_youtube)
        if fim:
            cortar_local_por_ini_fim(video_local, ini, fim, saida_path)
        else:
            cortar_local_por_dur(video_local, ini, dur, saida_path)
        return "A_LOCAL", "", saida_path, ""

    ok, out_trecho, section = tentar_baixar_trecho(url_youtube, ini, dur, saida_path)
    if ok:
        return "B_TRECHO", out_trecho, saida_path, section

    video_local = garantir_download_inteiro(url_youtube)
    if fim:
        cortar_local_por_ini_fim(video_local, ini, fim, saida_path)
    else:
        cortar_local_por_dur(video_local, ini, dur, saida_path)
    return "A_LOCAL", out_trecho, saida_path, section


# =========================
# Upload (rclone copyto)
# =========================
def listar_mp4(pasta_local_final: str):
    return sorted(glob.glob(os.path.join(pasta_local_final, "*.mp4")))


def _rclone_copyto_with_progress(src_path: str, dst_path: str):
    cmd = ["rclone", "copyto", src_path, dst_path, "--progress", "--stats", "1s"]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    return p.returncode, (p.stdout or "")


def upload_drive_arquivo_a_arquivo(pasta_local_final: str, pasta_drive_final: str):
    files = listar_mp4(pasta_local_final)
    total = len(files)

    log_step(f"Upload: iniciando. Total={total}")
    if total == 0:
        log_step("Upload: nenhum .mp4 encontrado para enviar.")
        return

    failed = []
    for i, fpath in enumerate(files, 1):
        fname = os.path.basename(fpath)
        dst = f"{pasta_drive_final}/{fname}"
        log_step(f"Upload {i}/{total}: {fname}")

        last_rc = None
        last_out = ""
        for attempt in range(1, UPLOAD_MAX_ATTEMPTS + 1):
            rc, out = _rclone_copyto_with_progress(fpath, dst)
            last_rc, last_out = rc, out
            if out:
                print(out, flush=True)
            if rc == 0:
                break
            if attempt < UPLOAD_MAX_ATTEMPTS:
                time.sleep(UPLOAD_RETRY_SLEEP_SEC)

        if last_rc != 0:
            failed.append((fname, last_out[-3000:] if last_out else ""))

    if failed:
        for name, tail in failed:
            log_step(f"Falhou upload: {name}")
            if tail:
                print(tail, flush=True)
        raise RuntimeError(f"Upload falhou para {len(failed)}/{total} arquivo(s).")


# =========================
# Pipeline
# =========================
def _build_output_name(tipocorte: str, corte: dict, idx: int) -> str:
    desc = corte.get("desc") or f"corte_{idx}"
    ini = corte["ini"].replace(":", "-")

    if tipocorte == "LOUVOR":
        if corte["kind"] == "INTEGRAL":
            base = f"LOUVOR_{idx:03d}_INTEGRAL__{desc}"
        elif corte["kind"] == "OURO":
            alvo = corte.get("alvo")
            base = f"LOUVOR_{idx:03d}_OURO_{alvo:02d}__{desc}"
        else:
            base = f"LOUVOR_{idx:03d}__{desc}"
    else:
        base = f"{tipocorte}_{idx:03d}__{desc}"

    base = re.sub(r"[^\w\s-]", "", base).replace(" ", "_")
    base = re.sub(r"_+", "_", base).strip("_")[:120]
    return f"{base}__{ini}"


def iniciar_processamento(event_path: str):
    pipeline_start = datetime.now()
    os.makedirs(LOG_DIR, exist_ok=True)

    url_youtube, relatorio = ler_event_payload(event_path)
    if not url_youtube:
        raise ValueError("client_payload.url vazio")
    if not (relatorio or "").strip():
        raise ValueError("client_payload.relatorio vazio")

    # não logar relatório; só fingerprint
    log_step(f"Evento OK. url={url_youtube} rel_len={len(relatorio)} rel_sha1_12={sha1_12(relatorio)}")

    tipocorte, cortes = extrair_cortes(relatorio)
    total = len(cortes)
    log_step(f"Pipeline INICIO. tipocorte={tipocorte} cortes={total}")

    # info do vídeo
    log_step("yt-dlp --dump-json INICIO")
    cmd_info = [*ytdlp_base_cmd(), "--dump-json", url_youtube]
    rc, out = run_cmd_live(cmd_info, check=False)
    if rc != 0:
        raise RuntimeError(out)
    video_info = json.loads(out)
    log_step("yt-dlp --dump-json FIM")

    data_upload = datetime.strptime(video_info["upload_date"], "%Y%m%d")
    titulo_video = video_info["title"]

    rel_path = criar_caminho_hierarquico(data_upload, titulo_video, relatorio)
    pasta_local_final = os.path.join(BASE_PATH, rel_path)
    pasta_drive_final = f"{DRIVE_NAME}:/Cortes_Midia_Igreja/{rel_path}"

    log_name = f"historico_{pipeline_start.strftime('%d_%m_%Y_%H_%M_%S')}.md"
    log_path = os.path.join(LOG_DIR, log_name)

    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"# Relatório: {titulo_video}\n")
        log.write(f"- Tipocorte: {tipocorte}\n")
        log.write(f"- URL: {url_youtube}\n")
        log.write(f"- Total de cortes: {total}\n")
        log.write(f"- Saída local: {pasta_local_final}\n")
        log.write(f"- Destino Drive: {pasta_drive_final}\n")
        log.write(f"- Relatorio sha1_12: {sha1_12(relatorio)}\n\n")
        log.write("| # | Corte | Modo | Status | Tempo |\n|---|---|---|---|---|\n")

    log_step(f"Vídeo: {titulo_video}")
    log_step(f"Saída local: {pasta_local_final}")
    log_step(f"Destino Drive: {pasta_drive_final}")
    log_step(f"Log: {log_path}")

    if tipocorte == "LOUVOR":
        log_step("LOUVOR: pré-download do vídeo inteiro (cache)...")
        garantir_download_inteiro(url_youtube)
        log_step("LOUVOR: pré-download OK.")

    for idx, corte in enumerate(cortes, 1):
        _, g_temp = obter_telemetria()
        if g_temp > MAX_GPU_TEMP:
            log_step(f"GPU quente ({g_temp}°C). Cooldown 30s...")
            time.sleep(30)

        nome_final = _build_output_name(tipocorte, corte, idx)
        cut_start = datetime.now()

        titulo = corte.get("desc") or f"corte_{idx}"
        ini = corte["ini"]
        fim = corte.get("fim")
        dur = corte["dur_mmss"]
        janela = f"{ini} -> {fim}" if fim else f"{ini}+{dur}"

        log_step(f"Corte {idx}/{total} INICIO: {titulo} [{janela}]")

        modo = "-"
        status = "ERRO"
        debug_tail = ""

        try:
            modo, out_trecho, saida_path, _section = realizar_corte(
                url_youtube=url_youtube,
                corte=corte,
                nome_saida=nome_final,
                destino_local=pasta_local_final,
                tipocorte=tipocorte
            )
            status = "OK"
            debug_tail = (out_trecho or "")[-1500:]
        except Exception as e:
            debug_tail = str(e)[-1500:]
            log_step(f"Corte {idx}/{total} FALHOU: {e}")

        elapsed = (datetime.now() - cut_start).total_seconds()
        log_step(f"Corte {idx}/{total} FIM: {status} modo={modo} tempo={fmt_td(elapsed)}")

        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"| {idx} | {titulo} | {modo} | {status} | {fmt_td(elapsed)} |\n")
            if debug_tail:
                log.write(f"\n<details><summary>Debug corte #{idx}</summary>\n\n```\n{debug_tail}\n```\n</details>\n\n")

        time.sleep(COOL_DOWN_TIME)

    upload_drive_arquivo_a_arquivo(pasta_local_final, pasta_drive_final)

    total_s = (datetime.now() - pipeline_start).total_seconds()
    log_step(f"Pipeline FIM. Tempo total: {fmt_td(total_s)}. Log: {log_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-path", required=True)
    args = parser.parse_args()
    iniciar_processamento(args.event_path)
