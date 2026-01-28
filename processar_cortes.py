import os, subprocess, re, sys

# --- CONFIGURAÇÕES DO ENGENHEIRO ---
# Usando o drive F: conforme planejado para não lotar o C:
PASTA_LOCAL = "F:/Cortes_midia" 
# Certifique-se de que o nome 'meu_drive' seja o mesmo que você usará no 'rclone config'
DRIVE_REMOTO = "meu_drive:/Cortes_Midia" 

def realizar_corte(url, inicio, duracao, nome_saida):
    if not os.path.exists(PASTA_LOCAL): os.makedirs(PASTA_LOCAL)
    caminho_arquivo = os.path.join(PASTA_LOCAL, f"{nome_saida}.mp4")
    
    # Extrai as URLs de stream para evitar download do arquivo gigante
    print(f"🔍 Buscando streams para: {nome_saida}...")
    cmd_url = f'yt-dlp -g -f "bestvideo+bestaudio/best" {url}'
    urls = subprocess.check_output(cmd_url, shell=True).decode().split('\n')
    
    # Comando FFmpeg otimizado (Stream Copy) - Não reencoda, apenas corta
    ffmpeg_cmd = [
        'ffmpeg', '-y', '-ss', inicio, '-t', duracao, '-i', urls[0].strip(),
        '-ss', inicio, '-t', duracao, '-i', urls[1].strip(),
        '-map', '0:v', '-map', '1:a', '-c', 'copy', caminho_arquivo
    ]
    subprocess.run(ffmpeg_cmd, check=True)
    return caminho_arquivo

def executar_fluxo(relatorio, url_video):
    # Regex robusto para capturar os padrões do Gemini
    padrao = r"\[(\d{2}:\d{2}:\d{2})\].*?Duração:\s(\d{2}:\d{2})\)\nHook:\s\"(.*?)\""
    matches = re.findall(padrao, relatorio, re.DOTALL)
    
    for inicio, duracao, titulo in matches:
        # Limpeza de nome para o Windows
        nome_slug = re.sub(r'[^\w\s-]', '', titulo).replace(' ', '_')[:40]
        nome_final = f"{nome_slug}__{inicio.replace(':', '-')}"
        
        print(f"🎬 Processando Corte: {titulo}")
        try:
            # Adicionamos "00:" na duração para o formato HH:MM:SS
            realizar_corte(url_video, inicio, f"00:{duracao}", nome_final)
        except Exception as e:
            print(f"❌ Erro ao processar este corte: {e}")

    # Sincronização final com Google Drive
    print("☁️ Iniciando upload para o Google Drive...")
    subprocess.run(['rclone', 'copy', PASTA_LOCAL, DRIVE_REMOTO], check=True)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        executar_fluxo(sys.argv[1], sys.argv[2])
