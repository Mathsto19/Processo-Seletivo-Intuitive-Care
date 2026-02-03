from __future__ import annotations

import re
import time
import zipfile
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests


BASE_DIR_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"


def _http_get(url: str, timeout_s: int = 60) -> requests.Response:
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r


def _download_stream(url: str, destino: Path, timeout_s: int = 120) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".part")

    with requests.get(url, stream=True, timeout=timeout_s) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    tmp.replace(destino)


def _listar_links(base_dir_url: str) -> list[str]:
    html = _http_get(base_dir_url, timeout_s=60).text
    links = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    # mantém apenas arquivos, remove navegação
    files = []
    for href in links:
        if href.startswith("?") or href.startswith("/") or href.startswith("#"):
            continue
        if href.endswith("/"):
            continue
        files.append(href)
    return files


def _escolher_arquivo_cadop(base_dir_url: str) -> Tuple[str, str]:
    """ Retorna (url_arquivo, tipo) onde tipo ∈ {"csv","zip"}. Tenta priorizar Relatorio_cadop.csv; se não existir, pega algum .csv; se não existir, pega .zip."""
    files = _listar_links(base_dir_url)

    prefer_csv = None
    for f in files:
        if f.lower() == "relatorio_cadop.csv":
            prefer_csv = f
            break

    if prefer_csv:
        return (urljoin(base_dir_url, prefer_csv), "csv")

    csvs = [f for f in files if f.lower().endswith(".csv")]
    if csvs:
        # se tiver mais de um, escolhe o que mais parece "cadop"
        csvs.sort(key=lambda x: ("cadop" not in x.lower(), len(x)))
        return (urljoin(base_dir_url, csvs[0]), "csv")

    zips = [f for f in files if f.lower().endswith(".zip")]
    if zips:
        zips.sort(key=lambda x: ("cadop" not in x.lower(), len(x)))
        return (urljoin(base_dir_url, zips[0]), "zip")

    raise RuntimeError("Não encontrei arquivo .csv ou .zip no diretório de operadoras ativas.")


def _extrair_primeiro_csv(zip_path: Path, destino_csv: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as z:
        nomes = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not nomes:
            raise RuntimeError("ZIP baixado não contém CSV.")
        # prioriza algo com cadop no nome
        nomes.sort(key=lambda x: ("cadop" not in x.lower(), len(x)))
        nome = nomes[0]
        destino_csv.parent.mkdir(parents=True, exist_ok=True)
        with z.open(nome) as src, destino_csv.open("wb") as out:
            out.write(src.read())


def baixar_cadop(destino_csv: Path, forcar: bool = False, tentativas: int = 3) -> Path:
    """Baixa o cadastro de operadoras ativas e salva como CSV no caminho destino_csv. Retorna o caminho final do CSV."""
    destino_csv.parent.mkdir(parents=True, exist_ok=True)

    if destino_csv.exists() and destino_csv.stat().st_size > 0 and not forcar:
        return destino_csv

    ultimo_erro: Optional[Exception] = None

    for i in range(1, tentativas + 1):
        try:
            url, tipo = _escolher_arquivo_cadop(BASE_DIR_URL)

            if tipo == "csv":
                _download_stream(url, destino_csv, timeout_s=180)
                return destino_csv

            # tipo zip
            zip_path = destino_csv.with_suffix(".zip")
            _download_stream(url, zip_path, timeout_s=240)
            _extrair_primeiro_csv(zip_path, destino_csv)
            try:
                zip_path.unlink(missing_ok=True)
            except Exception:
                pass
            return destino_csv

        except Exception as e:
            ultimo_erro = e
            time.sleep(1.25 * i)

    raise RuntimeError(f"Falha ao baixar CADOP. Último erro: {ultimo_erro}")


if __name__ == "__main__":
    pasta_script = Path(__file__).resolve().parent
    destino = pasta_script / "Dados" / "Entradas" / "operadoras_cadastro.csv"
    print(f"Baixando cadastro para: {destino}")
    final = baixar_cadop(destino, forcar=False)
    print(f"Cadastro salvo em: {final}")
