from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# =========================
# Configurações
# =========================
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

LOGGER = logging.getLogger("teste1.1")


@dataclass(frozen=True, order=True)
class TrimestreRef:
    """Representa um trimestre (ano, trimestre)."""
    ano: int
    trimestre: int

    def rotulo(self) -> str:
        return f"{self.trimestre}T{self.ano}"


# Padrões de nomenclatura dos ZIPs
_PADROES: List[re.Pattern[str]] = [
    re.compile(r"^(?P<t>[1-4])T(?P<a>\d{4})\.zip$", re.IGNORECASE),
    re.compile(r"^(?P<a>\d{4})_(?P<t>[1-4])_trimestre\.zip$", re.IGNORECASE),
    re.compile(r"^\d{8}_(?P<a>\d{4})_(?P<t>[1-4])_trimestre\.zip$", re.IGNORECASE),
    re.compile(r"^\d{8}_(?P<t>[1-4])T(?P<a>\d{4})\.zip$", re.IGNORECASE),
    re.compile(r"^(?P<a>\d{4})-(?P<t>[1-4])t\.zip$", re.IGNORECASE),
]


def configurar_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )


def raiz_teste1() -> Path:
    return Path(__file__).resolve().parents[1]


def obter_texto(url: str, timeout_s: int = 30) -> str:
    """GET com retry automático."""
    erro: Optional[Exception] = None
    for tentativa in range(1, 6):
        try:
            resp = requests.get(url, timeout=timeout_s)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            erro = e
            espera = min(2 ** tentativa, 20)
            time.sleep(espera)
    raise RuntimeError(f"Falha ao acessar {url}: {erro}") from erro


def extrair_hrefs(html: str) -> List[str]:
    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    saida: List[str] = []
    vistos = set()
    for h in hrefs:
        if h in ("../", "./"):
            continue
        if h.startswith("?") or h.startswith("#"):
            continue
        if h not in vistos:
            vistos.add(h)
            saida.append(h)
    return saida


def listar_itens(url_dir: str) -> Tuple[List[str], List[str]]:
    """Retorna (subdiretorios, arquivos_zip)."""
    html = obter_texto(url_dir)
    hrefs = extrair_hrefs(html)

    subdirs = [h for h in hrefs if h.endswith("/")]
    zips = [h for h in hrefs if h.lower().endswith(".zip")]

    return subdirs, zips


def extrai_trimestre(nome_zip: str) -> Optional[TrimestreRef]:
    """Extrai (ano, trimestre) do nome do ZIP."""
    nome = Path(nome_zip).name
    for padrao in _PADROES:
        m = padrao.match(nome)
        if m:
            t = int(m.group("t"))
            a = int(m.group("a"))
            if 1 <= t <= 4:
                return TrimestreRef(ano=a, trimestre=t)
    return None


def coletar_zips_da_ans() -> Tuple[Dict[TrimestreRef, List[str]], List[str]]:
    """Coleta todos os ZIPs do FTP da ANS."""
    agrupado: Dict[TrimestreRef, List[str]] = {}
    ignorados: List[str] = []

    # ZIPs na raiz
    subdirs_base, zips_base = listar_itens(BASE_URL)
    for z in zips_base:
        tref = extrai_trimestre(z)
        url_zip = f"{BASE_URL}{z}"
        if tref is None:
            ignorados.append(url_zip)
            continue
        agrupado.setdefault(tref, []).append(url_zip)

    # ZIPs em subpastas YYYY/
    for d in subdirs_base:
        if not re.fullmatch(r"\d{4}/", d):
            continue
        url_ano = f"{BASE_URL}{d}"
        _, zips_ano = listar_itens(url_ano)
        for z in zips_ano:
            tref = extrai_trimestre(z)
            url_zip = f"{url_ano}{z}"
            if tref is None:
                ignorados.append(url_zip)
                continue
            agrupado.setdefault(tref, []).append(url_zip)

    return agrupado, ignorados


def selecionar_ultimos_3(agrupado: Dict[TrimestreRef, List[str]]) -> Dict[TrimestreRef, List[str]]:
    """Seleciona os 3 trimestres mais recentes."""
    if not agrupado:
        raise RuntimeError("Nenhum ZIP encontrado.")
    trimestres_ordenados = sorted(agrupado.keys())
    ultimos = trimestres_ordenados[-3:]
    return {t: agrupado[t] for t in ultimos}


def salvar_material(ultimos: Dict[TrimestreRef, List[str]], ignorados: List[str]) -> Path:
    """Salva JSON com os últimos 3 trimestres."""
    base = raiz_teste1()
    doc_dir = base / "Documentos"
    doc_dir.mkdir(parents=True, exist_ok=True)

    out_path = doc_dir / "Ultimos_3_trimestres.json"

    payload = {
        "base_url": BASE_URL,
        "ultimos_3_trimestres": [
            {
                "ano": t.ano,
                "trimestre": t.trimestre,
                "rotulo": t.rotulo(),
                "zip_urls": sorted(urls),
            }
            for t, urls in sorted(ultimos.items())
        ],
        "arquivos_ignorados": sorted(ignorados),
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return out_path


def main() -> None:
    configurar_logging()

    agrupado, ignorados = coletar_zips_da_ans()
    ultimos = selecionar_ultimos_3(agrupado)

    LOGGER.info("Total de trimestres reconhecidos: %d", len(agrupado))
    LOGGER.info("Total de arquivos ignorados: %d", len(ignorados))

    LOGGER.info("Últimos 3 trimestres:")
    for t in sorted(ultimos.keys()):
        LOGGER.info("  %s -> %d arquivo(s)", t.rotulo(), len(ultimos[t]))

    out_path = salvar_material(ultimos, ignorados)
    LOGGER.info("Salvo: %s", out_path)


if __name__ == "__main__":
    main()
