from __future__ import annotations

import csv
import json
import logging
import re
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import pandas as pd
import requests

def format_money_br(v: Decimal) -> str:
    v = v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    s = f"{v:,.2f}"              
    return s.replace(",", "X").replace(".", ",").replace("X", ".") 


LOGGER = logging.getLogger("teste1.2")

MANIFEST_1_1_FILENAME = "Ultimos_3_trimestres.json"
CHUNK_SIZE_CSV = 200_000


@dataclass(frozen=True, order=True)
class TrimestreRef:
    ano: int
    trimestre: int

    def rotulo(self) -> str:
        return f"{self.trimestre}T{self.ano}"


# =========================
# Pastas
# =========================
def raiz_teste1() -> Path:
    return Path(__file__).resolve().parents[1]


def pasta_documentos() -> Path:
    return raiz_teste1() / "Documentos"


def pasta_dados() -> Path:
    return raiz_teste1() / "Dados"


def pasta_extraido() -> Path:
    return pasta_dados() / "Extraído"


def pasta_normal() -> Path:
    return pasta_dados() / "Normal"


def pasta_saida() -> Path:
    return pasta_dados() / "Saída"


# =========================
# Logging
# =========================
def configurar_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )


# =========================
# Helpers
# =========================
def normalizar_texto(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper()


def normalizar_coluna(col: str) -> str:
    col = normalizar_texto(str(col))
    col = re.sub(r"[^A-Z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def apenas_digitos(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def analisar_decimal_br(valor: object) -> Optional[Decimal]:
    if valor is None:
        return None

    if isinstance(valor, (int, float, Decimal)):
        try:
            return Decimal(str(valor))
        except InvalidOperation:
            return None

    s = str(valor).strip()
    if not s:
        return None

    s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def detectar_delimitador(amostra: str) -> str:
    candidatos = [";", ",", "\t", "|"]
    cont = {c: amostra.count(c) for c in candidatos}
    melhor = max(cont, key=cont.get)
    return melhor if cont[melhor] > 0 else ";"


def despesa_eventos_sinistros(texto: str) -> bool:
    t = normalizar_texto(texto)

    if ("EVENTO" in t) or ("SINISTRO" in t):
        if "RECEITA" in t:
            return False
        return True

    return False


# =========================
# I/O
# =========================
def baixar_arquivo(url: str, destino: Path, timeout_s: int = 60) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".part")

    if destino.exists():
        return

    erro: Optional[Exception] = None
    for tentativa in range(1, 6):
        try:
            with requests.get(url, stream=True, timeout=timeout_s) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            tmp.replace(destino)
            return
        except Exception as e:
            erro = e
            espera = min(2 ** tentativa, 20)
            time.sleep(espera)

    raise RuntimeError(f"Falha ao baixar {url}: {erro}") from erro


def extrair_zip_seguro(zip_path: Path, destino_dir: Path) -> None:
    destino_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if not info.filename or info.filename.endswith("/"):
                continue

            alvo = (destino_dir / info.filename).resolve()
            if not str(alvo).startswith(str(destino_dir.resolve())):
                continue

            alvo.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(alvo, "wb") as dst:
                dst.write(src.read())


# =========================
# Leitura
# =========================
def ler_manifesto_1_1() -> Dict:
    path = pasta_documentos() / MANIFEST_1_1_FILENAME
    if not path.exists():
        raise FileNotFoundError(f"Manifesto não encontrado: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_arquivos_tabulares(root: Path) -> Iterator[Path]:
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in (".csv", ".txt", ".xlsx"):
            yield p


def iter_csv_chunks(path: Path, chunksize: int) -> Iterator[pd.DataFrame]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        head = "".join([f.readline() for _ in range(8)])
    sep = detectar_delimitador(head)

    try:
        yield from pd.read_csv(
            path,
            sep=sep,
            dtype=str,
            chunksize=chunksize,
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip",
        )
    except UnicodeDecodeError:
        yield from pd.read_csv(
            path,
            sep=sep,
            dtype=str,
            chunksize=chunksize,
            encoding="latin1",
            engine="python",
            on_bad_lines="skip",
        )


def iter_xlsx_frames(path: Path) -> Iterator[pd.DataFrame]:
    xls = pd.ExcelFile(path, engine="openpyxl")
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, dtype=str, engine="openpyxl")
        yield df


# =========================
# Detecção de colunas (CORRIGIDO)
# =========================
def escolher_colunas(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    orig_cols = [str(c) for c in df.columns]
    norm_map = {c: normalizar_coluna(c) for c in orig_cols}
    inv = {v: k for k, v in norm_map.items()}

    def pick(opcoes: List[str]) -> Optional[str]:
        for o in opcoes:
            if o in inv:
                return inv[o]
        return None

    return {
        "reg_ans": pick(["REG_ANS", "REGANS", "REGISTRO_ANS"]),
        "conta": pick(["CD_CONTA_CONTABIL", "COD_CONTA_CONTABIL", "CD_CONTA"]),
        "descricao": pick(["DESCRICAO", "DS_CONTA", "DESCRICAO_CONTA", "NM_CONTA", "CONTA", "ITEM", "DS_ITEM"]),
        "valor": pick(["VL_SALDO_FINAL", "VL_VALOR", "VALOR", "VLR", "VL"]),
        "data": pick(["DATA", "DT_REFERENCIA", "DT_COMPETENCIA"]),
    }


def filtrar_eventos_sinistros(df: pd.DataFrame, descricao_col: Optional[str]) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=bool)

    if descricao_col and descricao_col in df.columns:
        s = df[descricao_col].fillna("").astype(str)
        return s.map(despesa_eventos_sinistros)

    cols_texto = [c for c in df.columns if df[c].dtype == "object"][:8]
    if not cols_texto:
        return pd.Series([False] * len(df), dtype=bool)

    joined = df[cols_texto].fillna("").astype(str).agg(" ".join, axis=1)
    return joined.map(despesa_eventos_sinistros)


def agregar_normalizado(
    df: pd.DataFrame,
    tref: TrimestreRef,
    erros: List[Dict[str, str]],
    origem: str,
) -> Dict[str, Decimal]:
    """ Agrega: REG_ANS -> soma(valor) para despesas de Eventos/Sinistros. Regra anti-double-count: Se existir CD_CONTA_CONTABIL == "41", usa APENAS essa conta (total). Senão, cai para heurística por DESCRICAO."""
    if df.empty:
        return {}

    cols = escolher_colunas(df)
    reg_col = cols["reg_ans"]
    conta_col = cols["conta"]
    desc_col = cols["descricao"]
    valor_col = cols["valor"]

    if not reg_col or not valor_col:
        cols_norm = [normalizar_coluna(c) for c in df.columns[:60]]
        erros.append({
            "tipo": "colunas_nao_reconhecidas",
            "ano": str(tref.ano),
            "trimestre": str(tref.trimestre),
            "detalhe": f"{Path(origem).name} | reg_ans={reg_col} conta={conta_col} desc={desc_col} valor={valor_col} | cols={cols_norm}",
        })
        return {}

    # Máscara base (EVENTO/SINISTRO)
    mask = filtrar_eventos_sinistros(df, desc_col)

    # Anti double-count: se existir conta 41, fica só nela
    conta = None
    if conta_col and conta_col in df.columns:
        conta = df[conta_col].fillna("").astype(str).str.strip()

    mask_desc = filtrar_eventos_sinistros(df, desc_col)

    # Se existe 41, usa SÓ ela (não depende de texto)
    if conta is not None and (conta == "41").any():
        mask = (conta == "41")
    else:
        # Senão, usa heurística por descrição e restringe ao bloco de contas relevante
        mask = mask_desc
        if conta is not None:
            mask = mask & conta.str.match(r"^[47]")

    if mask is None or not bool(mask.any()):
        return {}

    sub = df.loc[mask, [reg_col, valor_col]].copy()
    sub["REG_ANS"] = sub[reg_col].fillna("").astype(str).str.strip()
    sub["VAL_DEC"] = sub[valor_col].map(analisar_decimal_br)

    # Remove valores inválidos
    inval = sub["VAL_DEC"].isna()
    if bool(inval.any()):
        erros.append({
            "tipo": "valor_invalido",
            "ano": str(tref.ano),
            "trimestre": str(tref.trimestre),
            "detalhe": f"{Path(origem).name} | exemplos_raw={sub.loc[inval, valor_col].head(3).tolist()}",
        })
        sub = sub.loc[~inval]

    if sub.empty:
        return {}

    # Groupby (bem mais rápido que iterrows)
    g = sub.groupby("REG_ANS")["VAL_DEC"].sum()

    out: Dict[str, Decimal] = {}
    for reg_ans, val in g.items():
        out[str(reg_ans)] = Decimal(str(val))

    return out


def processar_trimestre(
    extract_dir: Path,
    tref: TrimestreRef,
    erros: List[Dict[str, str]]
) -> Dict[str, Decimal]:
    total: Dict[str, Decimal] = {}

    for arquivo in iter_arquivos_tabulares(extract_dir):
        ext = arquivo.suffix.lower()
        try:
            if ext in (".csv", ".txt"):
                for chunk in iter_csv_chunks(arquivo, CHUNK_SIZE_CSV):
                    part = agregar_normalizado(chunk, tref, erros, origem=str(arquivo))
                    for k, v in part.items():
                        total[k] = total.get(k, Decimal("0")) + v

            elif ext == ".xlsx":
                for frame in iter_xlsx_frames(arquivo):
                    part = agregar_normalizado(frame, tref, erros, origem=str(arquivo))
                    for k, v in part.items():
                        total[k] = total.get(k, Decimal("0")) + v

        except Exception as e:
            erros.append({
                "tipo": "erro_leitura_arquivo",
                "ano": str(tref.ano),
                "trimestre": str(tref.trimestre),
                "detalhe": f"{arquivo.name} | {e}",
            })

    return total


def salvar_csv_intermediario(tref: TrimestreRef, agg: Dict[str, Decimal]) -> Path:
    pasta_normal().mkdir(parents=True, exist_ok=True)
    out_path = pasta_normal() / f"despesas_eventos_sinistros_{tref.rotulo()}.csv"

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")

        w.writerow(["REG_ANS", "Trimestre", "Ano", "ValorDespesas"])
        for reg_ans, val in sorted(agg.items()):
            w.writerow([reg_ans, f"{tref.trimestre}T", tref.ano, str(val)])

    return out_path


def salvar_relatorio_erros(erros: List[Dict[str, str]]) -> Path:
    pasta_documentos().mkdir(parents=True, exist_ok=True)
    out_path = pasta_documentos() / "relatorio_erros.csv"

    campos = ["tipo", "ano", "trimestre", "detalhe"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        for e in erros:
            w.writerow({c: e.get(c, "") for c in campos})

    return out_path


def main() -> None:
    configurar_logging()

    pasta_extraido().mkdir(parents=True, exist_ok=True)
    pasta_normal().mkdir(parents=True, exist_ok=True)
    pasta_saida().mkdir(parents=True, exist_ok=True)

    manifesto = ler_manifesto_1_1()
    ultimos = manifesto.get("ultimos_3_trimestres", [])
    if not ultimos:
        raise RuntimeError("Manifesto vazio.")

    LOGGER.info("Processando %d trimestres...\n", len(ultimos))

    erros: List[Dict[str, str]] = []

    for item in ultimos:
        ano = int(item["ano"])
        tri = int(item["trimestre"])
        urls: List[str] = list(item.get("zip_urls", []))

        tref = TrimestreRef(ano=ano, trimestre=tri)
        rotulo = tref.rotulo()

        LOGGER.info("Trimestre %s:", rotulo)

        trimestre_dir = pasta_extraido() / rotulo
        trimestre_dir.mkdir(parents=True, exist_ok=True)

        zip_paths: List[Path] = []
        for url in urls:
            nome = url.split("/")[-1]
            destino = trimestre_dir / nome
            LOGGER.info("  Baixando: %s", nome)
            baixar_arquivo(url, destino)
            zip_paths.append(destino)

        for zp in zip_paths:
            LOGGER.info("  Extraindo: %s", zp.name)
            try:
                extrair_zip_seguro(zp, trimestre_dir)
            except Exception as e:
                erros.append({
                    "tipo": "erro_extracao_zip",
                    "ano": str(ano),
                    "trimestre": str(tri),
                    "detalhe": f"{zp.name} | {e}",
                })

        LOGGER.info("  Processando arquivos...")
        agg = processar_trimestre(trimestre_dir, tref, erros)

        total_valor = sum(agg.values(), Decimal("0"))
        LOGGER.info("  Operadoras agregadas: %d", len(agg))
        LOGGER.info("  Total de despesas (Eventos/Sinistros) no trimestre: R$ %s\n", format_money_br(total_valor))

        out_csv = salvar_csv_intermediario(tref, agg)
        LOGGER.info("  Salvo: %s\n", out_csv.name)

    rel = salvar_relatorio_erros(erros)
    LOGGER.info("Relatório de erros: %s", rel.name)


if __name__ == "__main__":
    main()
