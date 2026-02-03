from __future__ import annotations

import csv
import logging
import re
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

LOGGER = logging.getLogger("teste1.3")

CADOP_URL = (
    "https://dadosabertos.ans.gov.br/FTP/PDA/"
    "operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
)


# =========================
# Pastas
# =========================
def raiz_teste1() -> Path:
    return Path(__file__).resolve().parents[1]


def pasta_documentos() -> Path:
    return raiz_teste1() / "Documentos"


def pasta_dados() -> Path:
    return raiz_teste1() / "Dados"


def pasta_normal() -> Path:
    return pasta_dados() / "Normal"


def pasta_saida() -> Path:
    return pasta_dados() / "Saída"


# =========================
# Logging
# =========================
def configurar_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


# =========================
# Helpers
# =========================
def apenas_digitos(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def normalizar_texto(s: str) -> str:
    return (s or "").strip().upper()


def parse_decimal_any(v: object) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    if not s:
        return None

    try:
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(".", "").replace(",", ".")
        return Decimal(s)
    except InvalidOperation:
        return None


def format_money_br(v: Decimal) -> str:
    v = v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    s = f"{v:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def baixar_arquivo(url: str, destino: Path, timeout_s: int = 60) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".part")
    if destino.exists():
        return

    with requests.get(url, stream=True, timeout=timeout_s) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    tmp.replace(destino)


# =========================
# CADOP (REG_ANS -> CNPJ/Razao)
# =========================
def carregar_cadop() -> pd.DataFrame:
    """Baixa e carrega o cadastro de operadoras da ANS."""
    pasta_documentos().mkdir(parents=True, exist_ok=True)
    cadop_path = pasta_documentos() / "Relatorio_cadop.csv"
    
    if not cadop_path.exists():
        LOGGER.info("Baixando CADOP...")
        baixar_arquivo(CADOP_URL, cadop_path)

    try:
        df = pd.read_csv(cadop_path, sep=";", dtype=str, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(cadop_path, sep=";", dtype=str, encoding="latin1")

    # Detecta colunas (aceita variações)
    cols = {c.strip().upper().replace("_", ""): c for c in df.columns}
    
    # REG_ANS pode ser: REGISTRO_ANS, REG_ANS, REGANS, REGISTRO_OPERADORA
    reg_col = (
        cols.get("REGISTROANS") or 
        cols.get("REGANS") or 
        cols.get("REGISTROOPERADORA")
    )
    
    # CNPJ
    cnpj_col = cols.get("CNPJ")
    
    # Razão Social pode ter variações
    razao_col = (
        cols.get("RAZAOSOCIAL") or 
        cols.get("NOMEEMPRESARIAL") or 
        cols.get("RAZAO") or
        cols.get("NOME")
    )

    if not reg_col or not cnpj_col or not razao_col:
        raise RuntimeError(f"CADOP sem colunas esperadas. Encontradas: {list(df.columns)[:30]}")

    out = df[[reg_col, cnpj_col, razao_col]].copy()
    out.columns = ["REG_ANS", "CNPJ", "RazaoSocial"]
    out["REG_ANS"] = out["REG_ANS"].fillna("").astype(str).str.strip()
    out["CNPJ"] = out["CNPJ"].fillna("").astype(str).map(apenas_digitos)
    out["RazaoSocial"] = out["RazaoSocial"].fillna("").astype(str).str.strip()
    out = out[out["REG_ANS"] != ""].drop_duplicates(subset=["REG_ANS"], keep="first")
    
    return out


# =========================
# Consolidação
# =========================
def listar_intermediarios() -> List[Path]:
    if not pasta_normal().exists():
        return []
    return sorted(pasta_normal().glob("despesas_eventos_sinistros_*.csv"))


def ler_intermediario(p: Path) -> pd.DataFrame:
    """Lê CSV intermediário do teste 1.2."""
    df = pd.read_csv(p, sep=";", dtype=str, encoding="utf-8")
    df.columns = [c.strip() for c in df.columns]
    
    if "REG_ANS" not in df.columns:
        raise RuntimeError(f"{p.name} não tem coluna REG_ANS.")
    
    df["REG_ANS"] = df["REG_ANS"].fillna("").astype(str).str.strip()

    # Extrai trimestre/ano do nome ou da coluna
    if "Trimestre" not in df.columns or "Ano" not in df.columns:
        m = re.search(r"_(\d)T(\d{4})\.csv$", p.name)
        if not m:
            raise RuntimeError(f"Não consegui inferir trimestre/ano do nome: {p.name}")
        df["Trimestre"] = f"{m.group(1)}T"
        df["Ano"] = m.group(2)
    else:
        df["Trimestre"] = df["Trimestre"].fillna("").astype(str).str.strip()
        df["Ano"] = df["Ano"].fillna("").astype(str).str.strip()

    if "ValorDespesas" not in df.columns:
        raise RuntimeError(f"{p.name} não tem coluna ValorDespesas.")

    df["ValorDec"] = df["ValorDespesas"].map(parse_decimal_any)
    return df


def consolidar() -> Tuple[pd.DataFrame, List[Dict[str, str]]]:
    """Consolida CSVs intermediários e faz JOIN com CADOP."""
    rel_incons: List[Dict[str, str]] = []

    arquivos = listar_intermediarios()
    if not arquivos:
        raise RuntimeError("Nenhum CSV intermediário encontrado em Dados/Normal.")

    LOGGER.info("Lendo %d arquivos intermediários...", len(arquivos))
    frames = []
    for p in arquivos:
        df = ler_intermediario(p)
        df["Fonte"] = p.name
        frames.append(df)

    base = pd.concat(frames, ignore_index=True)

    # Valores inválidos
    invalid = base["ValorDec"].isna()
    if bool(invalid.any()):
        for x in base.loc[invalid].head(20).itertuples(index=False):
            rel_incons.append({
                "tipo": "valor_invalido",
                "chave": f"REG_ANS={getattr(x,'REG_ANS','')}",
                "detalhe": f"fonte={getattr(x,'Fonte','')} raw={getattr(x,'ValorDespesas','')}",
            })
        base = base.loc[~invalid].copy()

    # JOIN com CADOP
    cadop = carregar_cadop()
    merged = base.merge(cadop, on="REG_ANS", how="left")

    # REG_ANS sem cadastro
    missing = merged["CNPJ"].isna() | (merged["CNPJ"].fillna("").astype(str).str.strip() == "")
    if bool(missing.any()):
        for x in merged.loc[missing].head(50).itertuples(index=False):
            rel_incons.append({
                "tipo": "reg_ans_sem_cadop",
                "chave": f"REG_ANS={getattr(x,'REG_ANS','')}",
                "detalhe": f"fonte={getattr(x,'Fonte','')}",
            })

    # Valores zero/negativos
    zero_or_neg = merged["ValorDec"] <= Decimal("0")
    if bool(zero_or_neg.any()):
        for x in merged.loc[zero_or_neg].head(50).itertuples(index=False):
            rel_incons.append({
                "tipo": "valor_zero_ou_negativo",
                "chave": f"CNPJ={getattr(x,'CNPJ','')} REG_ANS={getattr(x,'REG_ANS','')}",
                "detalhe": f"tri={getattr(x,'Trimestre','')} ano={getattr(x,'Ano','')} valor={getattr(x,'ValorDec','')}",
            })

    # CNPJ com múltiplas razões sociais
    tmp = merged.dropna(subset=["CNPJ", "RazaoSocial"]).copy()
    if not tmp.empty:
        nun = tmp.groupby("CNPJ")["RazaoSocial"].nunique()
        amb = nun[nun > 1].index.tolist()
        for cnpj in amb[:200]:
            razoes = sorted(tmp.loc[tmp["CNPJ"] == cnpj, "RazaoSocial"].unique().tolist())
            rel_incons.append({
                "tipo": "cnpj_com_razoes_diferentes",
                "chave": f"CNPJ={cnpj}",
                "detalhe": " | ".join(razoes[:6]) + (" ..." if len(razoes) > 6 else ""),
            })

    # Monta CSV final
    out = merged.copy()
    out["CNPJ"] = out["CNPJ"].fillna("").astype(str)
    out["RazaoSocial"] = out["RazaoSocial"].fillna("").astype(str)
    out["ValorDespesas"] = out["ValorDec"].map(
        lambda d: str(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    )

    final = out[["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]].copy()

    return final, rel_incons


def salvar_csv_final(df: pd.DataFrame) -> Path:
    pasta_saida().mkdir(parents=True, exist_ok=True)
    out_path = pasta_saida() / "consolidado_despesas.csv"
    df.to_csv(out_path, sep=";", index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
    return out_path


def salvar_relatorio_inconsistencias(rel: List[Dict[str, str]]) -> Path:
    pasta_documentos().mkdir(parents=True, exist_ok=True)
    out_path = pasta_documentos() / "relatorio_inconsistencias.csv"
    campos = ["tipo", "chave", "detalhe"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos, delimiter=";")
        w.writeheader()
        for r in rel:
            w.writerow({c: r.get(c, "") for c in campos})
    return out_path


def zipar(csv_path: Path) -> Path:
    out_zip = pasta_saida() / "consolidado_despesas.zip"
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, arcname=csv_path.name)
    return out_zip


def main() -> None:
    configurar_logging()

    LOGGER.info("Consolidando trimestres...\n")
    final, rel = consolidar()

    # Métricas
    total = sum((Decimal(v) for v in final["ValorDespesas"].tolist()), Decimal("0"))
    LOGGER.info("Linhas no consolidado: %d", len(final))
    LOGGER.info("Total consolidado: R$ %s\n", format_money_br(total))

    out_csv = salvar_csv_final(final)
    out_rel = salvar_relatorio_inconsistencias(rel)
    out_zip = zipar(out_csv)

    LOGGER.info("CSV final: %s", out_csv.name)
    LOGGER.info("Relatório de inconsistências: %s", out_rel.name)
    LOGGER.info("ZIP final: %s", out_zip.name)


if __name__ == "__main__":
    main()
