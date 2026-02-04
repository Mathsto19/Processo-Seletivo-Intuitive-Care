from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


_CNPJ_RE = re.compile(r"\D+")


def normalizar_cnpj(valor: str) -> str:
    """Remove tudo que não é dígito do CNPJ."""
    return _CNPJ_RE.sub("", valor or "")


def validar_formato_cnpj(valor: str) -> bool:
    """Valida se CNPJ tem 14 dígitos."""
    c = normalizar_cnpj(valor)
    return len(c) == 14


def raiz_projeto() -> Path:
    return Path(__file__).resolve().parents[1]


def diretorio_data() -> Path:
    return raiz_projeto() / "Data"


def carregar_csvs() -> Dict[str, pd.DataFrame]:
    """Carrega os CSVs da pasta Data/"""
    base = diretorio_data()
    ops_path = base / "operadoras.csv"
    exp_path = base / "despesas.csv"
    agg_path = base / "agregados.csv"

    faltando = [str(p) for p in [ops_path, exp_path, agg_path] if not p.exists()]
    if faltando:
        raise RuntimeError(f"CSV(s) nao encontrado(s): {faltando}. Rode database.py para gerar os CSVs.")

    ops = pd.read_csv(ops_path, dtype=str, encoding="utf-8-sig")
    exp = pd.read_csv(exp_path, dtype=str, encoding="utf-8-sig")
    agg = pd.read_csv(agg_path, dtype=str, encoding="utf-8-sig")

    if "cnpj" in ops.columns:
        ops["cnpj"] = ops["cnpj"].map(normalizar_cnpj)

    if "cnpj" in exp.columns:
        exp["cnpj"] = exp["cnpj"].map(normalizar_cnpj)

    if "ano" in exp.columns:
        exp["ano"] = pd.to_numeric(exp["ano"], errors="coerce").astype("Int64")
    if "trimestre" in exp.columns:
        exp["trimestre"] = pd.to_numeric(exp["trimestre"], errors="coerce").astype("Int64")
    if "valor" in exp.columns:
        exp["valor"] = pd.to_numeric(exp["valor"], errors="coerce")

    if "total_despesas" in agg.columns:
        agg["total_despesas"] = pd.to_numeric(agg["total_despesas"], errors="coerce")
    if "qtd_operadoras" in agg.columns:
        agg["qtd_operadoras"] = pd.to_numeric(agg["qtd_operadoras"], errors="coerce").astype("Int64")
    if "media_por_operadora" in agg.columns:
        agg["media_por_operadora"] = pd.to_numeric(agg["media_por_operadora"], errors="coerce")

    return {"operadoras": ops, "despesas": exp, "agregados": agg}


try:
    DADOS = carregar_csvs()
    ERRO_STARTUP = ""
except Exception as e:
    DADOS = {}
    ERRO_STARTUP = str(e)


# Schemas Pydantic

class OperadoraOut(BaseModel):
    cnpj: str
    razao_social: str = ""
    registro_ans: str = ""
    modalidade: str = ""
    uf: str = ""


class DespesaOut(BaseModel):
    ano: int
    trimestre: int
    valor: float


class OperadorasPaginadas(BaseModel):
    data: List[OperadoraOut]
    total: int
    page: int
    limit: int
    total_pages: int


class TopOperadora(BaseModel):
    cnpj: str
    razao_social: str = ""
    uf: str = ""
    total_despesas: float


class AgregadoPorUF(BaseModel):
    uf: str
    total_despesas: float
    qtd_operadoras: int
    media_por_operadora: float


class EstatisticasOut(BaseModel):
    total_despesas: float
    media_por_operadora: float
    top_5_operadoras: List[TopOperadora]
    por_uf: List[AgregadoPorUF]


# App FastAPI

app = FastAPI(title="IntuitiveCare API", version="1.0.0")

origens_cors = os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")
origens_cors = [o.strip() for o in origens_cors if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origens_cors if origens_cors else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _garantir_dados_carregados() -> None:
    """Valida se os dados foram carregados no startup"""
    if ERRO_STARTUP:
        raise HTTPException(status_code=500, detail=f"Dados nao carregados: {ERRO_STARTUP}")
    if not DADOS:
        raise HTTPException(status_code=500, detail="Dados nao carregados.")

def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    return "" if s.lower() == "nan" else s

# Rotas

@app.get("/api/operadoras", response_model=OperadorasPaginadas)
def listar_operadoras(
    page: int = Query(1, ge=1, description="Numero da pagina"),
    limit: int = Query(10, ge=1, le=100, description="Registros por pagina"),
    q: Optional[str] = Query(None, description="Busca por razao social ou CNPJ"),
) -> OperadorasPaginadas:
    """Lista operadoras com paginação e filtro opcional."""
    _garantir_dados_carregados()
    ops = DADOS["operadoras"].copy()

    for col in ["cnpj", "razao_social", "registro_ans", "modalidade", "uf"]:
        if col not in ops.columns:
            ops[col] = ""

    if q:
        termo_busca = q.strip()
        cnpj_digitos = normalizar_cnpj(termo_busca)

        if cnpj_digitos:
            ops = ops[ops["cnpj"].astype(str).str.contains(cnpj_digitos, na=False)]
        else:
            ops = ops[ops["razao_social"].astype(str).str.contains(termo_busca, case=False, na=False)]

    ops = ops.sort_values("razao_social", ascending=True, na_position="last")

    total = int(len(ops))
    total_paginas = max(1, int(math.ceil(total / limit))) if total > 0 else 1

    inicio = (page - 1) * limit
    pagina_df = ops.iloc[inicio: inicio + limit].copy()

    dados_saida = [
        OperadoraOut(
            cnpj=str(r.get("cnpj", "")),
            razao_social=str(r.get("razao_social", "")),
            registro_ans=_safe_str(r.get("registro_ans", "")),
            modalidade=_safe_str(r.get("modalidade", "")),
            uf=_safe_str(r.get("uf", "")),
        )
        for r in pagina_df.to_dict(orient="records")
    ]

    return OperadorasPaginadas(
        data=dados_saida,
        total=total,
        page=page,
        limit=limit,
        total_pages=total_paginas,
    )


@app.get("/api/operadoras/{cnpj}", response_model=OperadoraOut)
def obter_operadora(cnpj: str) -> OperadoraOut:
    """Retorna detalhes de uma operadora por CNPJ."""
    _garantir_dados_carregados()

    cnpj_normalizado = normalizar_cnpj(cnpj)
    if not validar_formato_cnpj(cnpj_normalizado):
        raise HTTPException(status_code=422, detail="CNPJ invalido (precisa ter 14 digitos).")

    ops = DADOS["operadoras"].copy()
    if "cnpj" not in ops.columns:
        raise HTTPException(status_code=500, detail="CSV operadoras.csv sem coluna cnpj.")

    linha = ops[ops["cnpj"].astype(str) == cnpj_normalizado]
    if linha.empty:
        raise HTTPException(status_code=404, detail="Operadora nao encontrada.")

    registro = linha.iloc[0].to_dict()
    return OperadoraOut(
        cnpj=str(registro.get("cnpj", "")),
        razao_social=str(registro.get("razao_social", "")),
        registro_ans=str(registro.get("registro_ans", "")),
        modalidade=str(registro.get("modalidade", "")),
        uf=str(registro.get("uf", "")),
    )


@app.get("/api/operadoras/{cnpj}/despesas", response_model=List[DespesaOut])
def obter_despesas_operadora(cnpj: str) -> List[DespesaOut]:
    """Retorna histórico de despesas de uma operadora."""
    _garantir_dados_carregados()

    cnpj_normalizado = normalizar_cnpj(cnpj)
    if not validar_formato_cnpj(cnpj_normalizado):
        raise HTTPException(status_code=422, detail="CNPJ invalido (precisa ter 14 digitos).")

    despesas = DADOS["despesas"].copy()
    if not {"cnpj", "ano", "trimestre", "valor"}.issubset(set(despesas.columns)):
        raise HTTPException(status_code=500, detail="CSV despesas.csv nao tem colunas esperadas.")

    df = despesas[despesas["cnpj"].astype(str) == cnpj_normalizado].copy()
    if df.empty:
        return []

    df = df.dropna(subset=["ano", "trimestre", "valor"]).copy()
    df = df.sort_values(["ano", "trimestre"], ascending=True)

    resultado: List[DespesaOut] = []
    for r in df.to_dict(orient="records"):
        resultado.append(
            DespesaOut(
                ano=int(r["ano"]),
                trimestre=int(r["trimestre"]),
                valor=float(r["valor"]),
            )
        )
    return resultado


@app.get("/api/estatisticas", response_model=EstatisticasOut)
def obter_estatisticas() -> EstatisticasOut:
    """Retorna estatísticas agregadas: total, média, top 5 e distribuição por UF."""
    _garantir_dados_carregados()

    ops = DADOS["operadoras"].copy()
    despesas = DADOS["despesas"].copy()
    agregados = DADOS["agregados"].copy()

    if not {"cnpj", "valor"}.issubset(set(despesas.columns)):
        raise HTTPException(status_code=500, detail="CSV despesas.csv nao tem colunas esperadas.")

    despesas_validas = despesas.dropna(subset=["cnpj", "valor"]).copy()
    total_despesas = float(despesas_validas["valor"].sum()) if not despesas_validas.empty else 0.0

    por_operadora = despesas_validas.groupby("cnpj", as_index=False)["valor"].sum()
    por_operadora = por_operadora.rename(columns={"valor": "total_despesas"})
    media_por_operadora = float(por_operadora["total_despesas"].mean()) if not por_operadora.empty else 0.0

    if "cnpj" in ops.columns:
        ops_join = ops[["cnpj", "razao_social", "uf"]].copy()
    else:
        ops_join = pd.DataFrame(columns=["cnpj", "razao_social", "uf"])

    top = por_operadora.merge(ops_join, on="cnpj", how="left")
    top["razao_social"] = top["razao_social"].fillna("")
    top["uf"] = top["uf"].fillna("")
    top = top.sort_values("total_despesas", ascending=False).head(5)

    top_5 = [
        TopOperadora(
            cnpj=str(r.get("cnpj", "")),
            razao_social=str(r.get("razao_social", "")),
            uf=str(r.get("uf", "")),
            total_despesas=float(r.get("total_despesas", 0.0)),
        )
        for r in top.to_dict(orient="records")
    ]

    por_uf: List[AgregadoPorUF] = []
    if {"uf", "total_despesas", "qtd_operadoras", "media_por_operadora"}.issubset(set(agregados.columns)):
        if not agregados.empty:
            agregados_ordenados = agregados.sort_values("total_despesas", ascending=False)
            for r in agregados_ordenados.to_dict(orient="records"):
                por_uf.append(
                    AgregadoPorUF(
                        uf=str(r.get("uf", "")),
                        total_despesas=float(r.get("total_despesas", 0.0) or 0.0),
                        qtd_operadoras=int(r.get("qtd_operadoras", 0) or 0),
                        media_por_operadora=float(r.get("media_por_operadora", 0.0) or 0.0),
                    )
                )

    return EstatisticasOut(
        total_despesas=total_despesas,
        media_por_operadora=media_por_operadora,
        top_5_operadoras=top_5,
        por_uf=por_uf,
    )


@app.get("/health")
def health() -> Dict[str, Any]:
    """Health check"""
    return {"status": "ok"}
