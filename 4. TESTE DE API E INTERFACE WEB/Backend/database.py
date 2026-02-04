from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class CaminhosExportacao:
    operadoras_csv: Path
    despesas_csv: Path
    agregados_csv: Path


def _raiz_projeto() -> Path:
    return Path(__file__).resolve().parents[1]


def _diretorio_data() -> Path:
    return _raiz_projeto() / "Data"


def _normalizar_chave(texto: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (texto or "").strip().lower())


def _normalizar_cnpj(cnpj: str) -> str:
    return re.sub(r"\D+", "", str(cnpj or ""))


def _escolher_coluna(colunas_existentes: Iterable[str], candidatos: Iterable[str]) -> Optional[str]:
    mapa: Dict[str, str] = {_normalizar_chave(c): c for c in colunas_existentes}
    for candidato in candidatos:
        chave = _normalizar_chave(candidato)
        if chave in mapa:
            return mapa[chave]
    return None


def _detectar_tabelas(engine: Engine) -> Tuple[str, str]:
    inspecao = inspect(engine)
    tabelas = inspecao.get_table_names()
    
    if not tabelas:
        raise RuntimeError("Nenhuma tabela encontrada no banco.")

    melhor_operadoras: Optional[str] = None
    melhor_score_ops = -1
    melhor_despesas: Optional[str] = None
    melhor_score_desp = -1

    for tabela in tabelas:
        colunas = [col["name"] for col in inspecao.get_columns(tabela)]
        colunas_norm = {_normalizar_chave(c) for c in colunas}

        if "cnpj" in colunas_norm:
            score_ops = 0
            termos_operadoras = ["razaosocial", "razao_social", "companyname", "company_name", "uf", "modalidade", "registroans"]
            for termo in termos_operadoras:
                if _normalizar_chave(termo) in colunas_norm:
                    score_ops += 1
            if score_ops > melhor_score_ops:
                melhor_score_ops = score_ops
                melhor_operadoras = tabela

        if "cnpj" in colunas_norm:
            tem_ano = any(k in colunas_norm for k in ["ano", "year"])
            tem_trimestre = any(k in colunas_norm for k in ["trimestre", "quarter"])
            tem_valor = any(k in colunas_norm for k in ["valor", "expensevalue", "valor_despesas", "valordespesas"])
            score_desp = int(tem_ano) + int(tem_trimestre) + int(tem_valor)
            if score_desp > melhor_score_desp:
                melhor_score_desp = score_desp
                melhor_despesas = tabela

    if not melhor_operadoras or melhor_score_ops < 1:
        raise RuntimeError("Tabela de operadoras nao encontrada.")
    if not melhor_despesas or melhor_score_desp < 3:
        raise RuntimeError("Tabela de despesas nao encontrada.")

    return melhor_operadoras, melhor_despesas


def _ler_operadoras(engine: Engine, tabela: str) -> pd.DataFrame:
    df = pd.read_sql_query(f"SELECT * FROM {tabela}", engine)

    col_cnpj = _escolher_coluna(df.columns, ["cnpj"])
    if not col_cnpj:
        raise RuntimeError(f"Tabela {tabela} sem coluna CNPJ.")

    col_razao = _escolher_coluna(df.columns, ["razao_social", "razaoSocial", "company_name", "companyName", "razao"])
    col_registro = _escolher_coluna(df.columns, ["registro_ans", "registroANS", "ans_registration", "registro"])
    col_modalidade = _escolher_coluna(df.columns, ["modalidade", "modality"])
    col_uf = _escolher_coluna(df.columns, ["uf", "estado", "state"])

    resultado = pd.DataFrame()
    resultado["cnpj"] = df[col_cnpj].map(_normalizar_cnpj)
    resultado["razao_social"] = df[col_razao] if col_razao else ""
    resultado["registro_ans"] = df[col_registro] if col_registro else ""
    resultado["modalidade"] = df[col_modalidade] if col_modalidade else ""
    resultado["uf"] = df[col_uf] if col_uf else ""

    resultado = resultado[resultado["cnpj"].str.len() == 14]
    resultado = resultado.drop_duplicates(subset=["cnpj"], keep="first").reset_index(drop=True)

    return resultado[["cnpj", "razao_social", "registro_ans", "modalidade", "uf"]]


def _ler_despesas(engine: Engine, tabela: str) -> pd.DataFrame:
    df = pd.read_sql_query(f"SELECT * FROM {tabela}", engine)

    col_cnpj = _escolher_coluna(df.columns, ["cnpj"])
    col_ano = _escolher_coluna(df.columns, ["ano", "year"])
    col_trimestre = _escolher_coluna(df.columns, ["trimestre", "quarter"])
    col_valor = _escolher_coluna(df.columns, ["valor", "expense_value", "expenseValue", "valor_despesas", "valorDespesas"])

    if not all([col_cnpj, col_ano, col_trimestre, col_valor]):
        raise RuntimeError(f"Tabela {tabela} sem colunas necessarias.")

    resultado = pd.DataFrame()
    resultado["cnpj"] = df[col_cnpj].map(_normalizar_cnpj)
    resultado["ano"] = pd.to_numeric(df[col_ano], errors="coerce").astype("Int64")
    resultado["trimestre"] = pd.to_numeric(df[col_trimestre], errors="coerce").astype("Int64")
    resultado["valor"] = pd.to_numeric(df[col_valor], errors="coerce")

    resultado = resultado.dropna(subset=["cnpj", "ano", "trimestre", "valor"])
    resultado = resultado[resultado["cnpj"].str.len() == 14].reset_index(drop=True)

    return resultado[["cnpj", "ano", "trimestre", "valor"]]


def _construir_agregados(operadoras: pd.DataFrame, despesas: pd.DataFrame) -> pd.DataFrame:
    ops_uf = operadoras[["cnpj", "uf"]].copy()
    ops_uf["uf"] = ops_uf["uf"].astype(str).str.strip()

    dados_completos = despesas.merge(ops_uf, on="cnpj", how="left")
    dados_completos = dados_completos[dados_completos["uf"].notna() & (dados_completos["uf"].astype(str).str.len() > 0)]

    total_por_uf = dados_completos.groupby("uf", as_index=False)["valor"].sum()
    total_por_uf = total_por_uf.rename(columns={"valor": "total_despesas"})

    qtd_por_uf = ops_uf[ops_uf["uf"].astype(str).str.len() > 0].groupby("uf", as_index=False)["cnpj"].nunique()
    qtd_por_uf = qtd_por_uf.rename(columns={"cnpj": "qtd_operadoras"})

    resultado = total_por_uf.merge(qtd_por_uf, on="uf", how="left")
    resultado["qtd_operadoras"] = resultado["qtd_operadoras"].fillna(0).astype(int)
    resultado["media_por_operadora"] = resultado.apply(
        lambda row: (row["total_despesas"] / row["qtd_operadoras"]) if row["qtd_operadoras"] > 0 else 0.0, axis=1
    )

    resultado = resultado.sort_values("total_despesas", ascending=False).reset_index(drop=True)
    return resultado[["uf", "total_despesas", "qtd_operadoras", "media_por_operadora"]]


def exportar_csvs_do_banco() -> None:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL nao definido no ambiente.")

    print("Conectando ao banco...")
    engine = create_engine(database_url, pool_pre_ping=True)

    diretorio_data = _diretorio_data()
    diretorio_data.mkdir(parents=True, exist_ok=True)

    tabela_operadoras = os.getenv("OPERATORS_TABLE", "").strip()
    tabela_despesas = os.getenv("EXPENSES_TABLE", "").strip()

    if not (tabela_operadoras and tabela_despesas):
        print("Detectando tabelas...")
        tabela_operadoras, tabela_despesas = _detectar_tabelas(engine)

    print(f"Operadoras: {tabela_operadoras}")
    print(f"Despesas: {tabela_despesas}")

    df_operadoras = _ler_operadoras(engine, tabela_operadoras)
    df_despesas = _ler_despesas(engine, tabela_despesas)
    df_agregados = _construir_agregados(df_operadoras, df_despesas)

    caminhos = CaminhosExportacao(
        operadoras_csv=diretorio_data / "operadoras.csv",
        despesas_csv=diretorio_data / "despesas.csv",
        agregados_csv=diretorio_data / "agregados.csv",
    )

    df_operadoras.to_csv(caminhos.operadoras_csv, index=False, encoding="utf-8-sig")
    df_despesas.to_csv(caminhos.despesas_csv, index=False, encoding="utf-8-sig")
    df_agregados.to_csv(caminhos.agregados_csv, index=False, encoding="utf-8-sig")

    print(f"\nCSVs exportados em: {diretorio_data}")
    print(f"operadoras.csv: {len(df_operadoras)} registros")
    print(f"despesas.csv: {len(df_despesas)} registros")
    print(f"agregados.csv: {len(df_agregados)} registros")


if __name__ == "__main__":
    exportar_csvs_do_banco()
