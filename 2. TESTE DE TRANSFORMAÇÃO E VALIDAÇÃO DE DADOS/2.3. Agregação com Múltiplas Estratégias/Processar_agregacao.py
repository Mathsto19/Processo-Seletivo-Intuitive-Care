import json
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


COLUNAS_ENTRADA = [
    "CNPJ",
    "RazaoSocial",
    "Trimestre",
    "Ano",
    "ValorDespesas",
    "RegistroANS",
    "Modalidade",
    "UF",
]


def ler_csv(caminho: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "latin1", "iso-8859-1"]
    separadores = [None, ";", ",", "\t", "|"]
    ultimo_erro: Optional[Exception] = None

    for enc in encodings:
        for sep in separadores:
            try:
                return pd.read_csv(
                    caminho,
                    dtype=str,
                    encoding=enc,
                    sep=sep,
                    engine="python",
                    on_bad_lines="skip",
                )
            except Exception as e:
                ultimo_erro = e

    raise RuntimeError(f"Não consegui ler CSV: {caminho}. Último erro: {ultimo_erro}")


def verificar_colunas(df: pd.DataFrame, colunas_esperadas: list[str]) -> None:
    faltando = [c for c in colunas_esperadas if c not in df.columns]
    if faltando:
        raise ValueError(
            "CSV de entrada não tem as colunas esperadas.\n"
            f"Faltando: {faltando}\n"
            f"Encontradas: {list(df.columns)}"
        )


def converter_numero(valor: Any) -> Optional[float]:
    if valor is None:
        return None

    texto = str(valor).strip()
    if not texto:
        return None

    # remove símbolos comuns
    texto = texto.replace("R$", "").strip()

    # tenta detectar BR/US
    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")  # BR
        else:
            texto = texto.replace(",", "")  # US
    elif "," in texto:
        texto = texto.replace(",", ".")

    try:
        return float(texto)
    except Exception:
        return None


def garantir_enriquecido(arquivo_enriquecido: Path, pasta_script: Path) -> None:
    """
    Se o enriquecido.csv não existir em 2.3, copia automaticamente de 2.2.
    """
    if arquivo_enriquecido.exists() and arquivo_enriquecido.stat().st_size > 0:
        return

    pasta_teste2 = pasta_script.parent 
    src = (
        pasta_teste2
        / "2.2. Enriquecimento de Dados com Tratamento de Falhas"
        / "Dados"
        / "Saídas"
        / "enriquecido.csv"
    )

    if src.exists() and src.stat().st_size > 0:
        arquivo_enriquecido.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, arquivo_enriquecido)
        print(f"Copiado automaticamente: {src} -> {arquivo_enriquecido}")
        return

    raise FileNotFoundError(
        "Não encontrei o enriquecido.csv.\n"
        f"Esperado em: {arquivo_enriquecido}\n"
        f"Ou em: {src}\n"
        "Rode a etapa 2.2 antes."
    )


def salvar_saida_vazia(pasta_saida: Path) -> None:
    arq_csv = pasta_saida / "despesas_agregadas.csv"
    arq_json = pasta_saida / "resumo_agregacao.json"

    colunas_saida = [
        "RazaoSocial",
        "UF",
        "total_despesas",
        "media_por_trimestre",
        "desvio_padrao",
        "qtd_registros",
        "qtd_trimestres",
    ]

    pd.DataFrame(columns=colunas_saida).to_csv(arq_csv, index=False, encoding="utf-8-sig")

    resumo = {
        "total_grupos": 0,
        "total_registros_entrada": 0,
        "arquivo_saida": arq_csv.name,
    }
    arq_json.write_text(json.dumps(resumo, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Entrada vazia. Saídas vazias geradas.")


def compactar_zip(arquivo_csv: Path, pasta_saida: Path, nome_zip: str) -> Path:
    """
    Cria Teste_{seu_nome}.zip contendo despesas_agregadas.csv
    """
    zip_base = pasta_saida / nome_zip.replace(".zip", "")
    zip_path = Path(str(zip_base) + ".zip")

    tmp_dir = pasta_saida / "__tmp_zip__"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(arquivo_csv, tmp_dir / arquivo_csv.name)
    shutil.make_archive(str(zip_base), "zip", root_dir=tmp_dir)

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return zip_path


def processar_agregacao(nome_zip: str = "Teste_Matheus.zip") -> None:
    pasta_script = Path(__file__).resolve().parent

    pasta_entradas = pasta_script / "Dados" / "Entradas"
    pasta_saidas = pasta_script / "Dados" / "Saídas"
    pasta_entradas.mkdir(parents=True, exist_ok=True)
    pasta_saidas.mkdir(parents=True, exist_ok=True)

    arquivo_entrada = pasta_entradas / "enriquecido.csv"
    garantir_enriquecido(arquivo_entrada, pasta_script)

    print(f"\nLendo: {arquivo_entrada.name}")
    df = ler_csv(arquivo_entrada)
    print(f"   Total de linhas: {len(df)}")

    verificar_colunas(df, COLUNAS_ENTRADA)

    if df.empty:
        salvar_saida_vazia(pasta_saidas)
        return

    # converte valor para float
    df["ValorDespesas_num"] = df["ValorDespesas"].apply(converter_numero)

    # remove linhas inválidas de valor (por segurança)
    df = df[df["ValorDespesas_num"].notna()].copy()

    # cria uma chave de trimestre (Ano + Trimestre) para contar trimestres distintos
    df["ano_trimestre"] = df["Ano"].astype(str).str.strip() + "-" + df["Trimestre"].astype(str).str.strip()

    # AGRUPAMENTO
    grupo = df.groupby(["RazaoSocial", "UF"], dropna=False)

    agregado = grupo.agg(
        total_despesas=("ValorDespesas_num", "sum"),
        media_por_trimestre=("ValorDespesas_num", "mean"),
        desvio_padrao=("ValorDespesas_num", "std"),
        qtd_registros=("ValorDespesas_num", "size"),
        qtd_trimestres=("ano_trimestre", "nunique"),
    ).reset_index()

    # std pode virar NaN quando só tem 1 registro no grupo
    agregado["desvio_padrao"] = agregado["desvio_padrao"].fillna(0.0)

    # ORDENAÇÃO: total desc (maior -> menor)
    agregado = agregado.sort_values(by="total_despesas", ascending=False, kind="mergesort")

    # salva CSV final
    arquivo_saida = pasta_saidas / "despesas_agregadas.csv"
    agregado.to_csv(arquivo_saida, index=False, encoding="utf-8-sig")

    # resumo
    resumo = {
        "total_registros_entrada": int(len(df)),
        "total_grupos": int(len(agregado)),
        "arquivo_saida": arquivo_saida.name,
        "zip": nome_zip,
    }
    (pasta_saidas / "resumo_agregacao.json").write_text(
        json.dumps(resumo, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # compacta
    zip_path = compactar_zip(arquivo_saida, pasta_saidas, nome_zip)

    print("Resultados:")
    print(f"  Registros usados na agregação: {len(df)}")
    print(f"  Grupos (RazaoSocial, UF): {len(agregado)}")
    print(f"  CSV gerado: {arquivo_saida}")
    print(f"  ZIP gerado: {zip_path}")


if __name__ == "__main__":
    processar_agregacao(nome_zip="Teste_Processo_2.zip")
