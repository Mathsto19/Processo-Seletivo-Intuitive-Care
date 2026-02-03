import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import shutil

import pandas as pd

from Baixar_cadastro import baixar_cadop


COLUNAS_VALIDADOS = ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]
COLUNAS_ADICIONAR = ["RegistroANS", "Modalidade", "UF"]

def garantir_validados(arquivo_validados: Path, pasta_script: Path) -> None:
    if arquivo_validados.exists() and arquivo_validados.stat().st_size > 0:
        return

    pasta_teste2 = pasta_script.parent 
    candidatos = [
        pasta_teste2 / "2.1. Validação de Dados com Estratégias Diferentes" / "Dados" / "Saídas" / "validados.csv",
        pasta_teste2 / "2.1. Validação de Dados com Estratégias Diferentes" / "Dados" / "Saidas" / "validados.csv",
    ]

    for src in candidatos:
        if src.exists() and src.stat().st_size > 0:
            arquivo_validados.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, arquivo_validados)
            print(f"Copiado automaticamente: {src} -> {arquivo_validados}")
            return


def normalizar_cnpj(valor: Any) -> Optional[str]:
    """Remove máscara e retorna CNPJ com 14 dígitos."""
    if valor is None:
        return None
    
    texto = str(valor).strip()
    if not texto:
        return None
    
    digitos = re.sub(r"\D", "", texto)
    if not digitos or len(digitos) > 14:
        return None
    
    return digitos.zfill(14)


def normalizar_nome_coluna(nome: str) -> str:
    """Remove acentos e caracteres especiais de nomes de colunas."""
    texto = str(nome).strip().lower()
    
    # Remove acentos
    texto = (
        texto.replace("á", "a").replace("à", "a").replace("ã", "a").replace("â", "a")
        .replace("é", "e").replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o").replace("ô", "o").replace("õ", "o")
        .replace("ú", "u")
        .replace("ç", "c")
    )
    
    # Remove tudo que não é letra ou número
    texto = re.sub(r"[^a-z0-9]+", "", texto)
    return texto


def limpar_texto(valor: Any) -> str:
    """Limpa valores de texto, convertendo None/nan/null em string vazia."""
    if valor is None:
        return ""
    
    texto = str(valor).strip()
    if texto.lower() in {"nan", "none", "null"}:
        return ""
    
    return texto


def ler_csv(caminho: Path) -> pd.DataFrame:
    """Lê CSV tentando múltiplos encodings e separadores."""
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


def verificar_colunas(df: pd.DataFrame, colunas_esperadas: list, nome_arquivo: str) -> None:
    """Verifica se DataFrame tem todas as colunas esperadas."""
    faltando = [col for col in colunas_esperadas if col not in df.columns]
    if faltando:
        raise ValueError(
            f"{nome_arquivo} sem colunas esperadas.\n"
            f"Faltando: {faltando}\n"
            f"Encontradas: {list(df.columns)}"
        )


def mapear_colunas_cadastro(df: pd.DataFrame) -> Tuple[str, str, str, str]:
    """Identifica automaticamente as colunas do cadastro ANS. Retorna: (coluna_cnpj, coluna_registro, coluna_modalidade, coluna_uf)"""
    # Cria mapa de colunas normalizadas
    mapa = {normalizar_nome_coluna(col): col for col in df.columns}
    
    def buscar_coluna(*opcoes: str) -> Optional[str]:
        """Busca a primeira coluna que bate com as opções."""
        for opcao in opcoes:
            if opcao in mapa:
                return mapa[opcao]
        return None
    
    def buscar_por_contencao(parte: str) -> Optional[str]:
        parte = normalizar_nome_coluna(parte)
        for norm, original in mapa.items():
            if parte in norm:
                return original
        return None
    
    # Busca as colunas necessárias
    col_cnpj = buscar_coluna("cnpj") or buscar_por_contencao("cnpj")
    col_registro = (
        buscar_coluna("registroans", "registrodaans", "registroansoperadora")
        or buscar_por_contencao("registroans")
    )
    col_modalidade = buscar_coluna("modalidade") or buscar_por_contencao("modalidade")
    col_uf = buscar_coluna("uf") or buscar_por_contencao("uf")

    # Valida se encontrou todas
    if not col_cnpj or not col_registro or not col_modalidade or not col_uf:
        raise ValueError(
            "Não consegui mapear colunas do cadastro ANS.\n"
            f"Encontrado: CNPJ={col_cnpj}, Registro={col_registro}, "
            f"Modalidade={col_modalidade}, UF={col_uf}\n"
            f"Colunas disponíveis: {list(df.columns)}"
        )
    
    return col_cnpj, col_registro, col_modalidade, col_uf


def calcular_score_completude(registro: str, modalidade: str, uf: str) -> int:
    """Calcula score baseado em quantos campos estão preenchidos."""
    score = 0
    if registro and registro.strip():
        score += 1
    if modalidade and modalidade.strip():
        score += 1
    if uf and uf.strip():
        score += 1
    return score


def extrair_numero_registro(valor: str) -> int:
    """Extrai número do registro ANS para desempate. Retorna valor alto se não for numérico."""
    if str(valor).isdigit():
        return int(valor)
    return 10**12


def deduplicar_cadastro(df_cadastro: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Remove duplicatas do cadastro usando critério determinístico. Retorna: df_dedup: 1 linha por CNPJ (escolha baseada em completude dos dados); df_divergentes: CNPJs que aparecem com dados diferentes"""
    df = df_cadastro.copy()
    
    # Normaliza dados
    df["CNPJ"] = df["CNPJ"].apply(normalizar_cnpj)
    df["RegistroANS"] = df["RegistroANS"].apply(limpar_texto)
    df["Modalidade"] = df["Modalidade"].apply(limpar_texto)
    df["UF"] = df["UF"].apply(limpar_texto)
    
    # Identifica CNPJs com dados divergentes
    combinacoes = df.dropna(subset=["CNPJ"])[["CNPJ", "RegistroANS", "Modalidade", "UF"]].drop_duplicates()
    contagem = combinacoes.groupby("CNPJ").size().reset_index(name="qtd_combinacoes")
    df_temp = combinacoes.merge(contagem, on="CNPJ", how="left")
    df_divergentes = df_temp[df_temp["qtd_combinacoes"].astype(int) > 1].drop(columns=["qtd_combinacoes"])
    
    # Critério de desempate: mais campos preenchidos ganha
    df["score"] = df.apply(
        lambda linha: calcular_score_completude(
            linha["RegistroANS"],
            linha["Modalidade"],
            linha["UF"]
        ),
        axis=1
    )
    
    # Desempate secundário: menor RegistroANS numérico
    df["registro_num"] = df["RegistroANS"].apply(extrair_numero_registro)
    
    # Ordena por CNPJ e critérios de desempate
    df = df.sort_values(
        by=["CNPJ", "score", "registro_num", "RegistroANS", "Modalidade", "UF"],
        ascending=[True, False, True, True, True, True],
        kind="mergesort",
    )
    
    # Fica com a primeira ocorrência de cada CNPJ
    df_dedup = df.dropna(subset=["CNPJ"]).drop_duplicates(subset=["CNPJ"], keep="first").copy()
    df_dedup = df_dedup.drop(columns=["score", "registro_num"])
    
    return df_dedup, df_divergentes


def gerar_saidas_vazias(pasta_saida: Path) -> None:
    """Gera arquivos de saída vazios quando não há dados para processar."""
    arq_enriquecido = pasta_saida / "enriquecido.csv"
    arq_sem_match = pasta_saida / "sem_match.csv"
    arq_duplicados = pasta_saida / "cadastro_duplicados.csv"
    arq_resumo = pasta_saida / "resumo_enriquecimento.json"
    
    # CSVs vazios
    pd.DataFrame(columns=COLUNAS_VALIDADOS + COLUNAS_ADICIONAR).to_csv(
        arq_enriquecido, index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(columns=COLUNAS_VALIDADOS + COLUNAS_ADICIONAR).to_csv(
        arq_sem_match, index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(columns=["CNPJ", "RegistroANS", "Modalidade", "UF"]).to_csv(
        arq_duplicados, index=False, encoding="utf-8-sig"
    )
    
    # Resumo vazio
    resumo = {
        "total": 0,
        "matched": 0,
        "sem_match": 0,
        "cadastro_divergentes": 0
    }
    arq_resumo.write_text(
        json.dumps(resumo, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    
    print("[OK] Entrada vazia. Saídas vazias geradas.")


def processar_enriquecimento() -> None:
    """Executa todo o processo da etapa 2.2."""
    pasta_script = Path(__file__).resolve().parent
    
    pasta_entradas = pasta_script / "Dados" / "Entradas"
    pasta_saidas = pasta_script / "Dados" / "Saídas"
    pasta_entradas.mkdir(parents=True, exist_ok=True)
    pasta_saidas.mkdir(parents=True, exist_ok=True)
    
    # Arquivos de entrada
    arquivo_validados = pasta_entradas / "validados.csv"
    arquivo_cadastro = pasta_entradas / "operadoras_cadastro.csv"
    
    # Arquivos de saída
    arquivo_enriquecido = pasta_saidas / "enriquecido.csv"
    arquivo_sem_match = pasta_saidas / "sem_match.csv"
    arquivo_duplicados = pasta_saidas / "cadastro_duplicados.csv"
    arquivo_resumo = pasta_saidas / "resumo_enriquecimento.json"
    
    garantir_validados(arquivo_validados, pasta_script)

    # Verifica se arquivo de validados existe
    if not arquivo_validados.exists():
        raise FileNotFoundError(
            f"Não encontrei {arquivo_validados}.\n"
            "Copie o validados.csv da etapa 2.1 para Dados/Entradas."
        )
    
    # Lê arquivo de validados
    print(f"\nLendo: {arquivo_validados.name}")
    df_validados = ler_csv(arquivo_validados)
    verificar_colunas(df_validados, COLUNAS_VALIDADOS, "validados.csv")
    
    # Se estiver vazio, gera saídas vazias
    if df_validados.empty:
        gerar_saidas_vazias(pasta_saidas)
        return
    
    # Normaliza CNPJ dos validados
    df_validados["CNPJ"] = df_validados["CNPJ"].apply(normalizar_cnpj)
    
    # Baixa cadastro se necessário
    if (not arquivo_cadastro.exists()) or arquivo_cadastro.stat().st_size == 0:
        print("\nBaixando cadastro de operadoras da ANS...")
        baixar_cadop(arquivo_cadastro, forcar=False)
    
    # Lê cadastro
    print(f"\nLendo: {arquivo_cadastro.name}")
    df_cadastro_completo = ler_csv(arquivo_cadastro)
    
    # Mapeia colunas do cadastro automaticamente
    col_cnpj, col_registro, col_modalidade, col_uf = mapear_colunas_cadastro(df_cadastro_completo)
    
    # Extrai apenas colunas relevantes
    df_cadastro = df_cadastro_completo[[col_cnpj, col_registro, col_modalidade, col_uf]].copy()
    df_cadastro.columns = ["CNPJ", "RegistroANS", "Modalidade", "UF"]
    
    # Remove duplicatas do cadastro
    print("\nRemovendo duplicatas do cadastro...")
    df_cadastro_limpo, df_cadastro_divergentes = deduplicar_cadastro(df_cadastro)
    
    # Salva CNPJs divergentes
    df_cadastro_divergentes.to_csv(arquivo_duplicados, index=False, encoding="utf-8-sig")
    
    # Faz JOIN
    print("\nRealizando JOIN por CNPJ...")
    df_resultado = df_validados.merge(
        df_cadastro_limpo,
        on="CNPJ",
        how="left",
        validate="m:1"
    )
    
    # Separa registros com match e sem match
    tem_registro = df_resultado["RegistroANS"].fillna("").astype(str).str.strip() != ""
    df_com_match = df_resultado[tem_registro].copy()
    df_sem_match = df_resultado[~tem_registro].copy()
    
    # Salva resultados
    print("\nSalvando resultados...")
    df_com_match.to_csv(arquivo_enriquecido, index=False, encoding="utf-8-sig")
    df_sem_match.to_csv(arquivo_sem_match, index=False, encoding="utf-8-sig")
    
    # Gera resumo
    total_divergentes = int(df_cadastro_divergentes["CNPJ"].nunique()) if not df_cadastro_divergentes.empty else 0
    
    resumo: Dict[str, Any] = {
        "total_registros": int(len(df_resultado)),
        "registros_enriquecidos": int(len(df_com_match)),
        "registros_sem_match": int(len(df_sem_match)),
        "cnpjs_divergentes_cadastro": total_divergentes,
        "arquivos_gerados": {
            "enriquecido": arquivo_enriquecido.name,
            "sem_match": arquivo_sem_match.name,
            "duplicados": arquivo_duplicados.name
        }
    }
    
    arquivo_resumo.write_text(
        json.dumps(resumo, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    # Exibe sumário
    print("Resultados:")
    print(f"  Total: {len(df_resultado)}")
    print(f"  Enriquecidos: {len(df_com_match)} ({len(df_com_match)/len(df_resultado)*100:.1f}%)")
    print(f"  Sem match: {len(df_sem_match)} ({len(df_sem_match)/len(df_resultado)*100:.1f}%)")
    print(f"  CNPJs divergentes no cadastro: {total_divergentes}")


if __name__ == "__main__":
    processar_enriquecimento()
