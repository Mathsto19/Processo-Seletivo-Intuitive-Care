from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import shutil

import pandas as pd


COLUNAS_ESPERADAS = ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]

def arquivo_tem_dados_csv(caminho: Path) -> bool:
    """
    True se o arquivo tem pelo menos 1 linha de dados além do cabeçalho.
    """
    if not caminho.exists():
        return False
    try:
        with caminho.open("rb") as f:
            content = f.read(1024 * 1024)  # até 1MB
        return content.count(b"\n") >= 2
    except Exception:
        return False


def sincronizar_entrada_da_tarefa1(arquivo_entrada: Path, pasta_script: Path) -> bool:
    """
    Se a entrada estiver vazia/ausente, copia automaticamente o consolidado do Teste 1
    para a entrada do Teste 2.1.
    Retorna True se copiou.
    """
    if arquivo_tem_dados_csv(arquivo_entrada):
        return False

    repo_root = pasta_script.parent.parent

    origem = repo_root / "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA" / "Dados" / "Saída" / "consolidado_despesas.csv"
    if not origem.exists():
        # fallback sem acento (caso sua pasta seja "Saida")
        origem = repo_root / "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA" / "Dados" / "Saida" / "consolidado_despesas.csv"

    if not origem.exists():
        return False

    arquivo_entrada.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(origem, arquivo_entrada)

    print(f"[INFO] Entrada sincronizada automaticamente do Teste 1: {origem}")
    return True


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


def validar_cnpj(cnpj14: Optional[str]) -> bool:
    """Valida CNPJ com dígitos verificadores."""
    if not cnpj14 or len(cnpj14) != 14 or not cnpj14.isdigit():
        return False

    if cnpj14 == cnpj14[0] * 14:
        return False

    def calcular_digito(base: str, pesos: List[int]) -> str:
        soma = sum(int(d) * p for d, p in zip(base, pesos))
        resto = soma % 11
        return "0" if resto < 2 else str(11 - resto)

    pesos_primeiro = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_segundo = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    base = cnpj14[:12]
    digito1 = calcular_digito(base, pesos_primeiro)
    digito2 = calcular_digito(base + digito1, pesos_segundo)

    return cnpj14 == base + digito1 + digito2


def converter_numero(valor: Any) -> Optional[float]:
    """Converte valores em formato BR (1.234,56) ou US (1,234.56). Detecta automaticamente qual formato baseado no último separador."""
    if valor is None:
        return None

    texto = str(valor).strip()
    if not texto:
        return None

    texto = re.sub(r"[R$\s]", "", texto)

    if "," in texto and "." in texto:
        pos_virgula = texto.rfind(",")
        pos_ponto = texto.rfind(".")
        if pos_virgula > pos_ponto:
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        texto = texto.replace(",", ".")

    texto = re.sub(r"[^0-9\.\-]", "", texto)

    if texto in ("", "-", ".", "-."):
        return None

    try:
        return float(texto)
    except ValueError:
        return None


def ler_csv(caminho: Path) -> pd.DataFrame:
    """Lê CSV tentando separadores e encodings comuns, com fallback resiliente."""
    encodings = ["utf-8-sig", "utf-8", "latin1", "iso-8859-1"]
    seps = [",", ";", "\t", "|"]

    ultimo_erro: Optional[Exception] = None

    # 1) Tentativa: sniff separador (sep=None) com engine python
    for enc in encodings:
        try:
            df = pd.read_csv(
                caminho,
                dtype=str,
                encoding=enc,
                sep=None,              # sniff
                engine="python",
            )
            return df
        except Exception as e:
            ultimo_erro = e

    # 2) Tentativa: força separadores comuns
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(
                    caminho,
                    dtype=str,
                    encoding=enc,
                    sep=sep,
                    engine="python",
                )
                return df
            except Exception as e:
                ultimo_erro = e

    # 3) Último recurso: pula linhas ruins (mantém o pipeline rodando)
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(
                    caminho,
                    dtype=str,
                    encoding=enc,
                    sep=sep,
                    engine="python",
                    on_bad_lines="skip", 
                )
                print("[WARN] Algumas linhas ruins foram ignoradas (on_bad_lines='skip').")
                return df
            except Exception as e:
                ultimo_erro = e

    raise RuntimeError(f"Não consegui ler o arquivo: {caminho}. Último erro: {ultimo_erro}")


def validar_linha(
    cnpj_raw: Any,
    razao_raw: Any,
    valor_raw: Any
) -> Tuple[bool, List[str], Optional[str], Optional[str], Optional[float]]:
    """Valida uma linha completa. Retorna: (valido?, [motivos], cnpj_normalizado, razao_normalizada, valor_convertido)"""
    motivos: List[str] = []

    cnpj_norm = normalizar_cnpj(cnpj_raw)
    if not validar_cnpj(cnpj_norm):
        motivos.append("cnpj_invalido")

    razao_norm = None if razao_raw is None else str(razao_raw).strip()
    if (not razao_norm) or (razao_norm.lower() in {"nan", "none", "null"}):
        motivos.append("razao_social_vazia")
        razao_norm = None

    valor_float = converter_numero(valor_raw)
    if valor_float is None or valor_float <= 0:
        motivos.append("valor_invalido_ou_nao_positivo")

    valido = len(motivos) == 0
    return (valido, motivos, cnpj_norm, razao_norm, valor_float)


def verificar_colunas(df: pd.DataFrame) -> None:
    """Verifica se o CSV tem todas as colunas necessárias."""
    faltando = [col for col in COLUNAS_ESPERADAS if col not in df.columns]
    if faltando:
        raise ValueError(
            f"   CSV não tem as colunas esperadas!\n"
            f"   Faltando: {faltando}\n"
            f"   Encontradas: {list(df.columns)}"
        )


def criar_arquivo_exemplo(caminho: Path) -> None:
    """Cria um CSV com cabeçalho esperado se o arquivo não existir."""
    caminho.parent.mkdir(parents=True, exist_ok=True)

    if caminho.exists():
        return

    df_exemplo = pd.DataFrame(columns=COLUNAS_ESPERADAS)
    df_exemplo.to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"[INFO] Arquivo de entrada não encontrado. Template criado em: {caminho}")


def salvar_saidas_vazias(
    arquivo_validados: Path,
    arquivo_invalidos: Path,
    arquivo_relatorio: Path
) -> None:
    """Gera saídas vazias quando o CSV de entrada está sem linhas."""
    df_validos = pd.DataFrame(columns=COLUNAS_ESPERADAS)
    df_invalidos = pd.DataFrame(columns=COLUNAS_ESPERADAS + ["motivo_rejeicao"])

    df_validos.to_csv(arquivo_validados, index=False, encoding="utf-8-sig")
    df_invalidos.to_csv(arquivo_invalidos, index=False, encoding="utf-8-sig")

    relatorio = {
        "total_linhas": 0,
        "linhas_validas": 0,
        "linhas_invalidas": 0,
        "taxa_rejeicao_pct": 0.0,
        "motivos_rejeicao": {},
    }
    arquivo_relatorio.write_text(json.dumps(relatorio, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n[OK] CSV de entrada está vazio (apenas cabeçalho). Saídas vazias geradas.")


def processar_validacao() -> None:
    """Executa todo o processo da etapa 2.1."""
    pasta_script = Path(__file__).resolve().parent

    arquivo_entrada = pasta_script / "Dados" / "Entradas" / "consolidado_teste1.csv"
    pasta_saida = pasta_script / "Dados" / "Saídas"
    arquivo_validados = pasta_saida / "validados.csv"
    arquivo_invalidos = pasta_saida / "invalidos.csv"
    arquivo_relatorio = pasta_saida / "resumo_validacao.json"

    pasta_saida.mkdir(parents=True, exist_ok=True)

    criar_arquivo_exemplo(arquivo_entrada)
    sincronizar_entrada_da_tarefa1(arquivo_entrada, pasta_script)

    print(f"\nLendo: {arquivo_entrada.name}")
    df = ler_csv(arquivo_entrada)
    print(f"   Total de linhas: {len(df)}")

    verificar_colunas(df)

    if df.empty:
        salvar_saidas_vazias(arquivo_validados, arquivo_invalidos, arquivo_relatorio)
        return

    print("\nValidando dados...")
    resultados = df.apply(
        lambda linha: validar_linha(
            linha.get("CNPJ"),
            linha.get("RazaoSocial"),
            linha.get("ValorDespesas"),
        ),
        axis=1,
        result_type="expand",
    )
    resultados.columns = ["valido", "motivos", "cnpj_norm", "razao_norm", "valor_float"]

    df_completo = pd.concat([df, resultados], axis=1)

    df_completo["motivo_rejeicao"] = df_completo["motivos"].apply(
        lambda lista: ";".join(lista) if isinstance(lista, list) else ""
    )

    df_validos = df_completo[df_completo["valido"]].copy()
    df_invalidos = df_completo[~df_completo["valido"]].copy()

    df_validos["CNPJ"] = df_validos["cnpj_norm"]
    df_validos["RazaoSocial"] = df_validos["razao_norm"]
    df_validos["ValorDespesas"] = df_validos["valor_float"]
    df_validos = df_validos[COLUNAS_ESPERADAS]

    df_invalidos["CNPJ"] = df_invalidos["cnpj_norm"]
    df_invalidos["RazaoSocial"] = df_invalidos["razao_norm"]
    df_invalidos["ValorDespesas"] = df_invalidos["valor_float"]
    df_invalidos = df_invalidos[COLUNAS_ESPERADAS + ["motivo_rejeicao"]]

    print("\nSalvando resultados...")
    df_validos.to_csv(arquivo_validados, index=False, encoding="utf-8-sig")
    print(f"   Validados: {arquivo_validados.name}")

    df_invalidos.to_csv(arquivo_invalidos, index=False, encoding="utf-8-sig")
    print(f"   Inválidos: {arquivo_invalidos.name}")

    total = len(df)
    validos = len(df_validos)
    invalidos = len(df_invalidos)

    contagem_motivos: Dict[str, int] = {}
    for motivo_str in df_invalidos["motivo_rejeicao"]:
        for motivo in str(motivo_str).split(";"):
            motivo = motivo.strip()
            if motivo:
                contagem_motivos[motivo] = contagem_motivos.get(motivo, 0) + 1

    relatorio = {
        "total_linhas": total,
        "linhas_validas": validos,
        "linhas_invalidas": invalidos,
        "taxa_rejeicao_pct": round((invalidos / total * 100), 2) if total > 0 else 0.0,
        "motivos_rejeicao": contagem_motivos,
    }

    arquivo_relatorio.write_text(
        json.dumps(relatorio, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"   Relatório: {arquivo_relatorio.name}")

    print(f"\nTotal de linhas: {total}")
    print(f"Válidas: {validos} ({(validos / total * 100):.1f}%)")
    print(f"Inválidas: {invalidos} ({(invalidos / total * 100):.1f}%)")

    if contagem_motivos:
        print("\nMotivos de rejeição:")
        for motivo, qtd in sorted(contagem_motivos.items(), key=lambda x: -x[1]):
            print(f"   • {motivo}: {qtd}")


if __name__ == "__main__":
    processar_validacao()
