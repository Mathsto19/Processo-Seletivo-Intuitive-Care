import shutil
from pathlib import Path

# Caminho base CORRETO - raiz do repositório
BASE_DIR = Path(r"C:\Users\mathe\Downloads\Intuitivecare-teste-2026")

# Pasta destino
PREPARACAO_DIR = BASE_DIR / "3. TESTE DE BANCO DE DADOS E ANÁLISE" / "Preparação"

# Criar pasta se não existir
PREPARACAO_DIR.mkdir(parents=True, exist_ok=True)

# Mapear origem → destino
arquivos = [
    # CAMINHO CORRETO do consolidado (sem subpasta 1.3)
    (
        BASE_DIR / "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA" / "Dados" / "Saída" / "consolidado_despesas.csv",
        PREPARACAO_DIR / "consolidado_despesas.csv"
    ),
    (
        BASE_DIR / "2. TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS" / "2.3. Agregação com Múltiplas Estratégias" / "Dados" / "Saídas" / "despesas_agregadas.csv",
        PREPARACAO_DIR / "despesas_agregadas.csv"
    ),
    (
        BASE_DIR / "2. TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS" / "2.2. Enriquecimento de Dados com Tratamento de Falhas" / "Dados" / "Saídas" / "enriquecido.csv",
        PREPARACAO_DIR / "enriquecido.csv"
    ),
]

for origem, destino in arquivos:
    if origem.exists():
        shutil.copy2(origem, destino)
        print(f"{destino.name}")
    else:
        print(f"{origem.name} - NÃO ENCONTRADO")
        print(f"   Procurado em: {origem}\n")

print(f"\nArquivos copiados para:")
print(f"{PREPARACAO_DIR}")
