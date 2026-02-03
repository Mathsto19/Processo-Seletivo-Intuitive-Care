# Teste 1 – API da ANS (Demonstrações Contábeis)

Esse projeto baixa os dados das demonstrações contábeis da ANS, filtra as despesas com eventos/sinistros e consolida tudo num CSV.

## Estrutura do projeto

```
1. TESTE DE INTEGRAÇÃO COM API PÚBLICA/
├── 1.1. Acesso à API de Dados Abertos da ANS/
│   └── Identificar_arquivos.py
├── 1.2. Processamento de Arquivos/
│   └── Baixar_extrair_processar.py
├── 1.3. Consolidação e Análise de Inconsistências/
│   └── Consolidar_e_gerar_zip.py
├── Dados/
│   ├── Extraído/           # ZIPs baixados + arquivos extraídos
│   │   ├── 1T2025/
│   │   ├── 2T2025/
│   │   └── 3T2025/
│   ├── Normal/             # CSVs intermediários (por trimestre)
│   │   ├── despesas_eventos_sinistros_1T2025.csv
│   │   ├── despesas_eventos_sinistros_2T2025.csv
│   │   └── despesas_eventos_sinistros_3T2025.csv
│   └── Saída/              # Output final
│       ├── consolidado_despesas.csv
│       └── consolidado_despesas.zip
├── Documentos/
│   ├── Relatorio_cadop.csv              # Cadastro de operadoras (ANS)
│   ├── relatorio_erros.csv              # Erros de processamento
│   ├── relatorio_inconsistencias.csv    # Inconsistências encontradas
│   └── Ultimos_3_trimestres.json        # Manifesto dos trimestres
├── README.md
└── requirements.txt
```

## Como funciona

Dividi em 3 etapas:

### 1.1 – Buscar os últimos 3 trimestres
Acessa o FTP da ANS, identifica os 3 trimestres mais recentes e salva a lista de URLs num JSON. Isso evita ter que refazer a busca toda vez que rodar as etapas seguintes.

### 1.2 – Baixar e processar
Aqui é onde fica pesado:
- Baixa os ZIPs (alguns trimestres têm mais de um arquivo)
- Extrai tudo automaticamente
- Lê CSV/TXT/XLSX (a estrutura varia bastante entre trimestres)
- Filtra só as linhas de "Eventos/Sinistros"
- Agrega por operadora (REG_ANS)

Os arquivos não têm CNPJ direto, só REG_ANS (código da operadora na ANS). Por isso preciso fazer JOIN com o CADOP depois.

### 1.3 – Consolidar e zipar
Junta os 3 trimestres, faz JOIN com o cadastro de operadoras (CADOP) pra pegar CNPJ/Razão Social, e gera o CSV final com as 5 colunas pedidas mais o ZIP.

## Inconsistências que encontrei

### CNPJs duplicados com nomes diferentes
Usei o CADOP como fonte da verdade. Se apareceu divergência, deixei documentado no relatório mas não tentei adivinhar qual era o certo.

### Valores zerados ou negativos
Deixei no CSV. Pode ser ajuste contábil legítimo (reversões, etc), então não quis forçar tudo pra positivo. Só marquei no relatório.

### Conta contábil "41"
Os arquivos têm hierarquia de contas (ex: 41, 4111, 411101). Usei só a conta 41 quando existia, senão ia somar tudo duplicado.

## Como rodar

**Requisitos:**
- Python 3.10+
- `pip install -r requirements.txt`

**Executar na ordem:**

```bash
# 1. Identificar os últimos 3 trimestres
python "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA/1.1. Acesso à API de Dados Abertos da ANS/Identificar_arquivos.py"

# 2. Baixar e processar os dados
python "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA/1.2. Processamento de Arquivos/Baixar_extrair_processar.py"

# 3. Consolidar e gerar o ZIP final
python "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA/1.3. Consolidação e Análise de Inconsistências/Consolidar_e_gerar_zip.py"
```

**Output esperado:**

```
Processando 3 trimestres...

Trimestre 1T2025:
  Baixando: 1T2025.zip
  Extraindo: 1T2025.zip
  Processando arquivos...
  Operadoras agregadas (REG_ANS): 719
  Total: 123456789.50

  Salvo: despesas_eventos_sinistros_1T2025.csv

[... mesma coisa pros outros 2 trimestres ...]

Consolidando trimestres...

Linhas no consolidado: 2157
Total consolidado: R$ 209.231.761.123,57

CSV final: consolidado_despesas.csv
ZIP final: consolidado_despesas.zip
```

## Observações

- O CSV usa `;` como separador (padrão ANS). Se abrir no Excel, escolhe "importar dados" e marca `;` como delimitador.
- Os logs mostram valores em reais formatados (R$ 1.234,56), mas o CSV mantém formato numérico padrão (1234.56) pra facilitar se precisar reprocessar.
- Não versionar `Dados/Extraído/` no Git (são aproximadamente 2GB de ZIPs e CSVs brutos). Só código e outputs finais.

## Arquivos gerados

### Dados processados
- `Dados/Normal/*.csv` - CSVs intermediários (um por trimestre)
- `Dados/Saída/consolidado_despesas.csv` - CSV final consolidado
- `Dados/Saída/consolidado_despesas.zip` - ZIP do CSV final

### Relatórios
- `Documentos/relatorio_inconsistencias.csv` - Problemas encontrados (CNPJs duplicados, valores zerados, etc)
- `Documentos/relatorio_erros.csv` - Erros de processamento (arquivos corrompidos, colunas não reconhecidas, etc)
- `Documentos/Relatorio_cadop.csv` - Cadastro de operadoras da ANS (baixado automaticamente)
- `Documentos/Ultimos_3_trimestres.json` - Manifesto dos trimestres identificados

## Decisões técnicas

### Processamento em chunks
Optei por processar os arquivos em lotes de 200 mil linhas porque alguns CSVs têm mais de 700 mil linhas. Isso mantém o consumo de memória controlado e permite rodar o script mesmo em máquinas com 4GB de RAM.

### Conta contábil "41"
Quando o arquivo tem a coluna de conta contábil, uso apenas os registros com conta "41" (que é o código padrão da ANS para Eventos/Sinistros). Isso evita somar a mesma despesa várias vezes por causa da hierarquia de contas. Se não tiver a coluna de conta, aí uso o filtro por descrição (procurando por "EVENTO" ou "SINISTRO").

### REG_ANS como chave primária
Os arquivos contábeis da ANS só trazem o REG_ANS (código de registro da operadora), não o CNPJ. Por isso agregei tudo por REG_ANS primeiro e depois fiz JOIN com o CADOP (cadastro oficial da ANS) pra pegar CNPJ e Razão Social. Algumas operadoras podem não ter match no CADOP, e esses casos ficam documentados no relatório de inconsistências.