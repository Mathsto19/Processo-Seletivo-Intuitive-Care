# Teste 2 — Transformação e Validação de Dados

Pipeline completo de validação, enriquecimento e agregação dos dados de despesas das operadoras de saúde. Este módulo processa o CSV consolidado gerado no Teste 1 através de três etapas sequenciais.

## Visão geral do fluxo

```
consolidado_teste1.csv (Teste 1)
        |
        v
   [2.1] Validação
        |
        v
   validados.csv
        |
        v
   [2.2] Enriquecimento + Cadastro ANS
        |
        v
   enriquecido.csv
        |
        v
   [2.3] Agregação + Estatísticas
        |
        v
   despesas_agregadas.csv + Teste_Matheus.zip
```

## Estrutura do diretório

```
2. TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS/
├── README.md (este arquivo)
├── requirements.txt
│
├── 2.1. Validação de Dados com Estratégias Diferentes/
│   ├── README.md
│   ├── Processar_validacao.py
│   └── Dados/
│       ├── Entradas/consolidado_teste1.csv
│       └── Saídas/validados.csv, invalidos.csv, resumo_validacao.json
│
├── 2.2. Enriquecimento de Dados com Tratamento de Falhas/
│   ├── README.md
│   ├── Baixar_cadastro.py
│   ├── Processar_enriquecimento.py
│   └── Dados/
│       ├── Entradas/validados.csv, operadoras_cadastro.csv
│       └── Saídas/enriquecido.csv, sem_match.csv, cadastro_duplicados.csv
│
└── 2.3. Agregação com Múltiplas Estratégias/
    ├── README.md
    ├── Processar_agregacao.py
    └── Dados/
        ├── Entradas/enriquecido.csv
        └── Saídas/despesas_agregadas.csv, Teste_Matheus.zip
```

## Como executar

Execute as etapas em ordem:

```bash
# Etapa 2.1 - Validação
cd "2.1. Validação de Dados com Estratégias Diferentes"
python Processar_validacao.py

# Etapa 2.2 - Enriquecimento
cd "../2.2. Enriquecimento de Dados com Tratamento de Falhas"
python Processar_enriquecimento.py

# Etapa 2.3 - Agregação
cd "../2.3. Agregação com Múltiplas Estratégias"
python Processar_agregacao.py
```

### Automações implementadas

Os scripts incluem automações para facilitar a execução:

- A etapa 2.2 baixa automaticamente o cadastro ANS se não existir
- A etapa 2.2 busca validados.csv na saída da 2.1 se não estiver na entrada
- A etapa 2.3 busca enriquecido.csv na saída da 2.2 se não estiver na entrada

## Etapa 2.1 — Validação

Aplica validações nos dados consolidados para garantir qualidade antes do enriquecimento.

### Regras de validação

- **CNPJ**: formato válido com 14 dígitos e dígitos verificadores corretos
- **RazaoSocial**: não pode estar vazia (rejeita nan, null, none)
- **ValorDespesas**: numérico e maior que zero (aceita formatos BR e US)

### Saídas

- `validados.csv` - Registros aprovados para seguir no pipeline
- `invalidos.csv` - Registros rejeitados com motivo da rejeição
- `resumo_validacao.json` - Estatísticas (total, válidos, inválidos, taxa de rejeição)

### Trade-off: Tratamento de CNPJs inválidos

**Opções consideradas:**
- Opção A: Corrigir automaticamente (completar zeros, recalcular DV)
- Opção B: Manter no dataset com flag cnpj_valido=false
- Opção C: Remover do fluxo principal e exportar para auditoria

**Decisão escolhida:** Opção C

**Justificativa:**

Prós:
- Garante confiabilidade do CNPJ como chave de join na etapa 2.2
- Evita matches incorretos ou dados poluídos no resultado final
- Mantém rastreabilidade completa via invalidos.csv

Contras:
- Registros potencialmente recuperáveis exigem correção manual

Por que essa escolha: O CNPJ é a chave de join da próxima etapa. Priorizar consistência do pipeline e evitar propagação de erros.

## Etapa 2.2 — Enriquecimento

Faz join com o cadastro de operadoras ativas da ANS usando CNPJ como chave, adicionando as colunas RegistroANS, Modalidade e UF.

### Saídas

- `enriquecido.csv` - Registros com match bem-sucedido no cadastro
- `sem_match.csv` - Registros sem correspondência no cadastro ANS
- `cadastro_duplicados.csv` - CNPJs com dados divergentes no cadastro
- `resumo_enriquecimento.json` - Estatísticas do processo

### Tratamento de inconsistências

**CNPJs sem match:** Preservados em arquivo separado para auditoria. Ocorre quando operadoras encerraram atividades mas constam nos dados contábeis.

**CNPJs duplicados no cadastro:** Identificados e exportados em cadastro_duplicados.csv. Para garantir relacionamento m:1 no join, aplica-se critério de desempate determinístico:
1. Prioridade para registro com maior completude (mais campos preenchidos)
2. Desempate pelo menor RegistroANS numérico

### Trade-off: Estratégia de processamento do JOIN

**Opções consideradas:**
- Opção A: pandas.merge em memória
- Opção B: Processamento em chunks (streaming)
- Opção C: Processamento em banco de dados (SQL)

**Decisão escolhida:** Opção A

**Justificativa:**

Prós:
- Implementação simples e confiável
- Fácil de auditar e debugar
- Adequado para volumes típicos do problema (milhares a dezenas de milhares de registros)

Contras:
- Não escalaria para milhões de linhas sem ajustes

Por que essa escolha: O volume de dados comporta processamento em memória. A complexidade adicional de chunks ou SQL não se justifica para o contexto do teste.

## Etapa 2.3 — Agregação

Agrupa dados por RazaoSocial e UF, calcula estatísticas descritivas e gera arquivo final ordenado.

### Cálculos realizados

- **total_despesas** - Soma das despesas do grupo
- **media_por_trimestre** - Média aritmética dos valores
- **desvio_padrao** - Desvio padrão (NaN substituído por 0.0 em grupos unitários)
- **qtd_registros** - Contagem de linhas no grupo
- **qtd_trimestres** - Trimestres distintos no grupo

### Saídas

- `despesas_agregadas.csv` - Resultado agregado ordenado por total_despesas (decrescente)
- `resumo_agregacao.json` - Estatísticas da execução
- `Teste_Matheus.zip` - Arquivo compactado com despesas_agregadas.csv

### Trade-off: Estratégia de ordenação

**Opções consideradas:**
- Opção A: sort_values em memória (pandas)
- Opção B: Ordenação externa (disk-based)
- Opção C: ORDER BY em SQL

**Decisão escolhida:** Opção A

**Justificativa:**

Prós:
- Dataset pós-agregação tem volume reduzido (uma linha por operadora/UF)
- Ordenação em memória é rápida e determinística
- Código simples e fácil de manter

Contras:
- Não escalaria para milhões de grupos agregados

Por que essa escolha: Após agregação, o volume se reduz drasticamente. Ordenação em memória é adequada e evita complexidade desnecessária.

## Dependências

```
pandas==2.2.2
requests==2.32.3
```

Instale com:

```bash
pip install -r requirements.txt
```

## Arquivos de entrada necessários

Para iniciar o pipeline, garanta que existe:

```
2.1. Validação de Dados com Estratégias Diferentes/Dados/Entradas/consolidado_teste1.csv
```

Este arquivo deve ser copiado da saída do Teste 1.3.

## Observações técnicas

- Todos os scripts tratam múltiplos encodings (utf-8-sig, latin1, iso-8859-1)
- Separadores CSV são detectados automaticamente (vírgula, ponto-e-vírgula)
- Entradas vazias geram saídas vazias sem erro (pipeline continua)
- Cada etapa gera JSON com resumo para facilitar auditoria
