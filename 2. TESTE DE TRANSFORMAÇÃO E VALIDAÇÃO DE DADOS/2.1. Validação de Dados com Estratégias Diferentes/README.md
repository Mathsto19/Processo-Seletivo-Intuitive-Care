# Etapa 2.1 — Validação de Dados

Esta etapa consome o CSV consolidado do Teste 1.3 e aplica validações para garantir consistência antes do enriquecimento (Etapa 2.2).

## Objetivo

Validar:
- **CNPJ** (formato + dígitos verificadores)
- **ValorDespesas** (numérico e **> 0**)
- **RazaoSocial** (não vazia)

## Arquivos

### Entrada
- `Dados/Entradas/consolidado_teste1.csv`

### Saídas
- `Dados/Saídas/validados.csv`  
  Apenas registros válidos, prontos para o join da Etapa 2.2.
- `Dados/Saídas/invalidos.csv`  
  Registros rejeitados, com coluna `motivo_rejeicao`.
- `Dados/Saídas/resumo_validacao.json`  
  Estatísticas de validação (contagens + motivos).

## Como executar

```bash
python Processar_validacao.py
```

## Regras de validação

### CNPJ
- Remove máscara e normaliza para 14 dígitos
- Valida dígitos verificadores (DV)
- Rejeita sequências repetidas (ex.: `00000000000000`)

### RazaoSocial
- Rejeita vazio e valores equivalentes (`nan`, `null`, `none`)

### ValorDespesas
- Converte formato BR/US (ex.: `1.234,56` ou `1,234.56`)
- Remove símbolos (`R$`)
- Rejeita `<= 0` ou não numérico

## Trade-off: Tratamento de CNPJs inválidos

### Opções consideradas:

**Opção A:** Corrigir automaticamente (heurísticas: completar zeros, recalcular DV, etc.)

**Opção B:** Manter no dataset final com flag `cnpj_valido=false`

**Opção C:** Remover do fluxo principal e exportar para auditoria

### Decisão escolhida: Opção C

### Justificativa:

**Prós:**
- Garante que `validados.csv` tenha chave confiável para o join da Etapa 2.2
- Evita correções heurísticas que podem introduzir erros silenciosos
- Mantém rastreabilidade com auditoria via `invalidos.csv`

**Contras:**
- Registros potencialmente recuperáveis exigem correção manual
- Gera um arquivo extra de auditoria

**Por que essa escolha:**  
O join da Etapa 2.2 depende do CNPJ como chave. Priorizei confiabilidade do fluxo principal.

## Observações

- Uma linha pode ter mais de um motivo (ex.: `cnpj_invalido;valor_invalido_ou_nao_positivo`)
- Se o CSV de entrada tiver apenas cabeçalho (0 linhas), o script gera saídas vazias e encerra sem erro
- O leitor de CSV tenta múltiplos encodings e separadores para lidar com variações do arquivo


# Teste 2 — Transformação e Validação de Dados

Pipeline completo de validação, enriquecimento e agregação dos dados de despesas das operadoras de saúde.

## Estrutura

```
teste_2_validacao_transformacao/
├── 2.1_validacao/              # Validação de CNPJ, valores e razão social
├── 2.2_enriquecimento/         # Join com cadastro de operadoras (ANS)
└── 2.3_agregacao/              # Agregação por operadora/UF com estatísticas
```

## Fluxo de execução

```
Teste 1.3 (consolidado)
        ↓
   [2.1] Validação
        ↓
   validados.csv
        ↓
   [2.2] Enriquecimento (+ cadastro ANS)
        ↓
   enriquecido.csv
        ↓
   [2.3] Agregação (grupo por operadora/UF)
        ↓
   agregado_final.csv
```

## Etapas

### [2.1 — Validação](./2.1_validacao/)
Valida CNPJ, valores positivos e razão social. Remove registros inválidos para auditoria.

**Entrada:** `consolidado_teste1.csv` (Teste 1.3)  
**Saída:** `validados.csv`, `invalidos.csv`, `resumo_validacao.json`

### [2.2 — Enriquecimento](./2.2_enriquecimento/)
Faz join com dados cadastrais da ANS. Adiciona `RegistroANS`, `Modalidade` e `UF`.

**Entrada:** `validados.csv` + cadastro ANS  
**Saída:** `enriquecido.csv`, `sem_match.csv`

### [2.3 — Agregação](./2.3_agregacao/)
Agrupa por `RazaoSocial` + `UF`. Calcula total, média por trimestre e desvio padrão.

**Entrada:** `enriquecido.csv`  
**Saída:** `agregado_final.csv`

## Dependências

```bash
pip install pandas requests
```

## Execução sequencial

```bash
# Etapa 2.1
cd 2.1_validacao
python Processar_validacao.py

# Etapa 2.2
cd ../2.2_enriquecimento
python Baixar_cadastro.py
python Processar_enriquecimento.py

# Etapa 2.3
cd ../2.3_agregacao
python Processar_agregacao.py
```

## Trade-offs documentados

Cada etapa contém decisões técnicas justificadas:
- **2.1:** Estratégia de tratamento de CNPJs inválidos
- **2.2:** Estratégia de JOIN e tratamento de duplicatas
- **2.3:** Estratégia de ordenação e agregação