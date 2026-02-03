# Etapa 2.2 — Enriquecimento de Dados

Esta etapa realiza o join entre os dados validados da etapa 2.1 e o cadastro de operadoras ativas da ANS, adicionando informações complementares necessárias para análises posteriores.

## Objetivo

Enriquecer o dataset validado com dados cadastrais usando CNPJ como chave, adicionando as colunas:
- **RegistroANS** - Número de registro da operadora na ANS
- **Modalidade** - Tipo de operadora (Autogestão, Cooperativa Médica, etc.)
- **UF** - Unidade federativa da sede da operadora

## Arquivos

### Entrada
- `Dados/Entradas/validados.csv` (gerado na etapa 2.1)
- `Dados/Entradas/operadoras_cadastro.csv` (baixado automaticamente da ANS se ausente)

### Saídas
- `Dados/Saídas/enriquecido.csv`  
  Registros com match bem-sucedido (inclui RegistroANS, Modalidade e UF)
- `Dados/Saídas/sem_match.csv`  
  Registros sem correspondência no cadastro ANS
- `Dados/Saídas/cadastro_duplicados.csv`  
  CNPJs que aparecem múltiplas vezes no cadastro com dados divergentes
- `Dados/Saídas/resumo_enriquecimento.json`  
  Estatísticas de execução (totais, matches, divergências)

## Como executar

```bash
python Processar_enriquecimento.py
```

O script busca automaticamente o validados.csv da etapa 2.1 e baixa o cadastro ANS se necessário.

## Processamento

1. Leitura e normalização dos CNPJs do dataset validado
2. Download do cadastro de operadoras ativas (CADOP) da ANS se ausente
3. Detecção automática das colunas do cadastro (nomes podem variar entre versões)
4. Deduplicação do cadastro para garantir 1 linha por CNPJ
5. JOIN usando CNPJ como chave (left join para preservar todos os registros validados)
6. Separação entre registros com match e sem match

## Tratamento de inconsistências

### CNPJs sem match no cadastro

Registros do dataset validado que não encontram correspondência no cadastro ANS são preservados em `sem_match.csv`. Isso ocorre porque:
- Operadoras podem ter encerrado atividades mas ainda constam nos dados contábeis
- Cadastro refere-se apenas a operadoras ativas no momento do download
- Diferenças de timing entre publicação dos dados

Esses registros não são descartados para permitir auditoria e análise posterior.

### CNPJs duplicados no cadastro com dados divergentes

O cadastro ANS pode conter o mesmo CNPJ múltiplas vezes com valores diferentes de RegistroANS, Modalidade ou UF. Isso violaria a premissa do join m:1 (muitos registros de despesas para um cadastro) e duplicaria indevidamente as despesas no resultado final.

**Solução implementada:**
- CNPJs com combinações divergentes são identificados e exportados em `cadastro_duplicados.csv`
- Aplica-se critério de desempate determinístico para escolher uma única linha:
  1. Prioridade para registro com maior número de campos preenchidos (completude)
  2. Desempate secundário pelo menor RegistroANS numérico
- O dataset final mantém cardinalidade m:1 sem duplicação artificial de despesas

## Trade-off: Estratégia de JOIN e deduplicação

### Opções consideradas:

**Opção A:** JOIN direto sem deduplicar o cadastro

**Opção B:** Deduplicar o cadastro antes do JOIN usando critério determinístico

**Opção C:** Processar em banco de dados (SQL) com deduplicação via query

### Decisão escolhida: Opção B

### Justificativa:

**Prós:**
- Garante relacionamento m:1 entre despesas e cadastro (previne duplicação de valores)
- Volume de dados comporta processamento em memória com pandas
- Mantém rastreabilidade completa via cadastro_duplicados.csv
- Critério de desempate determinístico e auditável

**Contras:**
- Requer definição de regra de escolha entre registros divergentes
- Gera arquivo adicional de auditoria

**Por que essa escolha:**  
A duplicação silenciosa de despesas devido a CNPJs repetidos no cadastro seria um erro crítico que comprometeria todas as análises subsequentes. O volume de dados não justifica a complexidade adicional de processamento em banco nesta etapa.

## Observações

- O leitor de CSV tenta múltiplos encodings (utf-8-sig, latin1, iso-8859-1) e separadores (vírgula, ponto-e-vírgula)
- As colunas do cadastro ANS são mapeadas automaticamente por normalização de nomes
- Se validados.csv estiver vazio, o script gera saídas vazias e encerra sem erro
- O script copia automaticamente validados.csv da etapa 2.1 se não encontrado em Entradas