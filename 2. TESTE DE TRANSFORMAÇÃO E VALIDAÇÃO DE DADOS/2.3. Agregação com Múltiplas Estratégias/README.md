# Etapa 2.3 — Agregação de Despesas

Esta etapa realiza a agregação dos dados enriquecidos, agrupando por operadora e unidade federativa para gerar um relatório consolidado com estatísticas descritivas.

## Objetivo

Agrupar dados por **RazaoSocial** e **UF** e calcular:
- Total de despesas por grupo
- Média de despesas por trimestre
- Desvio padrão das despesas
- Ordenar resultado por total de despesas (decrescente)

## Arquivos

### Entrada
- `Dados/Entradas/enriquecido.csv` (gerado na etapa 2.2)

O script busca automaticamente na etapa 2.2 se não encontrar na pasta Entradas.

### Saídas
- `Dados/Saídas/despesas_agregadas.csv`  
  Resultado final agregado e ordenado
- `Dados/Saídas/resumo_agregacao.json`  
  Estatísticas de execução
- `Dados/Saídas/Teste_Matheus.zip`  
  Arquivo compactado contendo despesas_agregadas.csv

## Como executar

```bash
python Processar_agregacao.py
```

Para alterar o nome do arquivo ZIP, edite a última linha do script:

```python
processar_agregacao(nome_zip="SeuNome.zip")
```

## Estrutura do resultado

O arquivo `despesas_agregadas.csv` contém uma linha por grupo (RazaoSocial, UF) com as colunas:

- **RazaoSocial** - Nome da operadora
- **UF** - Unidade federativa
- **total_despesas** - Soma de todas as despesas do grupo
- **media_por_trimestre** - Média aritmética das despesas
- **desvio_padrao** - Desvio padrão das despesas (identifica variabilidade)
- **qtd_registros** - Número de linhas que compõem o grupo
- **qtd_trimestres** - Número de trimestres distintos no grupo

**Ordenação:** Decrescente por `total_despesas` (maiores despesas primeiro)

## Processamento

1. Leitura do dataset enriquecido
2. Agrupamento por (RazaoSocial, UF)
3. Cálculo de agregações:
   - Soma total das despesas
   - Média aritmética dos valores
   - Desvio padrão dos valores
   - Contagem de registros e trimestres distintos
4. Substituição de desvio padrão NaN por 0.0 (grupos com registro único)
5. Ordenação determinística (mergesort) por total decrescente
6. Exportação para CSV e compactação em ZIP

## Notas sobre cálculos estatísticos

### Média por trimestre

A média calculada é a média aritmética simples de todos os registros do grupo. Como o dataset consolidado pode ter múltiplas linhas por operadora/trimestre (dependendo da granularidade dos dados de origem), a média reflete os valores tal como aparecem no enriquecido.csv.

### Desvio padrão

Calculado usando pandas (método padrão com ddof=1). Grupos com apenas um registro retornam NaN, que é substituído por 0.0 para evitar valores nulos no CSV final.

## Trade-off: Estratégia de ordenação

### Opções consideradas:

**Opção A:** Ordenar em memória com pandas (sort_values)

**Opção B:** Ordenação externa (algoritmo disk-based para volumes massivos)

**Opção C:** Processar em banco de dados (ORDER BY em SQL)

### Decisão escolhida: Opção A

### Justificativa:

**Prós:**
- Dataset agregado tem volume reduzido (uma linha por operadora/UF)
- Ordenação em memória é rápida e determinística (mergesort)
- Código simples e fácil de manter
- Adequado para volumes pequenos e médios

**Contras:**
- Não escalaria para milhões de grupos (limitação de memória)
- Depende de RAM disponível

**Por que essa escolha:**  
Após a agregação, o volume de dados se reduz drasticamente (apenas grupos únicos). Para o contexto do problema, ordenação em memória é suficiente e evita complexidade desnecessária. Alternativas como ordenação externa ou SQL só fariam sentido se o dataset pós-agregação excedesse a capacidade de RAM, o que não é o caso aqui.

## Observações

- O leitor de CSV tenta múltiplos encodings e separadores automaticamente
- Se enriquecido.csv estiver vazio, o script gera saídas vazias e encerra sem erro
- O arquivo ZIP é criado automaticamente na pasta Saídas
- Ordenação usa `kind='mergesort'` para garantir estabilidade e reprodutibilidade