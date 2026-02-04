# Teste 3 - Banco de Dados e Análise (PostgreSQL)

Entrega do Teste 3 (Banco de Dados e Análise) do processo seletivo da IntuitiveCare.

## Objetivo

1. Criar estrutura SQL (DDL) para armazenar:
   - Despesas consolidadas por operadora/trimestre
   - Cadastro de operadoras
   - Agregados por UF/razão social
2. Importar os CSVs gerados nos testes anteriores, tratando inconsistências de encoding e dados
3. Executar queries analíticas e salvar resultados

Banco utilizado: PostgreSQL (versão 10 ou superior).

A importação foi feita via Python + psycopg2 por robustez no tratamento de encoding e limpeza de dados, mas as tabelas e análises são em SQL puro.

---

## Estrutura de pastas

```
TESTE DE BANCO DE DADOS E ANALISE/
├── Preparacao/
│   ├── consolidado_despesas.csv
│   ├── despesas_agregadas.csv
│   ├── enriquecido.csv
│   ├── consolidado_despesas.utf8.csv
│   ├── despesas_agregadas.utf8.csv
│   └── enriquecido.utf8.csv
├── Tarefa Codigo/
│   ├── Reset.sql
│   ├── DDL_criar_tabelas.sql
│   ├── Verificar_tabelas.sql
│   └── Importar_dados.py
├── Queries/
│   ├── Query_1.sql
│   ├── Query_2.sql
│   ├── Query_3.sql
│   └── Query_3_1.sql
├── Resultados/
│   ├── Query_1.txt
│   ├── Query_2.txt
│   ├── Query_3.txt
│   └── Query_3_1.txt
├── requirements.txt
├── Trazer_arquivos.py
└── README.md
```

---

## Pré-requisitos

- PostgreSQL instalado e rodando (versão 10 ou superior)
- Python 3.10+
- Dependências Python:

```bash
pip install -r requirements.txt
```

---

## Como executar (fluxo recomendado)

### 1. Criar/usar um banco no PostgreSQL

Exemplo via pgAdmin ou psql:

```sql
CREATE DATABASE intuitive_care_db;
```

### 2. Reset + DDL (opcional)

Se você quiser executar os scripts SQL separadamente:

```bash
psql -U postgres -d intuitive_care_db
```

Dentro do psql:

```sql
\cd 'C:/Users/mathe/Downloads/Intuitivecare-teste-2026/3. TESTE DE BANCO DE DADOS E ANALISE/Tarefa Codigo'
\i 'Reset.sql'
\i 'DDL_criar_tabelas.sql'
\i 'Verificar_tabelas.sql'
```

Nota: O script `Importar_dados.py` já cria as tabelas automaticamente. Os arquivos SQL separados servem como entrega formal e para validação isolada da estrutura.

### 3. Importar CSVs (recomendado)

Execute o importador Python:

```bash
python "Tarefa Codigo/Importar_dados.py"
```

O script realiza:
- Gera versões `*.utf8.csv` em `Preparacao/` quando necessário
- Cria tabelas staging temporárias
- Executa COPY para staging
- Transforma, valida e carrega dados nas tabelas finais
- Registra rejeições na tabela `import_rejeicoes`
- Imprime resumo final de contagens

### 4. Executar queries analíticas e salvar resultados

Dentro do psql:

```sql
\cd 'C:/Users/mathe/Downloads/Intuitivecare-teste-2026/3. TESTE DE BANCO DE DADOS E ANALISE'

\o 'Resultados/Query_1.txt'
\i 'Queries/Query_1.sql'
\o

\o 'Resultados/Query_2.txt'
\i 'Queries/Query_2.sql'
\o

\o 'Resultados/Query_3.txt'
\i 'Queries/Query_3.sql'
\o

\o 'Resultados/Query_3_1.txt'
\i 'Queries/Query_3_1.sql'
\o
```

---

## Modelo de dados (tabelas finais)

### operadoras

- `cnpj` (PK) - CHAR(14)
- `razao_social` - TEXT
- `registro_ans` - TEXT (mantido como texto por inconsistência no CSV)
- `modalidade` - TEXT
- `uf` - CHAR(2)
- Índices: UF e razão social

### despesas_consolidadas

- PK composta: `(cnpj, ano, trimestre)`
- `valor_despesas` - NUMERIC(18,2)
- FK: `cnpj` -> `operadoras(cnpj)`
- Índices: período e cnpj

### despesas_agregadas

- PK composta: `(razao_social, uf)`
- Campos: total, média, desvio padrão, contagens
- Sem FK (CSV agregado não traz CNPJ)

### import_rejeicoes

- Log de linhas problemáticas durante a carga
- Armazena motivo + detalhe + linha em JSONB

---

## Tratamento de inconsistências na importação

### Principais problemas encontrados

#### Encoding inválido (WIN1252/bytes não mapeáveis para UTF-8)

Solução: Gerar `*.utf8.csv` por "best effort" (utf-8-sig com fallback para latin1) e remover caracteres de controle.

Justificativa: Evita falhas do COPY e mantém o processo reprodutível.

#### NULL em campos obrigatórios (ex: razão social, CNPJ)

Solução: Rejeitar e registrar em `import_rejeicoes` quando inviabiliza a carga. Em casos específicos, usar placeholder controlado para permitir FK (ex: operadora sem razão social, mas com CNPJ válido).

#### Strings em campos numéricos

Solução: Staging como TEXT + conversão com regex (remove símbolos, troca vírgula por ponto). Se não converter, rejeita e registra.

#### Datas/formatos inconsistentes

Solução: Não forçar tipos DATE no cadastro (registro_ans mantido como TEXT), pois o CSV não garante unicidade nem formato confiável. Mantém flexibilidade e evita rejeição desnecessária.

#### Duplicatas na mesma chave lógica

Solução: Para `despesas_consolidadas`, duplicatas por `(cnpj, ano, trimestre)` são agregadas antes do insert final. Duplicatas são registradas em `import_rejeicoes` para auditoria.

---

## Queries analíticas

### Query 1 - Top 5 crescimento percentual entre primeiro e último trimestre

Arquivo: `Queries/Query_1.sql`

Considera o primeiro e o último período disponível no conjunto. Operadoras sem dados no primeiro ou no último período não entram no ranking.

Justificativa: Crescimento percentual exige ponto inicial e final. Sem isso, a métrica fica enviesada ou ambígua.

### Query 2 - Top 5 UFs por despesa total + média por operadora

Arquivo: `Queries/Query_2.sql`

Calcula total de despesas por UF e a média por operadora em cada UF, evitando distorção por operadoras com mais ou menos trimestres.

### Query 3 - Operadoras acima da média em 2+ dos 3 trimestres analisados

Arquivo: `Queries/Query_3.sql`

Usa os 3 trimestres do recorte analisado e mede, por operadora, em quantos desses 3 períodos o valor ficou acima da média geral. Retorna a contagem final de operadoras que atendem o critério.

Arquivo extra: `Queries/Query_3_1.sql` - Versão detalhada para listar quais operadoras atendem o critério.

---

## Trade-offs e decisões técnicas

### Normalização

Opções consideradas:
- Opção A: Tabela desnormalizada com todos os dados
- Opção B: Tabelas normalizadas separadas (operadoras, despesas_consolidadas, despesas_agregadas)

Decisão escolhida: Opção B

Justificativa:
- Reduz redundância (UF/modalidade/razão não repetem em cada linha de despesa)
- Permite índices específicos e queries analíticas mais claras
- Evita inconsistências em atualizações (cadastro muda sem reescrever histórico)
- Requer JOIN nas análises (custo adicional aceitável)
- Volume esperado tende a crescer por trimestre. Normalização mantém o esquema mais estável e análises mais confiáveis.

### Tipos de dados (valores monetários)

Opções consideradas:
- Opção A: FLOAT
- Opção B: DECIMAL/NUMERIC
- Opção C: INTEGER (centavos)

Decisão escolhida: Opção B (NUMERIC(18,2))

Justificativa:
- Precisão decimal (evita erro de ponto flutuante)
- Boa compatibilidade com agregações e relatórios
- Pode ser um pouco mais lento que INTEGER, mas valores financeiros pedem precisão. Performance não é o gargalo principal neste contexto.

### Tipos de dados (datas)

Opções consideradas:
- Opção A: DATE/TIMESTAMP
- Opção B: VARCHAR/TEXT

Decisão escolhida: Opção B (TEXT para registro_ans)

Justificativa:
- Evita falhas por inconsistência de formato no CSV
- Mantém flexibilidade (o campo no CSV pode não ser data real e pode não ser único)
- Perde validação automática de datas no banco, mas o CSV cadastral não garante consistência. Priorizei carga robusta e auditável.

### Query 3 (abordagem)

Opções consideradas:
- Opção A: Subquery correlacionada
- Opção B: Window functions
- Opção C: CTEs (cálculo em etapas)

Decisão escolhida: Opção C (CTEs)

Justificativa:
- Fácil de ler, manter e debugar (cada etapa é verificável)
- Boa performance para o volume esperado
- Mais linhas de SQL, mas melhor equilíbrio entre legibilidade, manutenibilidade e robustez.

---

## Evidências de execução

A pasta `Resultados/` contém a saída das queries (*.txt), gerada via psql com `\o` + `\i`.

---

## Dependências

Ver arquivo `requirements.txt`:

```
psycopg2-binary>=2.9.9
