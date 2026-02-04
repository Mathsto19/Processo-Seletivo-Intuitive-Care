# Teste Estágio IntuitiveCare 2026

Solução completa para o teste prático da IntuitiveCare, implementando integração com dados públicos da ANS, transformação e validação de dados, análise em banco de dados SQL e API REST com interface web.

## Estrutura do Projeto

```
Intuitivecare-teste-2026/
├── 1. TESTE DE INTEGRAÇÃO COM API PÚBLICA/
├── 2. TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS/
├── 3. TESTE DE BANCO DE DADOS E ANALISE/
├── 4. TESTE DE API E INTERFACE WEB/
└── README.md (este arquivo)
```

Cada pasta contém seu próprio README com detalhes específicos de execução e trade-offs técnicos.

## Fluxo de Execução

Os testes formam uma cadeia de processamento:

1. **Teste 1:** Coleta dados da ANS (últimos 3 trimestres)
2. **Teste 2:** Valida, enriquece e agrega os dados
3. **Teste 3:** Importa para PostgreSQL e executa análises SQL
4. **Teste 4:** Expõe API REST e interface web

## Requisitos

### Ferramentas
- Python 3.10+
- PostgreSQL 10+
- Node.js 18+ (para frontend Vue.js)
- Git (para clonar o repositório)

### Opcional
- Postman (para testar API)

## Execução Rápida

### Teste 1: Integração com API da ANS

```bash
cd "1. TESTE DE INTEGRAÇÃO COM API PÚBLICA"
python "1.1. Acesso à API de Dados Abertos da ANS/Identificar_arquivos.py"
python "1.2. Processamento de Arquivos/Baixar_extrair_processar.py"
python "1.3. Consolidação e Análise de Inconsistências/Consolidar_e_gerar_zip.py"
```

**Saída:** `Dados/Saída/consolidado_despesas.csv`

---

### Teste 2: Transformação e Validação

```bash
cd "2. TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS"
cd "2.1. Validação de Dados com Estratégias Diferentes"
python Processar_validacao.py

cd "../2.2. Enriquecimento de Dados com Tratamento de Falhas"
python Processar_enriquecimento.py

cd "../2.3. Agregação com Múltiplas Estratégias"
python Processar_agregacao.py
```

**Saídas:** `validados.csv`, `enriquecido.csv`, `despesas_agregadas.csv`

---

### Teste 3: Banco de Dados SQL

```bash
cd "3. TESTE DE BANCO DE DADOS E ANALISE"
pip install -r requirements.txt

# Criar banco no PostgreSQL
# CREATE DATABASE intuitive_care_db;

# Importar dados
python "Tarefa Codigo/Importar_dados.py"

# Executar queries (via psql ou script)
psql -d intuitive_care_db -f "Queries/Query_1.sql"
```

**Saídas:** Tabelas populadas e resultados em `Resultados/`

---

### Teste 4: API e Interface Web

#### Backend

```bash
cd "4. TESTE DE API E INTERFACE WEB/Backend"

# Exportar dados do banco para CSV (se necessário)
$env:DATABASE_URL="postgresql+psycopg2://postgres:senha@localhost:5432/intuitive_care_db"
python database.py

# Subir API
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

**API:** http://localhost:8000
**Documentação:** http://localhost:8000/docs

#### Frontend

```bash
cd "4. TESTE DE API E INTERFACE WEB/Frontend"
npm install
npm run dev
```

**Interface:** http://localhost:5173

#### Postman

Coleção em: `4. TESTE DE API E INTERFACE WEB/Postman/IntuitiveCare_API.postman_collection.json`

**Como usar:**
1. Abrir Postman
2. Import → File → Selecionar JSON
3. Executar requisições (backend rodando)

---

## Endpoints da API (Teste 4)

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/api/operadoras` | Lista operadoras (paginação e busca) |
| GET | `/api/operadoras/{cnpj}` | Detalhes de uma operadora |
| GET | `/api/operadoras/{cnpj}/despesas` | Histórico de despesas |
| GET | `/api/estatisticas` | Estatísticas agregadas |
| GET | `/health` | Health check |

## Trade-offs Técnicos (Resumo)

### Backend (Teste 4)

**Framework: FastAPI**
- Documentação automática, validação com Pydantic, tipagem forte
- Escolhido pela modernidade e facilidade de avaliação

**Paginação: Offset-based**
- Adequado para volume moderado e dados estáticos
- Permite navegação direta entre páginas

**Estatísticas: Pré-calculadas**
- Dados carregados de CSV no startup
- Performance superior sem necessidade de cache

**Resposta: Dados + Metadados**
- Facilita UX no frontend (total, páginas, navegação)

### Frontend (Teste 4)

**Busca: Server-side**
- Evita carregar todos os dados no cliente
- Mantém consistência com paginação

**Estado: Local por componente**
- Aplicação pequena, sem necessidade de Pinia/Vuex
- Reduz dependências e complexidade

**Performance: Paginação no backend**
- Renderiza apenas itens visíveis
- Mantém DOM leve

**Erros: Mensagens específicas**
- Feedback claro em loading, erro e dados vazios
- Sem exposição de detalhes técnicos sensíveis

### Banco de Dados (Teste 3)

**Modelagem: Normalizada**
- Tabelas separadas (operadoras, despesas, agregados)
- Reduz redundância e facilita análises

**Tipo monetário: NUMERIC(18,2)**
- Precisão decimal para valores financeiros
- Evita erros de arredondamento

**Importação: Staging + Validação**
- Staging em TEXT com conversões controladas
- Log de rejeições para auditoria

### Dados (Testes 1 e 2)

**Teste 1: Resiliente a variações**
- Suporta CSV/TXT/XLSX
- Registra inconsistências encontradas

**Teste 2: Etapas separadas**
- Validação → Enriquecimento → Agregação
- Facilita rastreabilidade e reprocessamento

## Instruções de Entrega

**Prazo:** 7 dias a partir do recebimento

**Formato:**
- Compactar tudo em ZIP único
- Nome do arquivo: `Teste_{seu_nome}.zip`

**Envio:**
- Novo e-mail (não responder o convite)
- Assunto: copiar exatamente do e-mail de convite
- Anexar o ZIP

## Tecnologias Utilizadas

- **Python 3.10+** (scripts, API)
- **FastAPI** (backend REST)
- **Vue.js 3** (frontend)
- **PostgreSQL** (banco de dados)
- **Pandas** (processamento de dados)
- **Chart.js** (visualização)
- **SQLAlchemy** (ORM)

## Documentação Adicional

Cada teste possui README próprio com:
- Detalhes de execução
- Trade-offs específicos documentados
- Exemplos de saída
- Tratamento de casos extremos

## Notas

- Scripts validam dados e registram inconsistências
- Banco possui índices para otimizar consultas
- API possui documentação interativa (Swagger)
- Frontend trata estados de loading, erro e dados vazios
- Código segue boas práticas (KISS, tratamento de erros, validações)

## Dúvidas

Para dúvidas sobre a implementação, consultar os READMEs específicos de cada teste ou a documentação inline no código.
