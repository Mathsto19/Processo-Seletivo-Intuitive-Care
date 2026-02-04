
# Teste 4: API e Interface Web

API REST em FastAPI + Interface Web em Vue.js para consulta de operadoras de saúde, histórico de despesas e estatísticas agregadas.

## Estrutura do Projeto

```
4. TESTE DE API E INTERFACE WEB/
├── Backend/
│   ├── app.py
│   ├── database.py
│   └── requirements.txt
├── Data/
│   ├── operadoras.csv
│   ├── despesas.csv
│   └── agregados.csv
├── Frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── OperadorasTable.vue
│   │   │   ├── OperadoraDetalhes.vue
│   │   │   └── GraficoDespesas.vue
│   │   ├── services/
│   │   │   └── api.js
│   │   ├── App.vue
│   │   └── main.js
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── Postman/
│   └── IntuitiveCare_API.postman_collection.json
└── README.md
```

## Como Executar

### Backend

```powershell
cd Backend
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

API disponível em: http://localhost:8000
Documentação Swagger: http://localhost:8000/docs

### Frontend

```powershell
cd Frontend
npm install
npm run dev
```

Interface disponível em: http://localhost:5173

### Exportar dados do banco (opcional)

Se precisar popular os CSVs a partir do banco do Teste 3:

```powershell
cd Backend
$env:DATABASE_URL="postgresql+psycopg2://postgres:senha@localhost:5432/intuitive_care_db"
python database.py
```

## Endpoints da API

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/api/operadoras` | Lista operadoras (paginação: `page`, `limit`, busca: `q`) |
| GET | `/api/operadoras/{cnpj}` | Detalhes de uma operadora |
| GET | `/api/operadoras/{cnpj}/despesas` | Histórico de despesas da operadora |
| GET | `/api/estatisticas` | Estatísticas agregadas e distribuição por UF |
| GET | `/health` | Health check |

### Exemplos

**Listar operadoras (paginação):**
```
GET http://localhost:8000/api/operadoras?page=1&limit=15
```

**Buscar por razão social:**
```
GET http://localhost:8000/api/operadoras?q=unimed
```

**Buscar por CNPJ:**
```
GET http://localhost:8000/api/operadoras?q=00302775
```

**Detalhes de operadora:**
```
GET http://localhost:8000/api/operadoras/00302775000141
```

**Histórico de despesas:**
```
GET http://localhost:8000/api/operadoras/00302775000141/despesas
```

**Estatísticas:**
```
GET http://localhost:8000/api/estatisticas
```

## Funcionalidades Implementadas

### Backend
- Paginação offset-based com metadados (total, pages)
- Busca unificada por razão social ou CNPJ
- Validação de CNPJ (formato 14 dígitos)
- Tratamento de erros HTTP (404, 422, 500)
- Documentação automática (Swagger/OpenAPI)
- CORS configurado para desenvolvimento

### Frontend
- Tabela paginada com navegação
- Busca com debounce (500ms)
- Gráfico de barras (Chart.js) - Top 12 UFs + agregação "OUTROS"
- Página de detalhes com histórico de despesas
- Estados de loading, erro e dados vazios
- Formatação de CNPJ e valores monetários

## Coleção Postman

Arquivo: `Postman/IntuitiveCare_API.postman_collection.json`

**Como usar:**
1. Abrir Postman
2. Import → File → Selecionar o arquivo JSON
3. Executar as requisições (backend deve estar rodando)

**Alternativa:** Usar Swagger em http://localhost:8000/docs

## Trade-offs Técnicos

### Backend

#### 4.2.1 Escolha do Framework

**Opções consideradas:**
- Opção A: Flask
- Opção B: FastAPI

**Decisão escolhida:** FastAPI

**Justificativa:**
- **Prós:** Documentação automática (Swagger), validação de dados com Pydantic, tipagem forte, performance superior (ASGI), padrão moderno
- **Contras:** Curva de aprendizado ligeiramente maior que Flask
- **Por que essa escolha:** Para um teste técnico, FastAPI demonstra conhecimento de tecnologias atuais, gera documentação interativa automática (facilita avaliação), e valida dados sem código adicional

---

#### 4.2.2 Estratégia de Paginação

**Opções consideradas:**
- Opção A: Offset-based (page/limit)
- Opção B: Cursor-based
- Opção C: Keyset pagination

**Decisão escolhida:** Offset-based

**Justificativa:**
- **Prós:** Simples de implementar, permite navegação direta para qualquer página, adequado para datasets estáticos
- **Contras:** Performance degrada com offsets grandes, pode haver inconsistências se dados mudarem durante navegação
- **Por que essa escolha:** Volume moderado (724 operadoras), dados estáticos (sem atualizações frequentes), simplicidade para o contexto do teste. Cursor/Keyset seria melhor para datasets muito grandes com alta taxa de atualização

---

#### 4.2.3 Cache vs Queries Diretas

**Opções consideradas:**
- Opção A: Calcular sempre na hora
- Opção B: Cachear resultado por X minutos
- Opção C: Pré-calcular e armazenar em CSV/tabela

**Decisão escolhida:** Pré-calcular (agregados.csv)

**Justificativa:**
- **Prós:** Tempo de resposta mínimo, cálculos já realizados, sem overhead de cache ou lógica de invalidação
- **Contras:** Se dados do banco mudarem, precisa re-executar exportação
- **Por que essa escolha:** Dados carregados uma vez no startup da aplicação, não mudam em runtime. Foco em performance e simplicidade. Em produção com pipeline ETL, seria ideal manter tabela agregada atualizada

---

#### 4.2.4 Estrutura de Resposta da API

**Opções consideradas:**
- Opção A: Apenas dados `[{...}, {...}]`
- Opção B: Dados + metadados `{data: [...], total: 100, page: 1, limit: 10, total_pages: X}`

**Decisão escolhida:** Dados + metadados

**Justificativa:**
- **Prós:** Frontend pode calcular total de páginas, mostrar "X de Y resultados", habilitar/desabilitar navegação, UX completa
- **Contras:** Resposta ligeiramente maior (overhead desprezível)
- **Por que essa escolha:** Padrão de mercado para APIs REST com paginação, melhora experiência do usuário sem custo significativo

---

### Frontend

#### 4.3.1 Estratégia de Busca/Filtro

**Opções consideradas:**
- Opção A: Busca no servidor (server-side)
- Opção B: Busca no cliente (client-side)
- Opção C: Híbrido

**Decisão escolhida:** Busca no servidor

**Justificativa:**
- **Prós:** Não sobrecarrega cliente, funciona para qualquer volume de dados, mantém paginação consistente
- **Contras:** Cada busca gera requisição HTTP (mitigado com debounce)
- **Por que essa escolha:** Volume de dados moderado (724 operadoras), carregar tudo no cliente seria ineficiente. Server-side mantém coerência com paginação e permite escalar. Client-side só faria sentido com dataset pequeno (~50 registros) carregado inteiro

---

#### 4.3.2 Gerenciamento de Estado

**Opções consideradas:**
- Opção A: Props/Events simples
- Opção B: Vuex/Pinia
- Opção C: Composables (Vue 3)

**Decisão escolhida:** Props/Events + estado local por componente

**Justificativa:**
- **Prós:** Simples, sem dependências extras, fácil manutenção
- **Contras:** Não escala bem para aplicações complexas com muitos componentes compartilhando estado
- **Por que essa escolha:** Aplicação pequena (3 componentes principais), sem necessidade de estado global (não há autenticação, carrinho, preferências compartilhadas). Pinia seria indicado para apps com múltiplas páginas, dados compartilhados e lógica de negócio complexa no frontend

---

#### 4.3.3 Performance da Tabela

**Estratégia:** Paginação no backend + renderização limitada no DOM

**Justificativa:**
- **Prós:** Renderiza apenas 10-20 linhas por vez, mantém DOM leve, performance consistente
- **Contras:** Usuário precisa navegar páginas (não é scroll infinito)
- **Por que essa escolha:** Renderizar centenas de linhas degrada performance. Paginação resolve sem adicionar complexidade. Virtualização (vue-virtual-scroller) seria necessária apenas para scroll infinito com milhares de itens carregados simultaneamente

---

#### 4.3.4 Tratamento de Erros e Loading

**Implementação:**
- **Estados de loading:** Mensagem "Carregando..." em operações assíncronas
- **Erros de rede/API:** Mensagem específica ao usuário (ex: "Falha ao carregar estatísticas")
- **Dados vazios:** Mensagem "Nenhuma operadora encontrada" quando filtro não retorna resultados

**Análise crítica:**
- **Mensagens específicas vs genéricas:** Escolhi mensagens específicas porque ajudam a identificar rapidamente o problema (útil para avaliação técnica). Em produção, erros detalhados iriam para logs/observabilidade, e o usuário veria mensagens mais amigáveis
- **Não expõe stack traces:** Apenas mensagens objetivas, sem detalhes técnicos sensíveis
- **UX:** Evita tela vazia ou loading infinito, sempre há feedback visual claro

---

## Dependências

### Backend
```txt
fastapi>=0.109.0
uvicorn[standard]>=0.27.0
pandas>=2.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
pydantic>=2.0.0
```

### Frontend
```json
{
  "vue": "^3.4.0",
  "vue-router": "^4.2.0",
  "chart.js": "^4.4.0",
  "axios": "^1.6.0"
}
```

## Notas

- A API carrega os CSVs uma vez no startup (suficiente para o escopo do teste)
- Validação de CNPJ é apenas de formato (14 dígitos), não valida dígitos verificadores
- Dados de agregados por UF limitados a Top 12 + "OUTROS" para evitar poluição visual no gráfico
- CORS configurado para aceitar qualquer origem em desenvolvimento (em produção, especificar domínios permitidos)
- Frontend usa proxy do Vite para `/api` apontar para `http://localhost:8000`
