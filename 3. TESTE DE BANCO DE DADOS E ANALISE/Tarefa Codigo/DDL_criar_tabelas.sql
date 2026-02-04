-- =====================================================
-- TESTE 3 - TAREFA 3.2 (AJUSTADO AOS CSVs ATUAIS)
-- PostgreSQL
-- =====================================================

DROP TABLE IF EXISTS despesas_consolidadas CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
DROP TABLE IF EXISTS operadoras CASCADE;

-- Operadoras (derivada do enriquecido.csv)
-- OBS: "RegistroANS" no seu arquivo está como data (YYYY-MM-DD) e NÃO é único.
CREATE TABLE operadoras (
    cnpj CHAR(14) PRIMARY KEY,
    razao_social TEXT NOT NULL,
    registro_ans TEXT NULL,
    modalidade TEXT NULL,
    uf CHAR(2) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_operadoras_uf ON operadoras(uf);
CREATE INDEX idx_operadoras_razao ON operadoras(razao_social);

-- Despesas consolidadas (consolidado_despesas.csv)
-- Trimestre no seu CSV é 1T/2T/3T -> vamos armazenar como SMALLINT 1..4
CREATE TABLE despesas_consolidadas (
    cnpj CHAR(14) NOT NULL REFERENCES operadoras(cnpj) ON DELETE CASCADE,
    ano SMALLINT NOT NULL,
    trimestre SMALLINT NOT NULL,
    valor_despesas NUMERIC(18,2) NOT NULL,

    CONSTRAINT pk_despesas_consolidadas PRIMARY KEY (cnpj, ano, trimestre),
    CONSTRAINT ck_trimestre CHECK (trimestre BETWEEN 1 AND 4),
    CONSTRAINT ck_valor CHECK (valor_despesas >= 0)
);

CREATE INDEX idx_despesas_periodo ON despesas_consolidadas(ano, trimestre);
CREATE INDEX idx_despesas_cnpj ON despesas_consolidadas(cnpj);

-- Despesas agregadas (despesas_agregadas.csv)
-- Seu CSV agrega por RazaoSocial+UF e NÃO traz CNPJ, então não dá pra FK.
CREATE TABLE despesas_agregadas (
    razao_social TEXT NOT NULL,
    uf CHAR(2) NOT NULL,
    total_despesas NUMERIC(18,2) NOT NULL,
    media_por_trimestre NUMERIC(18,2) NULL,
    desvio_padrao NUMERIC(18,2) NULL,
    qtd_registros INTEGER NULL,
    qtd_trimestres INTEGER NULL,

    CONSTRAINT pk_despesas_agregadas PRIMARY KEY (razao_social, uf)
);

CREATE INDEX idx_agregadas_uf ON despesas_agregadas(uf);

SELECT 'DDL OK' AS status;
