from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PGConnection


BASE_DIR = Path(r"C:\Users\mathe\Downloads\Intuitivecare-teste-2026\3. TESTE DE BANCO DE DADOS E ANALISE")
CSV_DIR = BASE_DIR / "Preparacao"

CSV_FILES = {
    "enriquecido": CSV_DIR / "enriquecido.csv",
    "consolidado": CSV_DIR / "consolidado_despesas.csv",
    "agregadas": CSV_DIR / "despesas_agregadas.csv",
}

DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "intuitive_care_db")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD", "192508")


CONTROL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")


def decodificar_bytes(raw: bytes) -> str:
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("latin1")


def sanitizar_texto(texto: str) -> str:
    texto = CONTROL_CHARS.sub(" ", texto)
    return texto.replace("\r\n", "\n").replace("\r", "\n")


def gerar_copia_utf8(arquivo: Path) -> Path:
    if not arquivo.exists():
        raise FileNotFoundError(f"CSV não encontrado: {arquivo}")

    raw = arquivo.read_bytes()
    txt = sanitizar_texto(decodificar_bytes(raw))

    out = arquivo.with_suffix("")  # remove .csv
    out = out.with_name(out.name + ".utf8.csv")
    out.write_text(txt, encoding="utf-8", newline="\n")
    return out


def detectar_delimitador(arquivo_utf8: Path) -> str:
    linha = arquivo_utf8.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
    candidatos = [",", ";", "\t"]
    return max(candidatos, key=lambda c: linha.count(c))


@dataclass(frozen=True)
class CsvUtf8:
    enriquecido: Path
    consolidado: Path
    agregadas: Path


DDL_SQL = """
DROP TABLE IF EXISTS despesas_consolidadas CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
DROP TABLE IF EXISTS operadoras CASCADE;

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
"""


STAGING_SQL = """
DROP TABLE IF EXISTS stg_enriquecido_raw;
DROP TABLE IF EXISTS stg_consolidado_raw;
DROP TABLE IF EXISTS stg_agregadas_raw;
DROP TABLE IF EXISTS import_rejeicoes;

CREATE TABLE stg_enriquecido_raw (
    cnpj TEXT,
    razao_social TEXT,
    trimestre TEXT,
    ano TEXT,
    valor_despesas TEXT,
    registro_ans TEXT,
    modalidade TEXT,
    uf TEXT
);

CREATE TABLE stg_consolidado_raw (
    cnpj TEXT,
    razao_social TEXT,
    trimestre TEXT,
    ano TEXT,
    valor_despesas TEXT
);

CREATE TABLE stg_agregadas_raw (
    razao_social TEXT,
    uf TEXT,
    total_despesas TEXT,
    media_por_trimestre TEXT,
    desvio_padrao TEXT,
    qtd_registros TEXT,
    qtd_trimestres TEXT
);

CREATE TABLE import_rejeicoes (
    id BIGSERIAL PRIMARY KEY,
    tabela_alvo TEXT NOT NULL,
    motivo TEXT NOT NULL,
    detalhe TEXT NULL,
    linha_raw JSONB NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


TRANSFORM_SQL = r"""
-- enriquecido -> operadoras
DROP TABLE IF EXISTS tmp_enriq_clean;
CREATE TEMP TABLE tmp_enriq_clean AS
WITH base AS (
  SELECT
    regexp_replace(coalesce(cnpj,''), '[^0-9]', '', 'g') AS cnpj_digits,
    razao_social,
    registro_ans,
    modalidade,
    uf,
    row_to_json(t)::jsonb AS raw
  FROM stg_enriquecido_raw t
)
SELECT
    CASE
      WHEN cnpj_digits = '' THEN NULL
      WHEN length(cnpj_digits) BETWEEN 13 AND 14 THEN lpad(cnpj_digits, 14, '0')
      ELSE NULL
    END AS cnpj_clean,
    NULLIF(trim(razao_social), '') AS razao_clean,
    NULLIF(trim(registro_ans), '') AS registro_clean,
    NULLIF(trim(modalidade), '') AS modalidade_clean,
    NULLIF(upper(trim(uf)), '') AS uf_clean,
    raw
FROM base;

INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT 'operadoras', 'cnpj invalido/zerado ou razao_social vazia', NULL, raw
FROM tmp_enriq_clean
WHERE cnpj_clean IS NULL
   OR cnpj_clean = '00000000000000'
   OR cnpj_clean !~ '^[0-9]{14}$'
   OR razao_clean IS NULL;

INSERT INTO operadoras (cnpj, razao_social, registro_ans, modalidade, uf)
SELECT
    cnpj_clean,
    max(razao_clean),
    max(registro_clean),
    max(modalidade_clean),
    max(uf_clean)::char(2)
FROM tmp_enriq_clean
WHERE cnpj_clean ~ '^[0-9]{14}$'
  AND cnpj_clean <> '00000000000000'
  AND razao_clean IS NOT NULL
GROUP BY cnpj_clean
ON CONFLICT (cnpj) DO UPDATE
SET
    razao_social = EXCLUDED.razao_social,
    registro_ans = EXCLUDED.registro_ans,
    modalidade = EXCLUDED.modalidade,
    uf = EXCLUDED.uf;


-- consolidado -> despesas_consolidadas
DROP TABLE IF EXISTS tmp_cons_clean;
CREATE TEMP TABLE tmp_cons_clean AS
WITH base AS (
  SELECT
    regexp_replace(coalesce(cnpj,''), '[^0-9]', '', 'g') AS cnpj_digits,
    razao_social,
    trimestre,
    ano,
    valor_despesas,
    row_to_json(t)::jsonb AS raw
  FROM stg_consolidado_raw t
)
SELECT
    CASE
      WHEN cnpj_digits = '' THEN NULL
      WHEN length(cnpj_digits) BETWEEN 13 AND 14 THEN lpad(cnpj_digits, 14, '0')
      ELSE NULL
    END AS cnpj_clean,
    NULLIF(trim(razao_social), '') AS razao_clean,
    CASE upper(trim(trimestre))
        WHEN '1T' THEN 1 WHEN '2T' THEN 2 WHEN '3T' THEN 3 WHEN '4T' THEN 4
        WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4
        ELSE NULL
    END AS trimestre_int,
    CASE WHEN trim(ano) ~ '^[0-9]{4}$' THEN trim(ano)::smallint ELSE NULL END AS ano_int,
    CASE
        WHEN replace(regexp_replace(coalesce(valor_despesas,''), '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN round(replace(regexp_replace(valor_despesas, '[^0-9,.-]', '', 'g'), ',', '.')::numeric, 2)
        ELSE NULL
    END AS valor_num,
    raw
FROM base;

INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT 'despesas_consolidadas', 'cnpj invalido/zerado', NULL, raw
FROM tmp_cons_clean
WHERE cnpj_clean IS NULL
   OR cnpj_clean = '00000000000000'
   OR cnpj_clean !~ '^[0-9]{14}$';

-- cria "cand" como TEMP TABLE (porque CTE não vive entre instruções)
DROP TABLE IF EXISTS tmp_cand_operadoras;
CREATE TEMP TABLE tmp_cand_operadoras AS
SELECT
  cnpj_clean,
  max(razao_clean) AS razao_max
FROM tmp_cons_clean
WHERE cnpj_clean ~ '^[0-9]{14}$'
  AND cnpj_clean <> '00000000000000'
GROUP BY cnpj_clean;

INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT
  'operadoras',
  'razao_social ausente (placeholder)',
  NULL,
  jsonb_build_object('cnpj', cnpj_clean)
FROM tmp_cand_operadoras
WHERE razao_max IS NULL;

INSERT INTO operadoras (cnpj, razao_social)
SELECT
  cnpj_clean,
  coalesce(razao_max, 'RAZAO SOCIAL NAO INFORMADA')
FROM tmp_cand_operadoras
ON CONFLICT (cnpj) DO NOTHING;

INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT 'despesas_consolidadas', 'ano/trimestre/valor invalido ou negativo', NULL, raw
FROM tmp_cons_clean
WHERE (cnpj_clean ~ '^[0-9]{14}$' AND cnpj_clean <> '00000000000000')
  AND (ano_int IS NULL OR trimestre_int IS NULL OR valor_num IS NULL OR valor_num < 0);

WITH dup AS (
  SELECT cnpj_clean, ano_int, trimestre_int,
         count(*) AS qtd_linhas,
         count(DISTINCT valor_num) AS qtd_valores_distintos
  FROM tmp_cons_clean
  WHERE cnpj_clean ~ '^[0-9]{14}$'
    AND cnpj_clean <> '00000000000000'
    AND ano_int IS NOT NULL
    AND trimestre_int IS NOT NULL
    AND valor_num IS NOT NULL
    AND valor_num >= 0
  GROUP BY cnpj_clean, ano_int, trimestre_int
  HAVING count(*) > 1
)
INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT
  'despesas_consolidadas',
  'duplicata por (cnpj,ano,trimestre)',
  'qtd_linhas=' || qtd_linhas || ', qtd_valores_distintos=' || qtd_valores_distintos,
  jsonb_build_object('cnpj', cnpj_clean, 'ano', ano_int, 'trimestre', trimestre_int)
FROM dup;

INSERT INTO despesas_consolidadas (cnpj, ano, trimestre, valor_despesas)
SELECT
  t.cnpj_clean,
  t.ano_int,
  t.trimestre_int,
  CASE
    WHEN count(DISTINCT t.valor_num) = 1 THEN max(t.valor_num)
    ELSE sum(t.valor_num)
  END AS valor_final
FROM tmp_cons_clean t
JOIN operadoras o ON o.cnpj = t.cnpj_clean::char(14)
WHERE t.cnpj_clean ~ '^[0-9]{14}$'
  AND t.cnpj_clean <> '00000000000000'
  AND t.ano_int IS NOT NULL
  AND t.trimestre_int IS NOT NULL
  AND t.valor_num IS NOT NULL
  AND t.valor_num >= 0
GROUP BY t.cnpj_clean, t.ano_int, t.trimestre_int
ON CONFLICT (cnpj, ano, trimestre) DO UPDATE
SET valor_despesas = EXCLUDED.valor_despesas;


-- agregadas -> despesas_agregadas
DROP TABLE IF EXISTS tmp_aggr_clean;
CREATE TEMP TABLE tmp_aggr_clean AS
SELECT
    NULLIF(trim(razao_social), '') AS razao_clean,
    NULLIF(upper(trim(uf)), '') AS uf_clean,
    CASE
        WHEN replace(regexp_replace(coalesce(total_despesas,''), '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN round(replace(regexp_replace(total_despesas, '[^0-9,.-]', '', 'g'), ',', '.')::numeric, 2)
        ELSE NULL
    END AS total_num,
    CASE
        WHEN replace(regexp_replace(coalesce(media_por_trimestre,''), '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN round(replace(regexp_replace(media_por_trimestre, '[^0-9,.-]', '', 'g'), ',', '.')::numeric, 2)
        ELSE NULL
    END AS media_num,
    CASE
        WHEN replace(regexp_replace(coalesce(desvio_padrao,''), '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN round(replace(regexp_replace(desvio_padrao, '[^0-9,.-]', '', 'g'), ',', '.')::numeric, 2)
        ELSE NULL
    END AS desvio_num,
    CASE WHEN trim(qtd_registros) ~ '^[0-9]+$' THEN trim(qtd_registros)::int ELSE NULL END AS qtd_reg_int,
    CASE WHEN trim(qtd_trimestres) ~ '^[0-9]+$' THEN trim(qtd_trimestres)::int ELSE NULL END AS qtd_tri_int,
    row_to_json(t)::jsonb AS raw
FROM stg_agregadas_raw t;

INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT 'despesas_agregadas', 'razao_social/uf/total invalidos ou negativos', NULL, raw
FROM tmp_aggr_clean
WHERE razao_clean IS NULL OR uf_clean IS NULL OR total_num IS NULL OR total_num < 0;

WITH dup AS (
  SELECT razao_clean, uf_clean, count(*) AS qtd_linhas, count(DISTINCT total_num) AS qtd_valores_distintos
  FROM tmp_aggr_clean
  WHERE razao_clean IS NOT NULL AND uf_clean IS NOT NULL AND total_num IS NOT NULL AND total_num >= 0
  GROUP BY razao_clean, uf_clean
  HAVING count(*) > 1
)
INSERT INTO import_rejeicoes (tabela_alvo, motivo, detalhe, linha_raw)
SELECT
  'despesas_agregadas',
  'duplicata por (razao_social,uf)',
  'qtd_linhas=' || qtd_linhas || ', qtd_valores_distintos=' || qtd_valores_distintos,
  jsonb_build_object('razao_social', razao_clean, 'uf', uf_clean)
FROM dup;

INSERT INTO despesas_agregadas (razao_social, uf, total_despesas, media_por_trimestre, desvio_padrao, qtd_registros, qtd_trimestres)
SELECT
  razao_clean,
  uf_clean::char(2),
  CASE
    WHEN count(DISTINCT total_num) = 1 THEN max(total_num)
    ELSE sum(total_num)
  END AS total_final,
  max(media_num),
  max(desvio_num),
  max(qtd_reg_int),
  max(qtd_tri_int)
FROM tmp_aggr_clean
WHERE razao_clean IS NOT NULL
  AND uf_clean IS NOT NULL
  AND total_num IS NOT NULL
  AND total_num >= 0
GROUP BY razao_clean, uf_clean
ON CONFLICT (razao_social, uf) DO UPDATE
SET
  total_despesas = EXCLUDED.total_despesas,
  media_por_trimestre = EXCLUDED.media_por_trimestre,
  desvio_padrao = EXCLUDED.desvio_padrao,
  qtd_registros = EXCLUDED.qtd_registros,
  qtd_trimestres = EXCLUDED.qtd_trimestres;
"""


def conectar_banco() -> PGConnection:
    if not DB_PASSWORD:
        raise RuntimeError("Defina a senha do banco via env: set PGPASSWORD=...")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def executar_sql(conn: PGConnection, sql: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def copiar_csv(conn: PGConnection, tabela: str, colunas: str, arquivo_utf8: Path, delimitador: str) -> None:
    comando = (
        f"COPY {tabela} ({colunas}) FROM STDIN "
        f"WITH (FORMAT csv, HEADER true, DELIMITER '{delimitador}', ENCODING 'UTF8')"
    )
    try:
        with conn.cursor() as cur:
            with arquivo_utf8.open("r", encoding="utf-8", newline="") as f:
                cur.copy_expert(comando, f)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def normalizar_csvs() -> CsvUtf8:
    return CsvUtf8(
        enriquecido=gerar_copia_utf8(CSV_FILES["enriquecido"]),
        consolidado=gerar_copia_utf8(CSV_FILES["consolidado"]),
        agregadas=gerar_copia_utf8(CSV_FILES["agregadas"]),
    )


def mostrar_resumo(conn: PGConnection) -> None:
    q = """
    SELECT 'operadoras' AS tabela, count(*) AS registros FROM operadoras
    UNION ALL
    SELECT 'despesas_consolidadas', count(*) FROM despesas_consolidadas
    UNION ALL
    SELECT 'despesas_agregadas', count(*) FROM despesas_agregadas
    UNION ALL
    SELECT 'import_rejeicoes', count(*) FROM import_rejeicoes
    ORDER BY tabela;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()

    print("\nRESUMO:")
    for tabela, n in rows:
        print(f"  - {tabela:22s}: {n}")


def main() -> int:
    try:
        for nome, caminho in CSV_FILES.items():
            if not caminho.exists():
                raise FileNotFoundError(f"{nome}: {caminho}")

        arquivos = normalizar_csvs()
        delim_enriq = detectar_delimitador(arquivos.enriquecido)
        delim_cons = detectar_delimitador(arquivos.consolidado)
        delim_aggr = detectar_delimitador(arquivos.agregadas)

        conn = conectar_banco()
        try:
            executar_sql(conn, DDL_SQL)
            executar_sql(conn, STAGING_SQL)

            copiar_csv(conn, "stg_enriquecido_raw",
                       "cnpj, razao_social, trimestre, ano, valor_despesas, registro_ans, modalidade, uf",
                       arquivos.enriquecido, delim_enriq)

            copiar_csv(conn, "stg_consolidado_raw",
                       "cnpj, razao_social, trimestre, ano, valor_despesas",
                       arquivos.consolidado, delim_cons)

            copiar_csv(conn, "stg_agregadas_raw",
                       "razao_social, uf, total_despesas, media_por_trimestre, desvio_padrao, qtd_registros, qtd_trimestres",
                       arquivos.agregadas, delim_aggr)

            executar_sql(conn, TRANSFORM_SQL)
            mostrar_resumo(conn)
            return 0
        finally:
            conn.close()

    except FileNotFoundError as e:
        print("Arquivo não encontrado:", e)
        return 1
    except psycopg2.Error as e:
        print("Erro de banco:", e)
        return 1
    except Exception as e:
        print("Erro inesperado:", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
