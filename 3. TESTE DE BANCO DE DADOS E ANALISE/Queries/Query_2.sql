-- Query 2

WITH total_por_operadora AS (
  SELECT
    o.uf,
    d.cnpj,
    SUM(d.valor_despesas) AS total_operadora_uf
  FROM despesas_consolidadas d
  JOIN operadoras o ON o.cnpj = d.cnpj
  WHERE o.uf IS NOT NULL AND o.uf <> ''
  GROUP BY o.uf, d.cnpj
),
por_uf AS (
  SELECT
    uf,
    SUM(total_operadora_uf) AS total_despesas_uf,
    AVG(total_operadora_uf) AS media_por_operadora_uf,
    COUNT(*) AS qtd_operadoras_uf
  FROM total_por_operadora
  GROUP BY uf
)
SELECT
  uf,
  ROUND(total_despesas_uf, 2) AS total_despesas_uf,
  ROUND(media_por_operadora_uf, 2) AS media_por_operadora_uf,
  qtd_operadoras_uf
FROM por_uf
ORDER BY total_despesas_uf DESC
LIMIT 5;