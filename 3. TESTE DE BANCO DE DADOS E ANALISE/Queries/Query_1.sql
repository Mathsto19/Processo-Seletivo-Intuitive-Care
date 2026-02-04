-- Query 1

WITH limites AS (
  SELECT
    MIN(ano * 10 + trimestre) AS periodo_min,
    MAX(ano * 10 + trimestre) AS periodo_max
  FROM despesas_consolidadas
),
base AS (
  SELECT
    d.cnpj,
    MAX(d.valor_despesas) FILTER (
      WHERE (d.ano * 10 + d.trimestre) = (SELECT periodo_min FROM limites)
    ) AS valor_primeiro,
    MAX(d.valor_despesas) FILTER (
      WHERE (d.ano * 10 + d.trimestre) = (SELECT periodo_max FROM limites)
    ) AS valor_ultimo
  FROM despesas_consolidadas d
  GROUP BY d.cnpj
)
SELECT
  o.cnpj,
  o.razao_social,
  b.valor_primeiro,
  b.valor_ultimo,
  ROUND(((b.valor_ultimo - b.valor_primeiro) / b.valor_primeiro) * 100.0, 2) AS crescimento_percentual
FROM base b
JOIN operadoras o ON o.cnpj = b.cnpj
WHERE b.valor_primeiro IS NOT NULL
  AND b.valor_ultimo  IS NOT NULL
  AND b.valor_primeiro > 0
ORDER BY crescimento_percentual DESC
LIMIT 5;