-- Query 3

WITH ultimos_3_periodos AS (
  SELECT ano, trimestre
  FROM (
    SELECT DISTINCT ano, trimestre
    FROM despesas_consolidadas
  ) p
  ORDER BY ano DESC, trimestre DESC
  LIMIT 3
),
media_geral_por_periodo AS (
  SELECT
    d.ano,
    d.trimestre,
    AVG(d.valor_despesas) AS media_geral
  FROM despesas_consolidadas d
  JOIN ultimos_3_periodos u
    ON u.ano = d.ano AND u.trimestre = d.trimestre
  GROUP BY d.ano, d.trimestre
),
comparacoes AS (
  SELECT
    d.cnpj,
    CASE WHEN d.valor_despesas > m.media_geral THEN 1 ELSE 0 END AS acima_media
  FROM despesas_consolidadas d
  JOIN media_geral_por_periodo m
    ON m.ano = d.ano AND m.trimestre = d.trimestre
),
contagem AS (
  SELECT
    cnpj,
    SUM(acima_media) AS qtd_trimestres_acima_media
  FROM comparacoes
  GROUP BY cnpj
)
SELECT
  COUNT(*) AS qtd_operadoras_acima_media_em_2_ou_mais_trimestres
FROM contagem
WHERE qtd_trimestres_acima_media >= 2;