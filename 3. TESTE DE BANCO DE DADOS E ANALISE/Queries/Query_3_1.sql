-- Query 3.1 

WITH media_geral AS (
  SELECT avg(valor_despesas) AS media
  FROM despesas_consolidadas
),
operadoras_acima_media AS (
  SELECT 
    dc.cnpj,
    o.razao_social,
    dc.ano,
    dc.trimestre,
    dc.valor_despesas,
    mg.media AS media_geral,
    CASE WHEN dc.valor_despesas > mg.media THEN 1 ELSE 0 END AS acima_media
  FROM despesas_consolidadas dc
  JOIN operadoras o ON o.cnpj = dc.cnpj
  CROSS JOIN media_geral mg
),
contagem_trimestres AS (
  SELECT 
    cnpj,
    razao_social,
    sum(acima_media) AS qtd_trimestres_acima_media,
    count(*) AS total_trimestres
  FROM operadoras_acima_media
  GROUP BY cnpj, razao_social
)
SELECT 
  cnpj,
  razao_social,
  qtd_trimestres_acima_media,
  total_trimestres
FROM contagem_trimestres
WHERE qtd_trimestres_acima_media >= 2
ORDER BY qtd_trimestres_acima_media DESC, razao_social;
