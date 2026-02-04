-- Tabela OPERADORAS 
SELECT 
    cnpj,
    razao_social,
    registro_ans,
    modalidade,
    uf,
    created_at
FROM operadoras
ORDER BY razao_social;


-- Tabela DESPESAS_CONSOLIDADAS 
SELECT 
    dc.cnpj,
    o.razao_social,
    dc.ano,
    dc.trimestre,
    dc.valor_despesas
FROM despesas_consolidadas dc
JOIN operadoras o ON o.cnpj = dc.cnpj
ORDER BY dc.ano, dc.trimestre, o.razao_social;


-- Tabela DESPESAS_AGREGADAS 
SELECT 
    razao_social,
    uf,
    total_despesas,
    media_por_trimestre,
    desvio_padrao,
    qtd_registros,
    qtd_trimestres
FROM despesas_agregadas
ORDER BY razao_social, uf;


-- Tabela IMPORT_REJEICOES 
SELECT 
    id,
    tabela_alvo,
    motivo,
    detalhe,
    linha_raw,
    criado_em
FROM import_rejeicoes
ORDER BY id;
