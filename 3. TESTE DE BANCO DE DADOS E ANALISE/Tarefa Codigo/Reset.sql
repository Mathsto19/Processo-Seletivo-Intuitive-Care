-- Desabilita avisos de tabelas inexistentes temporariamente
SET client_min_messages TO WARNING;

-- 1. Apaga tabelas FINAIS (com CASCADE para remover dependencias)
DROP TABLE IF EXISTS despesas_consolidadas CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
DROP TABLE IF EXISTS operadoras CASCADE;

-- 2. Apaga tabelas STAGING (temporarias do processo ETL)
DROP TABLE IF EXISTS stg_enriquecido_raw CASCADE;
DROP TABLE IF EXISTS stg_consolidado_raw CASCADE;
DROP TABLE IF EXISTS stg_agregadas_raw CASCADE;

-- 3. Apaga tabela de LOG/AUDITORIA
DROP TABLE IF EXISTS import_rejeicoes CASCADE;

-- 4. Apaga tabelas TEMPORARIAS (caso existam)
DROP TABLE IF EXISTS tmp_enriq_clean CASCADE;
DROP TABLE IF EXISTS tmp_cons_clean CASCADE;
DROP TABLE IF EXISTS tmp_aggr_clean CASCADE;
DROP TABLE IF EXISTS tmp_cand_operadoras CASCADE;

-- Reativa nivel normal de mensagens
SET client_min_messages TO NOTICE;

-- =====================================================
-- VERIFICACAO: Lista todas as tabelas restantes no schema public
-- =====================================================
SELECT 
    schemaname AS schema,
    tablename AS tabela,
    tableowner AS dono
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;
