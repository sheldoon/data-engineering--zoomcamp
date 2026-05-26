-- ================================================
-- MÓDULO 3: DATA WAREHOUSE - BIGQUERY
-- Dataset: NYC Yellow Taxi Data (2024)
-- Fonte: Google Cloud Storage (formato Parquet)
-- ================================================


-- ================================================
-- 1. CRIAÇÃO DO DATASET
-- ================================================

-- Cria o dataset (schema) que vai conter todas as tabelas deste módulo
-- No BigQuery, um dataset é o equivalente a um schema/database no SQL tradicional
CREATE SCHEMA `YOUR_PROJECT_ID.homework`;


-- ================================================
-- 2. CRIAÇÃO DAS TABELAS
-- ================================================

-- Cria tabela externa apontando para os arquivos Parquet no GCS
-- Tabelas externas não armazenam dados no BigQuery — elas leem direto
-- do arquivo fonte (neste caso, o bucket GCS) a cada consulta
-- O wildcard (*) no URI faz com que o BigQuery leia todos os arquivos
-- que seguem o padrão yellow_tripdata_*.parquet no bucket
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.homework.yellow_taxi_external`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://YOUR_BUCKET_NAME/yellow_tripdata_*.parquet']
);

-- Cria tabela regular (nativa) a partir da tabela externa
-- Diferente da tabela externa, esta armazena os dados fisicamente no BigQuery
-- Vantagens: queries mais rápidas, suporte a particionamento/clustering,
-- e o BigQuery consegue usar metadados internos para otimizar consultas
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.homework.yellow_taxi`
AS
SELECT * FROM `YOUR_PROJECT_ID.homework.yellow_taxi_external`;


-- ================================================
-- 3. EXPLORAÇÃO INICIAL
-- ================================================

-- Conta o total de registros da tabela regular
-- COUNT(1) usa metadados internos do BigQuery — não escaneia dados reais,
-- por isso é instantâneo e não tem custo de processamento
SELECT COUNT(1) 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi`;

-- Conta registros pela coluna PULocationID na tabela regular
-- Diferente de COUNT(1), COUNT(coluna) precisa verificar valores nulos,
-- então o BigQuery escaneia a coluna — tem custo de processamento
SELECT COUNT(PULocationID) 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi`;

-- Mesma contagem na tabela externa
-- Como tabelas externas não têm metadados internos, o BigQuery precisa
-- ler o arquivo Parquet no GCS para retornar o resultado
-- Isso torna esta query mais lenta e cara que a mesma query na tabela regular
SELECT COUNT(PULocationID) 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi_external`;


-- ================================================
-- 4. COMPARAÇÃO DE CUSTO DE QUERIES
-- ================================================

-- Escaneia apenas a coluna PULocationID
-- O BigQuery é um banco colunar: você paga pelo volume de dados escaneados,
-- então selecionar menos colunas = menor custo
SELECT PULocationID 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi`;

-- Escaneia duas colunas: PULocationID e DOLocationID
-- O custo aumenta proporcionalmente ao tamanho das colunas adicionais
-- Evite SELECT * em produção por esse motivo
SELECT PULocationID, DOLocationID 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi`;


-- ================================================
-- 5. ANÁLISE DE QUALIDADE DE DADOS
-- ================================================

-- Identifica registros com tarifa igual a zero
-- Útil para detectar anomalias: corridas canceladas, erros de registro
-- ou viagens gratuitas que podem distorcer análises de receita
SELECT COUNT(fare_amount) 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi`
WHERE fare_amount = 0;


-- ================================================
-- 6. OTIMIZAÇÃO COM PARTICIONAMENTO E CLUSTERING
-- ================================================

-- Cria tabela particionada por data de desembarque
-- Particionamento divide os dados em blocos por data
-- Queries com filtro de data só escaneiam as partições relevantes,
-- reduzindo drasticamente o volume de dados processados e o custo
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
AS
SELECT * FROM `YOUR_PROJECT_ID.homework.yellow_taxi`;

-- Cria tabela particionada por data e clusterizada por VendorID
-- Clustering organiza os dados dentro de cada partição pela coluna VendorID
-- Queries que combinam filtro de data E VendorID se tornam ainda mais eficientes,
-- pois o BigQuery lê menos blocos dentro de cada partição
-- Regra geral: particione pela coluna de data mais usada em filtros,
-- clusterize pelas colunas de alta cardinalidade usadas em WHERE ou GROUP BY
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned_clustered`
CLUSTER BY VendorID
AS
SELECT * FROM `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned`;


-- ================================================
-- 7. COMPARAÇÃO DE PERFORMANCE: TABELA COMUM VS PARTICIONADA
-- ================================================

-- Query na tabela comum: escaneia TODOS os dados independente do filtro de data
-- O BigQuery não tem como pular registros fora do intervalo sem particionamento
SELECT VendorID 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Mesma query na tabela particionada: escaneia APENAS as partições de março/2024
-- Você consegue ver a diferença de MB processados no canto superior direito
-- do editor do BigQuery antes de executar a query (estimativa de custo)
SELECT VendorID 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


-- ================================================
-- 8. VALIDAÇÃO DA TABELA FINAL
-- ================================================

-- Confirma o total de registros na tabela particionada e clusterizada
-- Útil para validar que nenhum dado foi perdido durante a criação das tabelas otimizadas
SELECT COUNT(*) 
FROM `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned_clustered`;