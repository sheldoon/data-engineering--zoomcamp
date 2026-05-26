# Módulo 3 — Data Warehouse com BigQuery

## Visão geral

Neste módulo o objetivo foi entender como estruturar um data warehouse na nuvem
usando o **BigQuery** (GCP), cobrindo desde a criação de tabelas externas até
técnicas de otimização com particionamento e clustering.

Os arquivos Parquet foram carregados no **Google Cloud Storage** via script
Python, e todas as queries foram executadas diretamente pelo editor do BigQuery.

---

## Conteúdo

### Tabela externa vs tabela nativa

O módulo começa com a diferença fundamental entre os dois tipos de tabela
no BigQuery:

| | Tabela Externa | Tabela Nativa |
|---|---|---|
| Armazenamento | Arquivo no GCS | Dentro do BigQuery |
| Performance | Mais lenta | Mais rápida |
| Metadados internos | Não | Sim |
| Suporte a particionamento | Não | Sim |

A tabela externa aponta para os arquivos Parquet no GCS usando wildcard,
lendo todos os arquivos que seguem o padrão `yellow_tripdata_*.parquet`:

```sql
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.homework.yellow_taxi_external`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://YOUR_BUCKET_NAME/yellow_tripdata_*.parquet']
);
```

---

### BigQuery como banco colunar — impacto no custo

O BigQuery cobra pelo volume de dados escaneados por query. Por ser um banco
**colunar**, selecionar menos colunas reduz diretamente o custo.

Evidência prática medida neste módulo:

| Query | Dados processados |
|---|---|
| `SELECT PULocationID` | 155,12 MB |
| `SELECT PULocationID, DOLocationID` | 310,24 MB |

Adicionar uma segunda coluna de tamanho equivalente **dobrou exatamente** o
volume processado — confirmando na prática o comportamento colunar do BigQuery.
Isso reforça a importância de evitar `SELECT *` em produção.

---

### Particionamento e clustering

**Particionamento** divide os dados em blocos por data, permitindo que o
BigQuery leia apenas as partições relevantes para o filtro da query.

**Clustering** organiza os dados dentro de cada partição por uma coluna
específica, reduzindo ainda mais os blocos lidos em queries com filtros
combinados.

```sql
-- Tabela particionada por data de desembarque
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
AS SELECT * FROM `YOUR_PROJECT_ID.homework.yellow_taxi`;

-- Tabela particionada + clusterizada por VendorID
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned_clustered`
CLUSTER BY VendorID
AS SELECT * FROM `YOUR_PROJECT_ID.homework.yellow_taxi_partitioned`;
```

**Impacto medido do particionamento:**

| Query | Tabela comum | Tabela particionada | Redução |
|---|---|---|---|
| `WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'` | 310,24 MB | 26,84 MB | **91,3%** |

A tabela particionada processou **91,3% menos dados** para a mesma query —
uma redução de custo e tempo diretamente proporcional em ambiente de produção.

---

## Aprendizados principais

- Diferença prática entre tabela externa e tabela nativa no BigQuery
- Como o modelo colunar do BigQuery impacta custo e performance
- Como particionamento e clustering reduzem drasticamente o volume de dados
  processados em queries com filtros de data e colunas específicas
- Boas práticas de otimização de queries em data warehouses na nuvem

---

## Arquivos

| Arquivo | Descrição |
|---|---|
| `homework.sql` | Queries completas do módulo com comentários |
