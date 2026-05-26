# Módulo 1 — Containerização com Docker

## Visão geral

Neste módulo o objetivo foi entender como containerizar aplicações e serviços
usando Docker, e construir um pipeline simples de ingestão de dados com Python.

O ambiente foi configurado usando **GitHub Codespaces**, o que permitiu rodar
Docker em ambiente Linux diretamente na nuvem, sem necessidade de configuração
local.

---

## Conteúdo

### Docker e Docker Compose

- Criação e execução de containers isolados
- Configuração de serviços com `docker-compose.yml`
- Subida de container Postgres e PgAdmin para armazenamento e visualização dos dados

### Ingestão de dados com Python

Pipeline de ingestão do dataset **NYC Yellow Taxi** para um banco de dados
Postgres rodando em container Docker.

**Stack utilizada:**
- `Pandas` — leitura e manipulação do dataset
- `SQLAlchemy` — conexão e escrita no Postgres

**Decisão técnica — ingestão em chunks:**

O dataset de táxi de NYC é grande o suficiente para causar problemas de memória
se carregado inteiramente na memória de uma vez. Por isso o script foi
implementado com ingestão em chunks de **100.000 registros por vez**, usando
o parâmetro `chunksize` do Pandas:

```python
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.to_sql(name=table_name, con=engine, if_exists='replace')
            first = False
        else:
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
```

Essa abordagem é uma boa prática em pipelines reais, onde o volume de dados
frequentemente ultrapassa a memória disponível.

---

## Aprendizados principais

- Como Docker resolve o problema de "funciona na minha máquina" ao isolar
  dependências em containers reproduzíveis
- Como orquestrar múltiplos serviços (Postgres + PgAdmin) com Docker Compose
- Como construir um pipeline de ingestão eficiente em memória usando chunks
- Como conectar Python a um banco de dados relacional com SQLAlchemy

---

## Como executar

> **Pré-requisitos:** Docker e Docker Compose instalados, ou GitHub Codespaces.

```bash
# Sobe os containers
docker-compose up -d

# Executa o script de ingestão
python module-01-containerization/ingest_data.py
```

> Substitua as variáveis de conexão no script pelos valores do seu ambiente
> (host, porta, usuário, senha e nome do banco).
