# 🌦️ Weather ETL com Apache Airflow 3

Pipeline de Engenharia de Dados que coleta, transforma e carrega dados climáticos horários de **8 capitais brasileiras** em um Data Warehouse PostgreSQL, orquestrado com **Apache Airflow 3**.

![Graph view da DAG weather_etl](docs/images/01_graph_weather_etl.png)

## 🏗️ Arquitetura

```
                 ┌──────────────── weather_etl (@daily, catchup) ────────────────┐
 Open-Meteo API  │ create_tables                                                  │
 (8 capitais) ──►│ target_date ─► extract[8] ─► transform[8] ─► load ─► quality_check ──► Asset: weather_hourly
                 └────────────────────────────────────────────────────────────────┘                │
                                                                                                    ▼ (data-aware scheduling)
                                                     weather_daily_summary: build_daily_table ─► hottest_cities_report
                                                                                     │
                                                                                     ▼
                                                                    PostgreSQL: weather_hourly / weather_daily
```

| DAG | Agendamento | O que faz |
|-----|-------------|-----------|
| `weather_etl` | `@daily` com backfill (catchup) | Extrai dados horários da API Open-Meteo, normaliza, valida e faz upsert no PostgreSQL |
| `weather_daily_summary` | Disparada pelo **Asset** `weather_hourly` | Agrega mín/máx/média de temperatura, umidade e chuva por cidade/dia e gera ranking |

## ✨ Conceitos de Airflow 3 aplicados

- **TaskFlow API** (`@dag`, `@task`) com o novo `airflow.sdk`
- **Dynamic Task Mapping**: `extract.partial(...).expand(city=CITIES)` cria uma task por cidade em tempo de execução
- **Assets (data-aware scheduling)**: a DAG de agregação roda automaticamente quando novos dados são publicados
- **Backfill/catchup** com processamento por data lógica (cada execução processa o dia anterior completo)
- **Idempotência**: `INSERT ... ON CONFLICT DO UPDATE`, então reprocessar um dia nunca duplica dados
- **Data Quality Check**: a execução falha se a cobertura (cidades × 24h) ficar abaixo de 95%, e o Asset não é publicado
- **Resiliência**: `retries` com *exponential backoff* e limite de concorrência por task (`max_active_tis_per_dag`) para respeitar a API
- **Connections via variável de ambiente** (`AIRFLOW_CONN_WAREHOUSE_DB`), sem credenciais no código das DAGs
- **PostgresHook** (provider oficial) para escrita e leitura no warehouse

## 📸 Execução real

**Histórico de execuções: 10 dias processados via backfill, 0 falhas**
![Grid view](docs/images/02_grid_runs.png)

**DAG disparada por Asset**
![Asset-triggered DAG](docs/images/03_graph_asset_trigger.png)

**Data Quality Check: 8 cidades, 192/192 registros**
![Quality check log](docs/images/05_log_quality_check.png)

**Resultado: ranking das capitais por temperatura média**
![Ranking log](docs/images/04_log_ranking.png)

## 🚀 Como executar

Pré-requisito: Docker Desktop.

```bash
docker compose up -d
```

- Airflow UI: http://localhost:8081 (ambiente local, sem login)
- PostgreSQL: `localhost:5433` (usuário/senha `airflow`, banco `warehouse`)

As DAGs começam ativas e o backfill dos últimos dias roda automaticamente. Para consultar os dados:

```bash
docker compose exec postgres psql -U airflow -d warehouse -c "SELECT * FROM weather_daily ORDER BY day DESC, temp_avg_c DESC LIMIT 10;"
```

Para encerrar: `docker compose down -v`

## 📁 Estrutura

```
├── dags/
│   ├── weather_etl.py             # ETL principal (extract → transform → load → quality)
│   └── weather_daily_summary.py   # Agregação disparada por Asset
├── include/
│   └── cities.py                  # Configuração das capitais monitoradas
├── sql/init/                      # Criação do banco warehouse
├── docs/images/                   # Prints da execução
└── docker-compose.yaml            # Airflow 3.1 + PostgreSQL 16
```

## 🛠️ Stack

Apache Airflow 3.1 · Python 3.12 · PostgreSQL 16 · Docker Compose · Open-Meteo API

> ⚠️ O `docker-compose.yaml` é configurado para **desenvolvimento local** (modo standalone, credenciais padrão, sem autenticação na UI). Não use essa configuração em produção.

Dados meteorológicos: [Open-Meteo](https://open-meteo.com/) (CC BY 4.0).
