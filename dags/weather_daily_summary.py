"""
Agregação diária disparada por dados (data-aware scheduling com Assets do Airflow 3).

Sempre que a DAG `weather_etl` publica o Asset `weather_hourly`, esta DAG recalcula
a tabela `weather_daily` (mín/máx/média de temperatura, chuva acumulada, umidade média)
e gera um ranking das capitais mais quentes.
"""

from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task

WAREHOUSE_CONN_ID = "warehouse_db"
WEATHER_HOURLY = Asset("weather_hourly")
WEATHER_DAILY = Asset("weather_daily")


@dag(
    dag_id="weather_daily_summary",
    description="Agregação diária disparada pelo Asset weather_hourly",
    schedule=[WEATHER_HOURLY],
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["analytics", "weather", "assets"],
    doc_md=__doc__,
)
def weather_daily_summary():
    @task(outlets=[WEATHER_DAILY])
    def build_daily_table() -> int:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS weather_daily (
                city             TEXT    NOT NULL,
                state            CHAR(2) NOT NULL,
                day              DATE    NOT NULL,
                temp_min_c       NUMERIC(5,2),
                temp_max_c       NUMERIC(5,2),
                temp_avg_c       NUMERIC(5,2),
                humidity_avg_pct NUMERIC(5,2),
                precipitation_mm NUMERIC(7,2),
                PRIMARY KEY (city, day)
            );

            INSERT INTO weather_daily
            SELECT city, state, measured_at::date,
                   MIN(temperature_c), MAX(temperature_c), ROUND(AVG(temperature_c), 2),
                   ROUND(AVG(humidity_pct), 2), SUM(precipitation_mm)
            FROM weather_hourly
            GROUP BY city, state, measured_at::date
            ON CONFLICT (city, day) DO UPDATE SET
                temp_min_c       = EXCLUDED.temp_min_c,
                temp_max_c       = EXCLUDED.temp_max_c,
                temp_avg_c       = EXCLUDED.temp_avg_c,
                humidity_avg_pct = EXCLUDED.humidity_avg_pct,
                precipitation_mm = EXCLUDED.precipitation_mm;
            """
        )
        total = hook.get_first("SELECT COUNT(*) FROM weather_daily")[0]
        print(f"[daily] weather_daily possui {total} linhas (cidade x dia)")
        return total

    @task
    def hottest_cities_report(_: int) -> None:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        rows = hook.get_records(
            """
            SELECT city, state, ROUND(AVG(temp_avg_c), 1), MAX(temp_max_c), SUM(precipitation_mm)
            FROM weather_daily
            GROUP BY city, state
            ORDER BY 3 DESC
            """
        )
        print("Ranking de capitais por temperatura média")
        print(f"{'#':<3}{'Cidade':<18}{'UF':<4}{'Média °C':>10}{'Máx °C':>9}{'Chuva mm':>10}")
        for i, (city, uf, avg, tmax, rain) in enumerate(rows, start=1):
            print(f"{i:<3}{city:<18}{uf:<4}{avg:>10}{tmax:>9}{rain:>10}")

    hottest_cities_report(build_daily_table())


weather_daily_summary()
