"""
Pipeline ETL de dados climáticos das capitais brasileiras.

Extract   -> API pública Open-Meteo (uma task por cidade, via Dynamic Task Mapping)
Transform -> normalização e validação dos registros horários
Load      -> upsert idempotente no PostgreSQL (warehouse.weather_hourly)
Quality   -> checagens de qualidade de dados antes de publicar o Asset

Ao final, o Asset `weather_hourly` é atualizado e dispara a DAG de agregação diária.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, dag, task

from include.cities import CITIES

WAREHOUSE_CONN_ID = "warehouse_db"
WEATHER_HOURLY = Asset("weather_hourly")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="weather_etl",
    description="ETL diário de clima das capitais brasileiras (Open-Meteo -> PostgreSQL)",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 8, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    tags=["etl", "weather", "postgres"],
    doc_md=__doc__,
)
def weather_etl():
    @task
    def create_tables() -> None:
        """Cria a tabela de destino caso ainda não exista."""
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS weather_hourly (
                city            TEXT        NOT NULL,
                state           CHAR(2)     NOT NULL,
                measured_at     TIMESTAMP   NOT NULL,
                temperature_c   NUMERIC(5,2),
                humidity_pct    NUMERIC(5,2),
                precipitation_mm NUMERIC(6,2),
                wind_speed_kmh  NUMERIC(6,2),
                loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),
                PRIMARY KEY (city, measured_at)
            );
            """
        )

    @task
    def target_date(logical_date=None) -> str:
        """Processa sempre o dia anterior à execução (dia completo)."""
        return logical_date.subtract(days=1).to_date_string()

    @task(max_active_tis_per_dag=4)
    def extract(city: dict, day: str) -> dict:
        """Busca os dados horários de uma cidade na API Open-Meteo."""
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "hourly": ",".join(HOURLY_VARS),
            "start_date": day,
            "end_date": day,
            "timezone": "America/Sao_Paulo",
        }
        response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        print(f"[extract] {city['name']}: {len(payload['hourly']['time'])} registros para {day}")
        return {"city": city, "hourly": payload["hourly"]}

    @task
    def transform(raw: dict) -> list[dict]:
        """Converte o formato colunar da API em linhas e descarta registros inválidos."""
        city, hourly = raw["city"], raw["hourly"]
        rows = []
        for i, ts in enumerate(hourly["time"]):
            temp = hourly["temperature_2m"][i]
            if temp is None or not -10 <= temp <= 50:
                continue  # descarta leituras ausentes ou fisicamente implausíveis
            rows.append(
                {
                    "city": city["name"],
                    "state": city["state"],
                    "measured_at": ts,
                    "temperature_c": temp,
                    "humidity_pct": hourly["relative_humidity_2m"][i],
                    "precipitation_mm": hourly["precipitation"][i] or 0.0,
                    "wind_speed_kmh": hourly["wind_speed_10m"][i],
                }
            )
        print(f"[transform] {city['name']}: {len(rows)} linhas válidas")
        return rows

    @task
    def load(batches: list[list[dict]]) -> int:
        """Upsert idempotente: reprocessar um dia nunca duplica dados."""
        rows = [row for batch in batches for row in batch]
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        sql = """
            INSERT INTO weather_hourly
                (city, state, measured_at, temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh)
            VALUES (%(city)s, %(state)s, %(measured_at)s, %(temperature_c)s,
                    %(humidity_pct)s, %(precipitation_mm)s, %(wind_speed_kmh)s)
            ON CONFLICT (city, measured_at) DO UPDATE SET
                temperature_c    = EXCLUDED.temperature_c,
                humidity_pct     = EXCLUDED.humidity_pct,
                precipitation_mm = EXCLUDED.precipitation_mm,
                wind_speed_kmh   = EXCLUDED.wind_speed_kmh,
                loaded_at        = NOW();
        """
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(sql, rows)
            conn.commit()
        print(f"[load] {len(rows)} linhas gravadas em weather_hourly")
        return len(rows)

    @task(outlets=[WEATHER_HOURLY])
    def quality_check(day: str, loaded: int) -> None:
        """Garante cobertura completa (todas as cidades x 24h) antes de publicar o Asset."""
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        cities, rows = hook.get_first(
            """
            SELECT COUNT(DISTINCT city), COUNT(*)
            FROM weather_hourly
            WHERE measured_at::date = %s
            """,
            parameters=(day,),
        )
        expected = len(CITIES) * 24
        print(f"[quality] {day}: {cities} cidades, {rows}/{expected} registros (load={loaded})")
        if cities != len(CITIES) or rows < expected * 0.95:
            raise ValueError(f"Falha de qualidade em {day}: {rows}/{expected} registros")

    day = target_date()
    raw = extract.partial(day=day).expand(city=CITIES)
    clean = transform.expand(raw=raw)
    loaded = load(clean)
    create_tables() >> raw
    quality_check(day, loaded)


weather_etl()
