"""
Coleta diária executada pelo GitHub Actions.

Busca os dados horários do dia anterior na API Open-Meteo para as capitais
monitoradas, agrega por cidade e mantém um dataset histórico versionado no
repositório (data/weather_daily.csv) e um relatório em Markdown (data/REPORT.md).

Mesma lógica de negócio da DAG `weather_daily_summary`, rodando sem infraestrutura.
Uso: python scripts/daily_report.py [YYYY-MM-DD]
"""

from __future__ import annotations

import csv
import json
import sys
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from include.cities import CITIES  # noqa: E402

DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "weather_daily.csv"
REPORT_PATH = DATA_DIR / "REPORT.md"
FIELDS = ["day", "city", "state", "temp_min_c", "temp_max_c", "temp_avg_c",
          "humidity_avg_pct", "precipitation_mm", "hours"]
API = "https://api.open-meteo.com/v1/forecast"


def fetch(city: dict, day: str) -> dict:
    params = urllib.parse.urlencode({
        "latitude": city["lat"], "longitude": city["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "start_date": day, "end_date": day, "timezone": "America/Sao_Paulo",
    })
    with urllib.request.urlopen(f"{API}?{params}", timeout=30) as resp:
        return json.load(resp)["hourly"]


def aggregate(city: dict, day: str, hourly: dict) -> dict:
    temps = [t for t in hourly["temperature_2m"] if t is not None]
    hums = [h for h in hourly["relative_humidity_2m"] if h is not None]
    rain = [p or 0.0 for p in hourly["precipitation"]]
    if len(temps) < 23:  # data quality: exige o dia (quase) completo
        raise ValueError(f"{city['name']} {day}: apenas {len(temps)} leituras")
    return {
        "day": day, "city": city["name"], "state": city["state"],
        "temp_min_c": round(min(temps), 1), "temp_max_c": round(max(temps), 1),
        "temp_avg_c": round(sum(temps) / len(temps), 1),
        "humidity_avg_pct": round(sum(hums) / len(hums), 1),
        "precipitation_mm": round(sum(rain), 1), "hours": len(temps),
    }


def load_rows() -> list[dict]:
    if not CSV_PATH.exists():
        return []
    with CSV_PATH.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def save_rows(rows: list[dict]) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    rows.sort(key=lambda r: (r["day"], r["city"]))
    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_report(rows: list[dict], day: str) -> None:
    today = sorted((r for r in rows if r["day"] == day),
                   key=lambda r: float(r["temp_avg_c"]), reverse=True)
    days = sorted({r["day"] for r in rows})
    lines = [
        "# Clima nas capitais",
        "",
        f"Último dia processado: {day}. Histórico com {len(days)} dias e {len(rows)} "
        f"registros, desde {days[0]}.",
        "",
        "Arquivo gerado pelo workflow `daily-weather.yml`.",
        "",
        "| # | Capital | UF | Média °C | Mín °C | Máx °C | Umidade % | Chuva mm |",
        "|---|---------|----|---------:|-------:|-------:|----------:|---------:|",
    ]
    for i, r in enumerate(today, 1):
        lines.append(f"| {i} | {r['city']} | {r['state']} | {r['temp_avg_c']} | "
                     f"{r['temp_min_c']} | {r['temp_max_c']} | "
                     f"{r['humidity_avg_pct']} | {r['precipitation_mm']} |")
    hottest = max(rows, key=lambda r: float(r["temp_max_c"]))
    rainiest = max(rows, key=lambda r: float(r["precipitation_mm"]))
    lines += [
        "",
        "## Recordes do histórico",
        f"- Maior temperatura: {hottest['temp_max_c']} °C em {hottest['city']} ({hottest['day']})",
        f"- Dia mais chuvoso: {rainiest['precipitation_mm']} mm em {rainiest['city']} ({rainiest['day']})",
        "",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    day = sys.argv[1] if len(sys.argv) > 1 else (date.today() - timedelta(days=1)).isoformat()
    new = [aggregate(c, day, fetch(c, day)) for c in CITIES]
    rows = [r for r in load_rows() if r["day"] != day] + new  # idempotente
    save_rows(rows)
    write_report(rows, day)
    print(f"{day}: {len(new)} capitais processadas, {len(rows)} registros no histórico")


if __name__ == "__main__":
    main()
