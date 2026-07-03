# Weather Lakehouse Serving System

A weather data platform that processes historical Open-Meteo data through a
Databricks lakehouse and serves curated metrics through a Node.js REST API.

```text
Open-Meteo
    ↓
Raw CSV → Bronze Delta → Silver Delta → Gold Delta
                                              ↓
                                      Data Quality
                                              ↓
                                  Lakebase / Postgres
                                              ↓
                                      Node.js REST API
```

## Documentation

- [Course plan](docs/course-plan.md)
- [Lesson 1: Raw to Bronze and Silver](docs/lesson-01-raw-bronze-silver.md)
- [Lesson 2: Gold and Data Quality](docs/lesson-02-gold-data-quality.md)
- [Lesson 3: Serving and Node.js API](docs/lesson-03-serving-api.md)
- [Full project reference](End-to-End%20Weather%20Lakehouse%20Serving%20Project%20操作指南.md)

## Repository

```text
.
├── data/
│   ├── README.md
│   └── fetch_open_meteo_weather.py
├── databricks/
│   ├── 00_setup.py
│   ├── 00_table_schemas.py
│   ├── 01_ingest_bronze_weather.py
│   ├── 02_clean_silver_weather.py
│   ├── 03_build_gold_weather_metrics.py
│   └── 04_data_quality_checks.py
├── docs/
└── requirements.txt
```

## Quick Start

Install the local data-fetch dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

Download the weather dataset:

```bash
python3 data/fetch_open_meteo_weather.py
```

Upload `data/raw_weather_daily.csv` to:

```text
/Volumes/workspace/default/weather/raw_weather_daily.csv
```

Run the Databricks notebooks in filename order. Continue with the serving and API
setup in the Lesson 3 document.

The repository includes the generated lesson CSV for offline classroom use. Local
environment files remain excluded from Git.
