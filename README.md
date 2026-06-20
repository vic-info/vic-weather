# Weather Lakehouse Serving System

Starter repo for a 3-session weather lakehouse course.

The course keeps one clear production-style path:

```text
Raw Weather Data
        ↓
Bronze Delta
        ↓
Silver Delta
        ↓
Gold Delta
        ↓
Data Quality
        ↓
Lakebase/Postgres Serving
        ↓
REST API
```

Lesson 1 covers the first part only:

```text
Open-Meteo Historical Weather API
        ↓
Local raw CSV
        ↓
Databricks Bronze Delta table
        ↓
Databricks Silver Delta table
```

## Lesson 1 Goals

By the end of Lesson 1, students should be able to:

1. Fetch historical daily weather data from Open-Meteo.
2. Save raw weather data as a local CSV file.
3. Upload the CSV into a Databricks Volume.
4. Create a Bronze Delta table from raw CSV.
5. Create a Silver Delta table with cleaned fields and weather flags.

## Course Scope

Core topics:

- Bronze / Silver / Gold data layering
- PySpark transforms
- Gold table design
- Data quality checks
- Serving layer
- REST API

Brief topics:

- Unity Catalog
- Databricks Volume
- Databricks Workflow
- Swagger
- Lakebase synced table

After-class extensions:

- Node.js backend
- React frontend
- OAuth authentication
- Continuous sync
- More weather risk rules
- Automated Job scheduling

Full 3-session plan:

```text
docs/course-plan.md
```

## Repo Structure

```text
.
├── README.md
├── requirements.txt
├── data/
│   └── README.md
├── scripts/
│   └── fetch_open_meteo_weather.py
├── databricks/
│   ├── 00_setup.py
│   ├── 01_ingest_bronze_weather.py
│   └── 02_clean_silver_weather.py
└── docs/
    ├── course-plan.md
    ├── lesson-01-raw-bronze-silver.md
    ├── lesson-02-gold-data-quality.md
    └── lesson-03-serving-api.md
```

## Local Setup

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip3 install -r requirements.txt
```

Fetch raw data:

```bash
python3 scripts/fetch_open_meteo_weather.py
```

Expected output:

```text
data/raw_weather_daily.csv
```

## Databricks Setup

In Databricks, create a folder such as:

```text
/Workspace/Users/<your-email>/weather-lakehouse-serving-system
```

Import these files as notebooks or copy their code into notebooks:

```text
databricks/00_setup.py
databricks/01_ingest_bronze_weather.py
databricks/02_clean_silver_weather.py
```

Upload `data/raw_weather_daily.csv` to:

```text
/Volumes/workspace/default/weather/raw_weather_daily.csv
```

Run notebooks in this order:

```text
00_setup
01_ingest_bronze_weather
02_clean_silver_weather
```

## Lesson 1 Expected Tables

```text
workspace.default.bronze_weather_daily_raw
workspace.default.silver_weather_daily_clean
```

## Next Lessons

[Lesson 2](docs/lesson-02-gold-data-quality.md) will add Gold tables and data quality checks.

[Lesson 3](docs/lesson-03-serving-api.md) will add Lakebase/Postgres serving and a Spring Boot API.
