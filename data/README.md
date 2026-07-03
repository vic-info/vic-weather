# Data Directory

This directory contains the weather download script and its generated CSV file.

From the repository root, run:

```bash
python3 data/fetch_open_meteo_weather.py
```

This creates:

```text
data/raw_weather_daily.csv
```

The repository includes a generated CSV so the Databricks lesson can run without
calling the external API. Run the script again whenever you want to refresh it.

Upload the generated CSV to the Databricks Volume path:

```text
/Volumes/workspace/default/weather/raw_weather_daily.csv
```
