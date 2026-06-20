from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import requests


CITIES = [
    {"city": "San Francisco", "latitude": 37.7749, "longitude": -122.4194},
    {"city": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
    {"city": "Austin", "latitude": 30.2672, "longitude": -97.7431},
    {"city": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
]

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
]

EXPECTED_COLUMNS = [
    "time",
    *DAILY_FIELDS,
    "city",
    "latitude",
    "longitude",
]


def fetch_city_weather(city: dict[str, float | str], start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": DAILY_FIELDS,
        "timezone": "auto",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    daily = payload["daily"]

    city_df = pd.DataFrame(daily)
    city_df["city"] = city["city"]
    city_df["latitude"] = city["latitude"]
    city_df["longitude"] = city["longitude"]

    return city_df


def fetch_weather_dataset(start_date: str, end_date: str) -> pd.DataFrame:
    city_frames = [
        fetch_city_weather(city, start_date=start_date, end_date=end_date)
        for city in CITIES
    ]

    return pd.concat(city_frames, ignore_index=True)


def validate_weather_dataset(weather_df: pd.DataFrame) -> None:
    missing_columns = [
        column for column in EXPECTED_COLUMNS
        if column not in weather_df.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing expected columns: {missing_columns}")

    null_counts = weather_df[EXPECTED_COLUMNS].isna().sum()
    columns_with_nulls = null_counts[null_counts > 0]
    if not columns_with_nulls.empty:
        raise ValueError(
            "Raw weather data contains nulls:\n"
            + columns_with_nulls.to_string()
        )

    city_summary = (
        weather_df
        .groupby("city")
        .agg(
            rows=("time", "count"),
            min_date=("time", "min"),
            max_date=("time", "max"),
        )
        .sort_index()
    )

    print("\nDataset summary:")
    print(city_summary.to_string())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch daily historical weather data from Open-Meteo."
    )
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--output", default="data/raw_weather_daily.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    weather_df = fetch_weather_dataset(
        start_date=args.start_date,
        end_date=args.end_date,
    )
    validate_weather_dataset(weather_df)
    weather_df.to_csv(output_path, index=False)

    print("\nSample rows:")
    print(weather_df.head())
    print(f"Rows written: {len(weather_df)}")
    print(f"Output file: {output_path}")


if __name__ == "__main__":
    main()
