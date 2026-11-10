"""Silver layer: typed, validated, de-duplicated and enriched records.

Bad rows are never silently dropped: each is routed to a quarantine table with
the first rule it broke, so data loss is measurable and auditable.
"""
from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

NATURAL_KEY = ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id",
               "trip_distance", "total_amount"]


def _trip_id(df: pd.DataFrame) -> pd.Series:
    key = df[NATURAL_KEY].astype(str).agg("|".join, axis=1)
    return key.map(lambda s: hashlib.md5(s.encode()).hexdigest()[:16])


def clean_trips(bronze: pd.DataFrame, valid_zone_ids: set[int], month: str,
                co2_g_per_mile: float = 400.0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean, rejected) DataFrames for one month of trips."""
    df = bronze.copy()
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    for c in ["trip_distance", "fare_amount", "tip_amount", "tolls_amount",
              "congestion_surcharge", "total_amount", "passenger_count"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["pu_location_id", "do_location_id", "payment_type", "vendor_id"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    df["duration_min"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    df["avg_speed_mph"] = df["trip_distance"] / (df["duration_min"] / 60)

    month_start = pd.Timestamp(f"{month}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)

    # Ordered rules: the first rule a row fails is recorded as its reject reason.
    rules = [
        ("missing_timestamp", df["pickup_datetime"].isna() | df["dropoff_datetime"].isna()),
        ("pickup_outside_month", (df["pickup_datetime"] < month_start) | (df["pickup_datetime"] >= month_end)),
        ("non_positive_duration", df["duration_min"] <= 0),
        ("implausible_duration", (df["duration_min"] < 1) | (df["duration_min"] > 180)),
        ("non_positive_distance", ~(df["trip_distance"] > 0)),
        ("implausible_distance", df["trip_distance"] > 100),
        ("negative_amount", (df["fare_amount"] < 0) | (df["total_amount"] < 0)),
        ("implausible_speed", df["avg_speed_mph"] > 80),
        ("unknown_zone", ~df["pu_location_id"].isin(valid_zone_ids) | ~df["do_location_id"].isin(valid_zone_ids)),
    ]
    reason = pd.Series(pd.NA, index=df.index, dtype="object")
    for name, mask in rules:
        reason = reason.mask(reason.isna() & mask.fillna(False).astype(bool), name)

    df["trip_id"] = _trip_id(df)
    dup = df["trip_id"].duplicated(keep="first") & reason.isna()
    reason = reason.mask(dup, "duplicate")

    rejected = df[reason.notna()].copy()
    rejected["reject_reason"] = reason[reason.notna()]
    rejected = rejected[["trip_id", "source_month", "reject_reason", "pickup_datetime",
                         "pu_location_id", "trip_distance", "fare_amount", "total_amount"]]
    rejected["pickup_datetime"] = rejected["pickup_datetime"].astype(str)

    clean = df[reason.isna()].copy()
    clean["passenger_count_imputed"] = clean["passenger_count"].isna()
    clean["passenger_count"] = clean["passenger_count"].fillna(1).clip(1, 8).astype(int)
    clean["pickup_date"] = clean["pickup_datetime"].dt.strftime("%Y-%m-%d")
    clean["pickup_hour"] = clean["pickup_datetime"].dt.hour.astype(int)
    clean["day_of_week"] = clean["pickup_datetime"].dt.dayofweek.astype(int)
    clean["is_weekend"] = (clean["day_of_week"] >= 5).astype(int)
    clean["weather_hour_key"] = clean["pickup_datetime"].dt.strftime("%Y-%m-%d %H:00")
    clean["tip_pct"] = np.where(clean["fare_amount"] > 0,
                                clean["tip_amount"] / clean["fare_amount"] * 100, 0.0).round(2)
    clean["co2_kg"] = (clean["trip_distance"] * co2_g_per_mile / 1000).round(4)
    clean["duration_min"] = clean["duration_min"].round(2)
    clean["avg_speed_mph"] = clean["avg_speed_mph"].round(2)
    for c in ["pickup_datetime", "dropoff_datetime"]:
        clean[c] = clean[c].dt.strftime("%Y-%m-%d %H:%M:%S")
    for c in ["pu_location_id", "do_location_id", "payment_type", "vendor_id"]:
        clean[c] = clean[c].astype("float").astype("Int64")

    cols = ["trip_id", "source_month", "vendor_id", "pickup_datetime", "dropoff_datetime",
            "pickup_date", "pickup_hour", "day_of_week", "is_weekend", "weather_hour_key",
            "pu_location_id", "do_location_id", "passenger_count", "passenger_count_imputed",
            "trip_distance", "duration_min", "avg_speed_mph", "fare_amount", "tip_amount",
            "tip_pct", "tolls_amount", "congestion_surcharge", "total_amount", "payment_type",
            "co2_kg"]
    clean["passenger_count_imputed"] = clean["passenger_count_imputed"].astype(int)
    return clean[cols].reset_index(drop=True), rejected.reset_index(drop=True)


def clean_weather(bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze.copy()
    ts = pd.to_datetime(df["time"], errors="coerce")
    df = df[ts.notna()].copy()
    df["hour_key"] = ts[ts.notna()].dt.strftime("%Y-%m-%d %H:00")
    df["temperature_c"] = pd.to_numeric(df["temperature_2m"], errors="coerce")
    df["precip_mm"] = pd.to_numeric(df["precipitation"], errors="coerce").fillna(0)
    df["wind_kmh"] = pd.to_numeric(df["wind_speed_10m"], errors="coerce")
    df["precip_bucket"] = pd.cut(df["precip_mm"], [-0.01, 0.1, 2.5, 1e9],
                                 labels=["dry", "light_rain", "heavy_rain"]).astype(str)
    df["temp_bucket"] = pd.cut(df["temperature_c"], [-100, 0, 10, 20, 100],
                               labels=["below_0C", "0-10C", "10-20C", "above_20C"]).astype(str)
    df = df.drop_duplicates("hour_key")
    return df[["hour_key", "source_month", "temperature_c", "precip_mm", "wind_kmh",
               "precip_bucket", "temp_bucket"]].reset_index(drop=True)


def clean_zones(bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze.copy()
    df["location_id"] = pd.to_numeric(df["location_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["location_id"]).drop_duplicates("location_id")
    df["borough"] = df["borough"].fillna("Unknown").replace({"N/A": "Unknown"})
    df["zone"] = df["zone"].fillna("Unknown")
    return df[["location_id", "borough", "zone", "service_zone"]].reset_index(drop=True)
