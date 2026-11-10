"""Synthetic NYC-like taxi, zone and weather data.

Used for offline development, unit tests and CI. It mimics the real TLC schema
and deliberately injects the kinds of defects found in real trip records
(negative fares, zero-distance trips, clock errors, unknown zones, duplicates),
so the cleaning and data-quality layers have something real to catch.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

BOROUGH_RANGES = [
    (1, 1, "EWR"), (2, 50, "Queens"), (51, 90, "Bronx"), (91, 170, "Manhattan"),
    (171, 185, "Staten Island"), (186, 263, "Brooklyn"), (264, 265, "Unknown"),
]

HOUR_WEIGHTS = np.array([
    0.35, 0.25, 0.18, 0.15, 0.18, 0.35, 0.70, 1.05, 1.20, 1.00, 0.90, 0.92,
    0.95, 0.95, 1.00, 1.10, 1.20, 1.30, 1.25, 1.05, 0.90, 0.80, 0.65, 0.50,
])


def _seed(month: str, salt: int = 0) -> int:
    y, m = map(int, month.split("-"))
    return y * 100 + m + salt


def _month_hours(month: str) -> pd.DatetimeIndex:
    start = pd.Timestamp(f"{month}-01")
    end = start + pd.offsets.MonthBegin(1)
    return pd.date_range(start, end, freq="h", inclusive="left")


def generate_zones() -> pd.DataFrame:
    rows = []
    for lo, hi, borough in BOROUGH_RANGES:
        for zid in range(lo, hi + 1):
            rows.append({
                "LocationID": zid,
                "Borough": borough,
                "Zone": f"{borough} Zone {zid:03d}",
                "service_zone": "Yellow Zone" if borough == "Manhattan" else "Boro Zone",
            })
    return pd.DataFrame(rows)


def generate_weather(month: str) -> pd.DataFrame:
    rng = np.random.default_rng(_seed(month, 7))
    hours = _month_hours(month)
    n = len(hours)
    m = hours.month[0]
    seasonal = 12 - 13 * np.cos((m - 1) / 12 * 2 * np.pi)          # ~-1C Jan, ~25C Jul
    daily = 4 * np.sin((hours.hour.values - 9) / 24 * 2 * np.pi)
    temp = seasonal + daily + rng.normal(0, 1.5, n)

    raining = np.zeros(n, dtype=bool)
    for i in range(1, n):
        raining[i] = rng.random() < (0.72 if raining[i - 1] else 0.035)
    precip = np.where(raining, rng.exponential(1.4, n), 0.0)
    wind = np.clip(rng.normal(14, 6, n) + raining * 6, 0, None)

    return pd.DataFrame({
        "time": hours.strftime("%Y-%m-%dT%H:%M"),
        "temperature_2m": temp.round(1),
        "precipitation": precip.round(2),
        "wind_speed_10m": wind.round(1),
    })


def generate_trips(month: str, base_trips_per_hour: float = 60.0) -> pd.DataFrame:
    """Return a DataFrame shaped like a TLC green-taxi monthly file."""
    rng = np.random.default_rng(_seed(month))
    hours = _month_hours(month)
    weather = generate_weather(month)
    precip = weather["precipitation"].values

    weekend = hours.dayofweek.values >= 5
    weights = HOUR_WEIGHTS[hours.hour.values] * np.where(weekend, 0.85, 1.0)
    weights = weights * (1 + 0.25 * (precip > 0.1))                 # rain lifts taxi demand
    counts = rng.poisson(base_trips_per_hour * weights)
    hour_idx = np.repeat(np.arange(len(hours)), counts)
    n = len(hour_idx)

    pickup = hours.values[hour_idx] + rng.integers(0, 3600, n).astype("timedelta64[s]")
    h = hours.hour.values[hour_idx]
    is_rush = (~weekend[hour_idx]) & (((h >= 7) & (h <= 9)) | ((h >= 16) & (h <= 19)))
    is_night = (h <= 5) | (h >= 22)
    rain = precip[hour_idx] > 0.1

    zones = generate_zones()
    valid_ids = zones.loc[zones.Borough != "Unknown", "LocationID"].values
    zone_borough = dict(zip(zones.LocationID, zones.Borough))
    pref = np.array([3.0 if zone_borough[z] in ("Brooklyn", "Queens") else
                     2.0 if zone_borough[z] == "Manhattan" else 1.0 for z in valid_ids])
    pu = rng.choice(valid_ids, n, p=pref / pref.sum())
    same = rng.random(n) < 0.6
    do = np.where(same, pu + rng.integers(-3, 4, n), rng.choice(valid_ids, n))
    do = np.clip(do, 2, 263)

    speed = np.full(n, 13.0)
    speed = np.where(is_rush, 8.5, speed)
    speed = np.where(is_night, 19.0, speed)
    speed = speed * np.where(rain, 0.85, 1.0)
    speed = speed * np.where(np.vectorize(zone_borough.get)(pu) == "Manhattan", 0.75, 1.0)
    speed = np.clip(speed * rng.normal(1, 0.15, n), 3, 45)

    dist = np.clip(rng.lognormal(0.8, 0.6, n), 0.2, 40).round(2)
    duration_s = (dist / speed * 3600 + rng.normal(90, 30, n)).clip(60).astype(int)
    dropoff = pickup + duration_s.astype("timedelta64[s]")

    payment = rng.choice([1, 2, 3], n, p=[0.68, 0.30, 0.02])
    fare = (3.0 + 2.5 * dist + 0.5 * duration_s / 60).round(2)
    tip = np.where(payment == 1, fare * rng.uniform(0.1, 0.25, n), 0).round(2)
    tolls = np.where(rng.random(n) < 0.03, 6.94, 0.0)
    congestion = np.where(np.vectorize(zone_borough.get)(do) == "Manhattan", 2.75, 0.0)
    total = (fare + tip + tolls + congestion + 1.5).round(2)

    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], n, p=[0.2, 0.8]),
        "lpep_pickup_datetime": pickup,
        "lpep_dropoff_datetime": dropoff,
        "store_and_fwd_flag": "N",
        "RatecodeID": 1.0,
        "PULocationID": pu,
        "DOLocationID": do,
        "passenger_count": rng.choice([1, 1, 1, 2, 3, 5], n).astype(float),
        "trip_distance": dist,
        "fare_amount": fare,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": tip,
        "tolls_amount": tolls,
        "improvement_surcharge": 1.0,
        "total_amount": total,
        "payment_type": payment.astype(float),
        "trip_type": 1.0,
        "congestion_surcharge": congestion,
    })
    return _inject_defects(df, rng)


def _inject_defects(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    n = len(df)
    k = max(1, int(n * 0.005))

    def pick():
        return rng.choice(n, k, replace=False)

    df.loc[pick(), "fare_amount"] *= -1                                  # refunds / sign errors
    df.loc[pick(), "trip_distance"] = 0.0                                # meter not started
    idx = pick()
    df.loc[idx, "lpep_dropoff_datetime"] = df.loc[idx, "lpep_pickup_datetime"] - pd.Timedelta("5min")
    df.loc[pick(), "PULocationID"] = 999                                 # unknown zone
    df.loc[pick(), "lpep_pickup_datetime"] = pd.Timestamp("2009-01-01 00:10")  # clock reset
    df.loc[pick(), "passenger_count"] = np.nan                           # missing meta
    dups = df.sample(k, random_state=1)                                  # duplicate uploads
    return pd.concat([df, dups], ignore_index=True)
