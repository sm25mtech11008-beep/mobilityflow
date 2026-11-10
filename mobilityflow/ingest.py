"""Extract layer: pull raw data from the NYC TLC and Open-Meteo, or synthesize it.

Every extract returns a DataFrame that conforms to a fixed bronze contract
(column names + order). Schema drift in the source is logged, not silently
absorbed, so a column rename upstream shows up in the run log.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from . import synthetic
from .config import Settings

log = logging.getLogger(__name__)

TRIP_CONTRACT = [
    "vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount", "tolls_amount",
    "congestion_surcharge", "total_amount", "payment_type",
]
_RENAMES = {
    "vendorid": "vendor_id",
    "lpep_pickup_datetime": "pickup_datetime", "tpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime", "tpep_dropoff_datetime": "dropoff_datetime",
    "pulocationid": "pu_location_id", "dolocationid": "do_location_id",
}


class SourceError(RuntimeError):
    pass


def with_retries(fn, attempts: int = 3, backoff: float = 2.0):
    """Call fn(), retrying transient failures with exponential backoff."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except (requests.RequestException, SourceError) as exc:
            if i == attempts:
                raise
            wait = backoff ** i
            log.warning("Attempt %d/%d failed (%s); retrying in %.0fs", i, attempts, exc, wait)
            time.sleep(wait)


def download(url: str, dest: Path, timeout: int = 120) -> Path:
    """Download url to dest, skipping if already cached (raw zone is immutable)."""
    if dest.exists() and dest.stat().st_size > 0:
        log.info("Cache hit: %s", dest.name)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _get():
        with requests.get(url, stream=True, timeout=timeout) as r:
            if r.status_code == 403 or r.status_code == 404:
                raise SourceError(f"{url} returned {r.status_code} (month not published yet?)")
            r.raise_for_status()
            tmp = dest.with_suffix(dest.suffix + ".part")
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
            tmp.rename(dest)  # atomic: never leave a half-written file in the raw zone
        return dest

    log.info("Downloading %s", url)
    return with_retries(_get)


def _enforce_contract(df: pd.DataFrame, contract: list[str], name: str) -> pd.DataFrame:
    missing = [c for c in contract if c not in df.columns]
    extra = [c for c in df.columns if c not in contract]
    if missing:
        log.warning("[%s] schema drift - missing columns filled with NULL: %s", name, missing)
    if extra:
        log.info("[%s] dropping %d non-contract columns: %s", name, len(extra), extra)
    return df.reindex(columns=contract)


def _stamp(df: pd.DataFrame, month: str | None, source_file: str) -> pd.DataFrame:
    df = df.copy()
    if month is not None:
        df["source_month"] = month
    df["_source_file"] = source_file
    df["_ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return df


# ---------------------------------------------------------------- extracts
def extract_trips(settings: Settings, month: str, source: str) -> pd.DataFrame:
    if source == "synthetic":
        raw, fname = synthetic.generate_trips(month), f"synthetic_{month}"
    else:
        fname = f"{settings.dataset}_tripdata_{month}.parquet"
        path = download(f"{settings.tlc_base_url}/{fname}", settings.raw_dir / fname)
        raw = pd.read_parquet(path)

    raw.columns = [_RENAMES.get(c.lower(), c.lower()) for c in raw.columns]
    df = _enforce_contract(raw, TRIP_CONTRACT, f"trips {month}")
    cap = settings.max_rows_per_month
    if cap and len(df) > cap:
        log.info("Sampling %d of %d rows for %s (memory cap)", cap, len(df), month)
        df = df.sample(cap, random_state=42)
    return _stamp(df, month, fname)


def extract_zones(settings: Settings, source: str) -> pd.DataFrame:
    if source == "synthetic":
        raw, fname = synthetic.generate_zones(), "synthetic_zones"
    else:
        fname = "taxi_zone_lookup.csv"
        raw = pd.read_csv(download(settings.zone_lookup_url, settings.raw_dir / fname))
    raw = raw.rename(columns={"LocationID": "location_id", "Borough": "borough",
                              "Zone": "zone", "service_zone": "service_zone"})
    df = _enforce_contract(raw, ["location_id", "borough", "zone", "service_zone"], "zones")
    return _stamp(df, None, fname)


def extract_weather(settings: Settings, month: str, source: str) -> pd.DataFrame:
    if source == "synthetic":
        raw, fname = synthetic.generate_weather(month), f"synthetic_weather_{month}"
    else:
        start = pd.Timestamp(f"{month}-01")
        end = start + pd.offsets.MonthEnd(0)
        params = {
            "latitude": settings.city_lat, "longitude": settings.city_lon,
            "start_date": start.strftime("%Y-%m-%d"), "end_date": end.strftime("%Y-%m-%d"),
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": settings.timezone,
        }

        def _get():
            r = requests.get(settings.weather_url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()

        payload = with_retries(_get)
        raw = pd.DataFrame(payload["hourly"])
        fname = f"open-meteo_{month}"
        (settings.raw_dir / f"{fname}.csv").parent.mkdir(parents=True, exist_ok=True)
        raw.to_csv(settings.raw_dir / f"{fname}.csv", index=False)

    df = _enforce_contract(raw, ["time", "temperature_2m", "precipitation", "wind_speed_10m"],
                           f"weather {month}")
    return _stamp(df, month, fname)
