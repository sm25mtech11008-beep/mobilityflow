"""Tests for cleaning rules, the DQ gate, idempotency and incremental loads.

Run with:  pytest -q
They use synthetic data, so no network is needed (works in CI).
"""
from __future__ import annotations

import pandas as pd
import pytest

from mobilityflow import ingest, quality, synthetic, transform
from mobilityflow.config import Settings
from mobilityflow.pipeline import Pipeline

ZONES = set(range(1, 266))


def _bronze(rows: list[dict]) -> pd.DataFrame:
    base = dict(vendor_id=2, pu_location_id=10, do_location_id=20, passenger_count=1,
                trip_distance=2.0, fare_amount=10.0, tip_amount=2.0, tolls_amount=0.0,
                congestion_surcharge=0.0, total_amount=13.0, payment_type=1,
                source_month="2024-01")
    df = pd.DataFrame([{**base, **r} for r in rows])
    return df


def test_cleaning_rules_route_bad_rows_to_quarantine():
    df = _bronze([
        dict(pickup_datetime="2024-01-05 08:00:00", dropoff_datetime="2024-01-05 08:15:00"),  # good
        dict(pickup_datetime="2024-01-05 09:00:00", dropoff_datetime="2024-01-05 08:55:00"),  # time travel
        dict(pickup_datetime="2009-01-01 00:00:00", dropoff_datetime="2024-01-05 08:15:00"),  # clock reset
        dict(pickup_datetime="2024-01-05 10:00:00", dropoff_datetime="2024-01-05 10:10:00", trip_distance=0),
        dict(pickup_datetime="2024-01-05 11:00:00", dropoff_datetime="2024-01-05 11:10:00", fare_amount=-5),
        dict(pickup_datetime="2024-01-05 12:00:00", dropoff_datetime="2024-01-05 12:10:00", pu_location_id=999),
        dict(pickup_datetime="2024-01-05 13:00:00", dropoff_datetime="2024-01-05 13:01:30", trip_distance=50),
    ])
    clean, rejected = transform.clean_trips(df, ZONES, "2024-01")
    assert len(clean) == 1
    assert set(rejected.reject_reason) == {
        "non_positive_duration", "pickup_outside_month", "non_positive_distance",
        "negative_amount", "unknown_zone", "implausible_speed",
    }
    row = clean.iloc[0]
    assert row.pickup_hour == 8 and row.weather_hour_key == "2024-01-05 08:00"
    assert row.co2_kg == pytest.approx(2.0 * 400 / 1000)


def test_duplicates_are_quarantined_not_dropped():
    good = dict(pickup_datetime="2024-01-05 08:00:00", dropoff_datetime="2024-01-05 08:15:00")
    clean, rejected = transform.clean_trips(_bronze([good, good]), ZONES, "2024-01")
    assert len(clean) == 1
    assert list(rejected.reject_reason) == ["duplicate"]


def test_contract_handles_schema_drift():
    raw = pd.DataFrame({"VendorID": [1], "tpep_pickup_datetime": ["2024-01-01"], "new_col": [1]})
    raw.columns = [ingest._RENAMES.get(c.lower(), c.lower()) for c in raw.columns]
    out = ingest._enforce_contract(raw, ingest.TRIP_CONTRACT, "test")
    assert list(out.columns) == ingest.TRIP_CONTRACT
    assert out["pickup_datetime"].iloc[0] == "2024-01-01"
    assert out["fare_amount"].isna().all()


def test_dq_gate_blocks_bad_batch():
    month = "2024-01"
    bronze = synthetic.generate_trips(month)
    bronze.columns = [ingest._RENAMES.get(c.lower(), c.lower()) for c in bronze.columns]
    bronze["source_month"] = month
    weather = transform.clean_weather(synthetic.generate_weather(month).assign(source_month=month))
    clean, rejected = transform.clean_trips(bronze, ZONES, month)

    ok = quality.check_trip_batch("r", month, clean, rejected, weather, ZONES, 0.15, 0.95)
    quality.enforce(ok)  # healthy batch passes

    # Simulate a broken upstream feed: most rows rejected
    bad = quality.check_trip_batch("r", month, clean.head(10), rejected, weather, ZONES, 0.15, 0.95)
    with pytest.raises(quality.DataQualityError):
        quality.enforce(bad)


@pytest.fixture
def settings(tmp_path):
    return Settings(data_dir=tmp_path / "data", db_path=tmp_path / "data" / "wh.duckdb",
                    report_dir=tmp_path / "reports")


def test_end_to_end_idempotent_and_incremental(settings):
    p = Pipeline(settings, source="synthetic")
    first = p.run(["2024-01"], report=True)
    assert first["status"] == "success"
    n1 = p.wh.scalar("SELECT COUNT(*) FROM fact_trips")
    assert n1 > 10_000
    assert (settings.report_dir / "report.html").exists()

    # Incremental: second run skips the loaded month, data unchanged
    second = Pipeline(settings, source="synthetic").run(["2024-01"], report=False)
    assert second["months"] == []

    # Idempotent: a full refresh reprocesses but never duplicates
    p3 = Pipeline(settings, source="synthetic")
    p3.run(["2024-01"], full_refresh=True, report=False)
    assert p3.wh.scalar("SELECT COUNT(*) FROM fact_trips") == n1
    assert p3.wh.scalar("SELECT COUNT(*) FROM bronze_trips") == \
        n1 + p3.wh.scalar("SELECT COUNT(*) FROM silver_trips_rejected")

    marts = ["mart_hourly_demand", "mart_weather_impact", "mart_borough_emissions",
             "mart_congestion_hotspots", "mart_od_flows"]
    for m in marts:
        assert p3.wh.scalar(f"SELECT COUNT(*) FROM {m}") > 0, m
    assert p3.wh.scalar("SELECT COUNT(*) FROM pipeline_runs") == 3
