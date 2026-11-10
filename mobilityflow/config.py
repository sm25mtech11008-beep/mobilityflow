"""Central configuration for the MobilityFlow pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Settings:
    # --- storage -----------------------------------------------------------
    data_dir: Path = PROJECT_ROOT / "data"
    db_path: Path = PROJECT_ROOT / "data" / "warehouse.duckdb"
    report_dir: Path = PROJECT_ROOT / "reports"
    sql_dir: Path = PROJECT_ROOT / "sql"
    engine: str = "auto"  # "duckdb", "sqlite" or "auto" (duckdb if installed)

    # --- sources -----------------------------------------------------------
    dataset: str = "green"  # NYC TLC dataset: "green" (~50k rows/month) or "yellow" (~3M)
    tlc_base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    zone_lookup_url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    weather_url: str = "https://archive-api.open-meteo.com/v1/archive"
    city_lat: float = 40.7128
    city_lon: float = -74.0060
    timezone: str = "America/New_York"

    # --- processing ----------------------------------------------------------
    # Cap rows per month so the pipeline fits in free Colab RAM (None = no cap).
    max_rows_per_month: int | None = 500_000
    # Assumed tailpipe CO2 for a typical petrol passenger car, grams per mile.
    # US EPA's commonly cited figure is ~400 g/mile; change it for your scenario.
    co2_g_per_mile: float = 400.0

    # --- data-quality thresholds ---------------------------------------------
    max_reject_rate: float = 0.15
    min_weather_coverage: float = 0.95

    extra: dict = field(default_factory=dict)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    def ensure_dirs(self) -> None:
        for d in (self.data_dir, self.raw_dir, self.report_dir):
            d.mkdir(parents=True, exist_ok=True)
