"""Orchestrator: runs the medallion pipeline month by month.

    raw files ──► bronze_* ──► silver_* ──DQ gate──► gold (dims, fact, marts) ──► report

Features worth noting:
  * Incremental: months already loaded successfully are skipped (watermark in
    `pipeline_state`) unless --full-refresh is given.
  * Idempotent: each month is written with overwrite-partition semantics, so a
    re-run or a crash halfway never duplicates rows.
  * Observable: every run is logged to `pipeline_runs`; every DQ check to `dq_results`.

Usage:
    python -m mobilityflow.pipeline --months 2024-01 2024-02 2024-03
    python -m mobilityflow.pipeline --start 2024-01 --end 2024-06 --source nyc
    python -m mobilityflow.pipeline --source synthetic --months 2024-01   # offline
"""
from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import datetime, timezone

import pandas as pd

from . import ingest, quality, transform
from .config import Settings
from .warehouse import Warehouse

log = logging.getLogger("mobilityflow")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def month_range(start: str, end: str) -> list[str]:
    return [p.strftime("%Y-%m") for p in pd.period_range(start, end, freq="M")]


class Pipeline:
    def __init__(self, settings: Settings | None = None, source: str = "nyc"):
        self.s = settings or Settings()
        self.s.ensure_dirs()
        self.source = source
        self.s.extra["source"] = source
        self.wh = Warehouse(self.s.db_path, self.s.engine)
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        self.timings: dict[str, float] = {}

    # ------------------------------------------------------------ bookkeeping
    def _loaded_months(self) -> set[str]:
        if not self.wh.table_exists("pipeline_state"):
            return set()
        df = self.wh.query_df("SELECT source_month FROM pipeline_state WHERE status = 'success'")
        return set(df["source_month"])

    def _record_state(self, month: str, status: str, clean: int = 0, rejected: int = 0) -> None:
        row = pd.DataFrame([{"source_month": month, "status": status, "clean_rows": clean,
                             "rejected_rows": rejected, "run_id": self.run_id, "updated_at": _now()}])
        self.wh.write_df(row, "pipeline_state", "overwrite_partition", "source_month", month)

    def _timed(self, name, fn, *args, **kwargs):
        t0 = time.perf_counter()
        out = fn(*args, **kwargs)
        self.timings[name] = self.timings.get(name, 0) + time.perf_counter() - t0
        return out

    # ------------------------------------------------------------------ steps
    def load_zones(self) -> set[int]:
        bronze = self._timed("extract", ingest.extract_zones, self.s, self.source)
        self.wh.write_df(bronze, "bronze_zones", "replace")
        silver = transform.clean_zones(bronze)
        self.wh.write_df(silver, "silver_zones", "replace")
        log.info("Zones loaded: %d", len(silver))
        return set(int(x) for x in silver["location_id"])

    def process_month(self, month: str, zone_ids: set[int]) -> dict:
        log.info("── %s ──────────────────────────────", month)
        # Extract → bronze (raw, contract-conformed, stamped with lineage columns)
        trips_raw = self._timed("extract", ingest.extract_trips, self.s, month, self.source)
        weather_raw = self._timed("extract", ingest.extract_weather, self.s, month, self.source)
        self.wh.write_df(trips_raw, "bronze_trips", "overwrite_partition", "source_month", month)
        self.wh.write_df(weather_raw, "bronze_weather", "overwrite_partition", "source_month", month)
        log.info("Bronze: %d trips, %d weather hours", len(trips_raw), len(weather_raw))

        # Bronze → silver (read back from the warehouse: layers are decoupled)
        bronze_trips = self.wh.query_df("SELECT * FROM bronze_trips WHERE source_month = ?", [month])
        bronze_weather = self.wh.query_df("SELECT * FROM bronze_weather WHERE source_month = ?", [month])
        clean, rejected = self._timed("transform", transform.clean_trips, bronze_trips, zone_ids,
                                      month, self.s.co2_g_per_mile)
        weather = self._timed("transform", transform.clean_weather, bronze_weather)

        # DQ gate — critical failure stops this month before it is published
        results = quality.check_trip_batch(self.run_id, month, clean, rejected, weather, zone_ids,
                                           self.s.max_reject_rate, self.s.min_weather_coverage)
        self.wh.write_df(quality.to_frame(results), "dq_results", "append")
        for r in results:
            log.info("  DQ %-28s %-8s %s observed=%s", r.check_name, r.severity,
                     "PASS" if r.passed else "FAIL", r.observed)
        quality.enforce(results)

        self.wh.write_df(clean, "silver_trips", "overwrite_partition", "source_month", month)
        self.wh.write_df(rejected, "silver_trips_rejected", "overwrite_partition", "source_month", month)
        self.wh.write_df(weather, "silver_weather", "overwrite_partition", "source_month", month)
        self._record_state(month, "success", len(clean), len(rejected))
        log.info("Silver: %d clean, %d quarantined (%.1f%%)", len(clean), len(rejected),
                 100 * len(rejected) / max(1, len(clean) + len(rejected)))
        return {"month": month, "clean": len(clean), "rejected": len(rejected)}

    def build_gold(self) -> None:
        for f in sorted((self.s.sql_dir / "gold").glob("*.sql")):
            log.info("Gold: running %s", f.name)
            self._timed("gold", self.wh.run_sql_file, f)

    # ------------------------------------------------------------------- run
    def run(self, months: list[str], full_refresh: bool = False, report: bool = True) -> dict:
        started = _now()
        status, error, results = "success", "", []
        try:
            zone_ids = self.load_zones()
            done = set() if full_refresh else self._loaded_months()
            todo = [m for m in months if m not in done]
            skipped = [m for m in months if m in done]
            if skipped:
                log.info("Incremental: skipping already-loaded months %s", skipped)
            for m in todo:
                try:
                    results.append(self.process_month(m, zone_ids))
                except (quality.DataQualityError, ingest.SourceError) as exc:
                    log.error("Month %s failed: %s", m, exc)
                    self._record_state(m, "failed")
                    status, error = "partial", str(exc)
            if self.wh.table_exists("silver_trips"):
                self.build_gold()
                if report:
                    from .report import build_report
                    self._timed("report", build_report, self.wh, self.s)
            else:
                status = "failed"
        except Exception as exc:  # noqa: BLE001 - recorded, then re-raised
            status, error = "failed", repr(exc)
            raise
        finally:
            run_row = pd.DataFrame([{
                "run_id": self.run_id, "started_at": started, "finished_at": _now(),
                "status": status, "source": self.source, "engine": self.wh.engine,
                "months_requested": ",".join(months),
                "months_processed": ",".join(r["month"] for r in results),
                "clean_rows": sum(r["clean"] for r in results),
                "rejected_rows": sum(r["rejected"] for r in results),
                "error": error[:500],
                "timings": ", ".join(f"{k}={v:.1f}s" for k, v in self.timings.items()),
            }])
            self.wh.write_df(run_row, "pipeline_runs", "append")
            log.info("Run %s finished: %s (%s)", self.run_id, status,
                     ", ".join(f"{k}={v:.1f}s" for k, v in self.timings.items()))
        return {"run_id": self.run_id, "status": status, "months": results}


def main(argv: list[str] | None = None) -> dict:
    p = argparse.ArgumentParser(description="MobilityFlow: NYC taxi + weather ELT pipeline")
    p.add_argument("--months", nargs="*", help="Months as YYYY-MM")
    p.add_argument("--start", help="First month YYYY-MM (with --end)")
    p.add_argument("--end", help="Last month YYYY-MM")
    p.add_argument("--source", choices=["nyc", "synthetic"], default="nyc")
    p.add_argument("--dataset", choices=["green", "yellow"], default="green")
    p.add_argument("--engine", choices=["auto", "duckdb", "sqlite"], default="auto")
    p.add_argument("--max-rows", type=int, default=500_000, help="Row cap per month (0 = none)")
    p.add_argument("--full-refresh", action="store_true", help="Reprocess already-loaded months")
    p.add_argument("--no-report", action="store_true")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    months = a.months or (month_range(a.start, a.end) if a.start and a.end else None)
    if not months:
        p.error("give --months or --start/--end")
    s = Settings(dataset=a.dataset, engine=a.engine, max_rows_per_month=a.max_rows or None)
    return Pipeline(s, source=a.source).run(months, full_refresh=a.full_refresh,
                                             report=not a.no_report)


if __name__ == "__main__":
    main()
