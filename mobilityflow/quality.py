"""Data-quality gate between silver and gold.

Each check records an observed value against a threshold. Results are written
to the `dq_results` table on every run (pass or fail), giving an audit trail.
A failed *critical* check stops the pipeline before bad data reaches gold.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    run_id: str
    source_month: str
    check_name: str
    severity: str          # "critical" | "warning"
    passed: bool
    observed: float
    threshold: str
    details: str = ""


def check_trip_batch(run_id: str, month: str, clean: pd.DataFrame, rejected: pd.DataFrame,
                     weather: pd.DataFrame, valid_zone_ids: set[int],
                     max_reject_rate: float, min_weather_coverage: float) -> list[CheckResult]:
    total = len(clean) + len(rejected)
    reject_rate = len(rejected) / total if total else 1.0
    coverage = clean["weather_hour_key"].isin(set(weather["hour_key"])).mean() if len(clean) else 0.0
    null_keys = int(clean[["trip_id", "pickup_datetime", "pu_location_id"]].isna().any(axis=1).sum())
    dup_ids = int(clean["trip_id"].duplicated().sum())
    orphans = int((~clean["pu_location_id"].isin(valid_zone_ids)).sum()
                  + (~clean["do_location_id"].isin(valid_zone_ids)).sum())
    top_reasons = (rejected["reject_reason"].value_counts().head(3).to_dict()
                   if len(rejected) else {})

    def r(name, sev, ok, obs, thr, det=""):
        return CheckResult(run_id, month, name, sev, bool(ok), float(obs), thr, det)

    return [
        r("row_count_positive", "critical", len(clean) > 0, len(clean), "> 0"),
        r("reject_rate", "critical", reject_rate <= max_reject_rate, round(reject_rate, 4),
          f"<= {max_reject_rate}", f"top reasons: {top_reasons}"),
        r("no_null_keys", "critical", null_keys == 0, null_keys, "== 0"),
        r("trip_id_unique", "critical", dup_ids == 0, dup_ids, "== 0"),
        r("zone_referential_integrity", "critical", orphans == 0, orphans, "== 0"),
        r("weather_coverage", "warning", coverage >= min_weather_coverage, round(coverage, 4),
          f">= {min_weather_coverage}"),
    ]


def to_frame(results: list[CheckResult]) -> pd.DataFrame:
    df = pd.DataFrame([asdict(r) for r in results])
    df["passed"] = df["passed"].astype(int)
    return df


def enforce(results: list[CheckResult]) -> None:
    failed = [r for r in results if r.severity == "critical" and not r.passed]
    if failed:
        msg = "; ".join(f"{r.check_name}={r.observed} (want {r.threshold})" for r in failed)
        raise DataQualityError(f"[{failed[0].source_month}] critical DQ failure: {msg}")
