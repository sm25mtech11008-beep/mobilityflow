"""Build a self-contained HTML report from the gold marts."""
from __future__ import annotations

import base64
import html
import io
import logging

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from .config import Settings  # noqa: E402
from .warehouse import Warehouse  # noqa: E402

log = logging.getLogger(__name__)

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _style(ax, title: str, xlabel: str = "", ylabel: str = ""):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10, fontweight="bold")
    ax.set_xlabel(xlabel, color=INK2, fontsize=9)
    ax.set_ylabel(ylabel, color=INK2, fontsize=9)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def _png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _table(df: pd.DataFrame) -> str:
    return df.to_html(index=False, classes="tbl", border=0, float_format=lambda x: f"{x:,.2f}")


def build_report(wh: Warehouse, s: Settings) -> str:
    q = wh.query_df
    hourly = q("SELECT day_type, pickup_hour, SUM(avg_trips_per_day) AS trips_per_day, "
               "SUM(avg_speed_mph * trips) * 1.0 / SUM(trips) AS speed "
               "FROM mart_hourly_demand GROUP BY day_type, pickup_hour ORDER BY pickup_hour")
    weather = q("SELECT * FROM mart_weather_impact")
    order = {"dry": 0, "light_rain": 1, "heavy_rain": 2}
    weather = weather[weather.precip_bucket.isin(order)].sort_values("precip_bucket", key=lambda c: c.map(order))
    emis = q("SELECT borough, SUM(est_co2_tonnes) AS co2_t, SUM(trips) AS trips, "
             "SUM(vehicle_miles) AS miles FROM mart_borough_emissions "
             "WHERE borough NOT IN ('Unknown','N/A') GROUP BY borough ORDER BY co2_t")
    hotspots = q("SELECT zone_name, borough, peak_trips, avg_peak_speed_mph, avg_peak_duration_min "
                 "FROM mart_congestion_hotspots ORDER BY avg_peak_speed_mph LIMIT 10")
    reasons = q("SELECT reject_reason, COUNT(*) AS rows_quarantined FROM silver_trips_rejected "
                "GROUP BY reject_reason ORDER BY rows_quarantined DESC")
    dq = q("SELECT source_month, check_name, severity, CASE WHEN passed=1 THEN 'PASS' ELSE 'FAIL' END AS result, "
           "observed, threshold FROM dq_results WHERE run_id = (SELECT MAX(run_id) FROM dq_results) "
           "ORDER BY source_month, check_name")
    state = q("SELECT source_month, status, clean_rows, rejected_rows, updated_at FROM pipeline_state "
              "ORDER BY source_month")

    total_trips = int(wh.scalar("SELECT COUNT(*) FROM fact_trips"))
    total_rej = int(wh.scalar("SELECT COUNT(*) FROM silver_trips_rejected"))
    co2_t = float(wh.scalar("SELECT SUM(co2_kg) FROM fact_trips") or 0) / 1000
    peak_speed = hourly[(hourly.day_type == "weekday") & hourly.pickup_hour.between(16, 19)]["speed"].mean()
    night_speed = hourly[(hourly.day_type == "weekday") & hourly.pickup_hour.between(0, 4)]["speed"].mean()

    charts = []
    # 1. Demand curve
    fig, ax = plt.subplots(figsize=(7.5, 3.2))
    for dt, color in (("weekday", BLUE), ("weekend", ORANGE)):
        d = hourly[hourly.day_type == dt]
        ax.plot(d.pickup_hour, d.trips_per_day, color=color, linewidth=2, label=dt.capitalize())
        last = d.iloc[-1]
        ax.annotate(dt.capitalize(), (last.pickup_hour, last.trips_per_day), xytext=(6, 0),
                    textcoords="offset points", va="center", fontsize=8, color=INK2)
    ax.set_xticks(range(0, 24, 3))
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    _style(ax, "Average trips per day by pickup hour", "Hour of day", "Trips / day")
    charts.append(("Demand rhythm", _png(fig)))

    # 2. Speed by hour (single series)
    fig, ax = plt.subplots(figsize=(7.5, 3.0))
    d = hourly[hourly.day_type == "weekday"]
    ax.plot(d.pickup_hour, d.speed, color=BLUE, linewidth=2, marker="o", markersize=4)
    ax.set_xticks(range(0, 24, 3))
    _style(ax, "Weekday average trip speed by hour (mph)", "Hour of day", "mph")
    charts.append(("Congestion profile", _png(fig)))

    # 3. Weather impact: two measures -> two panels, never a dual axis
    fig, axes = plt.subplots(1, 2, figsize=(7.5, 3.0))
    labels = [b.replace("_", " ") for b in weather.precip_bucket]
    for ax, col, title in ((axes[0], "trips_per_hour", "Trips per hour"),
                           (axes[1], "avg_speed_mph", "Average speed (mph)")):
        bars = ax.bar(labels, weather[col], color=AQUA, width=0.55, edgecolor=SURFACE, linewidth=2)
        ax.bar_label(bars, fmt="%.1f", fontsize=8, color=INK2, padding=2)
        _style(ax, title)
    fig.tight_layout()
    charts.append(("Weather impact", _png(fig)))

    # 4. Emissions by borough
    fig, ax = plt.subplots(figsize=(7.5, 2.8))
    bars = ax.barh(emis.borough, emis.co2_t, color=BLUE, height=0.55, edgecolor=SURFACE, linewidth=2)
    ax.bar_label(bars, fmt="%.1f t", fontsize=8, color=INK2, padding=3)
    _style(ax, f"Estimated tailpipe CO₂ by pickup borough (tonnes, @{s.co2_g_per_mile:.0f} g/mi)")
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.grid(axis="y", visible=False)
    charts.append(("Emissions", _png(fig)))

    tiles = [
        ("Clean trips", f"{total_trips:,}"),
        ("Quarantined rows", f"{total_rej:,} ({100 * total_rej / max(1, total_trips + total_rej):.1f}%)"),
        ("Est. CO₂", f"{co2_t:,.1f} t"),
        ("Evening peak vs night speed", f"{peak_speed:.1f} vs {night_speed:.1f} mph"),
    ]
    page = f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MobilityFlow Report</title>
<style>
:root{{--bg:#f9f9f7;--card:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--line:#e4e3df}}
body{{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}}
main{{max-width:900px;margin:0 auto;padding:24px 16px 48px}}
h1{{font-size:24px;margin:0 0 4px}} h2{{font-size:16px;margin:32px 0 8px}}
.sub{{color:var(--ink2);margin:0 0 20px}}
.tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.tile{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px}}
.tile b{{display:block;font-size:20px}} .tile span{{color:var(--ink2);font-size:12px}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;margin-top:12px;overflow-x:auto}}
img{{max-width:100%;height:auto;display:block}}
.tbl{{border-collapse:collapse;width:100%;font-size:12px}}
.tbl th{{text-align:left;color:var(--ink2);font-weight:600;border-bottom:1px solid var(--line);padding:6px 8px}}
.tbl td{{border-bottom:1px solid var(--line);padding:6px 8px}}
</style></head><body><main>
<h1>MobilityFlow &mdash; pipeline report</h1>
<p class="sub">Source: {html.escape("synthetic data" if s.extra.get("source") == "synthetic" else f"NYC TLC {s.dataset} taxi + Open-Meteo")}
 &middot; engine: {wh.engine} &middot; months: {", ".join(state.source_month)}</p>
<div class="tiles">{"".join(f'<div class="tile"><span>{k}</span><b>{v}</b></div>' for k, v in tiles)}</div>
{"".join(f'<h2>{t}</h2><div class="card"><img alt="{t}" src="data:image/png;base64,{b}"></div>' for t, b in charts)}
<h2>Congestion hotspots (weekday peaks, slowest 10 zones)</h2><div class="card">{_table(hotspots)}</div>
<h2>Data quality &mdash; latest run</h2><div class="card">{_table(dq)}</div>
<h2>Quarantined rows by reason</h2><div class="card">{_table(reasons)}</div>
<h2>Load state (watermark)</h2><div class="card">{_table(state)}</div>
</main></body></html>"""
    out = s.report_dir / "report.html"
    out.write_text(page)
    log.info("Report written: %s", out)
    return str(out)
