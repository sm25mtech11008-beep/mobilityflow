-- Gold: analytics marts answering specific mobility questions

-- Q1. When and where is demand highest, and how fast does traffic move then?
DROP TABLE IF EXISTS mart_hourly_demand;
CREATE TABLE mart_hourly_demand AS
SELECT
    z.borough,
    CASE WHEN d.is_weekend = 1 THEN 'weekend' ELSE 'weekday' END AS day_type,
    f.pickup_hour,
    COUNT(*)                                           AS trips,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT f.date_key), 2) AS avg_trips_per_day,
    ROUND(AVG(f.avg_speed_mph), 2)                     AS avg_speed_mph,
    ROUND(AVG(f.duration_min), 2)                      AS avg_duration_min
FROM fact_trips f
JOIN dim_zone z ON f.pu_zone_id = z.zone_id
JOIN dim_date d ON f.date_key   = d.date_key
GROUP BY z.borough, CASE WHEN d.is_weekend = 1 THEN 'weekend' ELSE 'weekday' END, f.pickup_hour;

-- Q2. How does rain change demand, speed and tipping?
DROP TABLE IF EXISTS mart_weather_impact;
CREATE TABLE mart_weather_impact AS
SELECT
    w.precip_bucket,
    COUNT(DISTINCT f.hour_key)                         AS hours_observed,
    COUNT(*)                                           AS trips,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT f.hour_key), 2) AS trips_per_hour,
    ROUND(AVG(f.avg_speed_mph), 2)                     AS avg_speed_mph,
    ROUND(AVG(f.duration_min), 2)                      AS avg_duration_min,
    ROUND(AVG(f.tip_pct), 2)                           AS avg_tip_pct
FROM fact_trips f
JOIN dim_weather_hour w ON f.hour_key = w.hour_key
GROUP BY w.precip_bucket;

-- Q3. Estimated tailpipe CO2 by borough and month (climate lens)
DROP TABLE IF EXISTS mart_borough_emissions;
CREATE TABLE mart_borough_emissions AS
SELECT
    d.year_month,
    z.borough,
    COUNT(*)                                   AS trips,
    ROUND(SUM(f.distance_mi), 1)               AS vehicle_miles,
    ROUND(SUM(f.co2_kg) / 1000.0, 3)           AS est_co2_tonnes,
    ROUND(AVG(f.co2_kg), 3)                    AS avg_co2_kg_per_trip,
    ROUND(SUM(CASE WHEN f.distance_mi < 1.5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                                               AS pct_short_trips_under_1_5mi
FROM fact_trips f
JOIN dim_zone z ON f.pu_zone_id = z.zone_id
JOIN dim_date d ON f.date_key   = d.date_key
GROUP BY d.year_month, z.borough;

-- Q4. Congestion hotspots: slowest pickup zones in weekday peak hours
DROP TABLE IF EXISTS mart_congestion_hotspots;
CREATE TABLE mart_congestion_hotspots AS
SELECT
    z.zone_id,
    z.zone_name,
    z.borough,
    COUNT(*)                          AS peak_trips,
    ROUND(AVG(f.avg_speed_mph), 2)    AS avg_peak_speed_mph,
    ROUND(AVG(f.duration_min), 2)     AS avg_peak_duration_min
FROM fact_trips f
JOIN dim_zone z ON f.pu_zone_id = z.zone_id
JOIN dim_date d ON f.date_key   = d.date_key
WHERE d.is_weekend = 0
  AND (f.pickup_hour BETWEEN 7 AND 9 OR f.pickup_hour BETWEEN 16 AND 19)
GROUP BY z.zone_id, z.zone_name, z.borough
HAVING COUNT(*) >= 50;

-- Q5. Origin-destination flows between boroughs
DROP TABLE IF EXISTS mart_od_flows;
CREATE TABLE mart_od_flows AS
SELECT
    zp.borough                        AS origin_borough,
    zd.borough                        AS destination_borough,
    COUNT(*)                          AS trips,
    ROUND(AVG(f.distance_mi), 2)      AS avg_distance_mi,
    ROUND(AVG(f.total_amount), 2)     AS avg_total_usd
FROM fact_trips f
JOIN dim_zone zp ON f.pu_zone_id = zp.zone_id
JOIN dim_zone zd ON f.do_zone_id = zd.zone_id
GROUP BY zp.borough, zd.borough;
