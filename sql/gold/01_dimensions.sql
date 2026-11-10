-- Gold: conformed dimensions (portable SQL: runs on DuckDB and SQLite)

DROP TABLE IF EXISTS dim_zone;
CREATE TABLE dim_zone AS
SELECT location_id AS zone_id, borough, zone AS zone_name, service_zone
FROM silver_zones;

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date AS
SELECT DISTINCT
    pickup_date                                   AS date_key,
    source_month                                  AS year_month,
    day_of_week,
    CASE day_of_week WHEN 0 THEN 'Mon' WHEN 1 THEN 'Tue' WHEN 2 THEN 'Wed'
                     WHEN 3 THEN 'Thu' WHEN 4 THEN 'Fri' WHEN 5 THEN 'Sat'
                     ELSE 'Sun' END               AS day_name,
    is_weekend
FROM silver_trips;

DROP TABLE IF EXISTS dim_weather_hour;
CREATE TABLE dim_weather_hour AS
SELECT hour_key, temperature_c, precip_mm, wind_kmh, precip_bucket, temp_bucket
FROM silver_weather;
