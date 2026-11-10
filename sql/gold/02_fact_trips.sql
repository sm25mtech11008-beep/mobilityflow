-- Gold: trip fact table at one-row-per-trip grain

DROP TABLE IF EXISTS fact_trips;
CREATE TABLE fact_trips AS
SELECT
    t.trip_id,
    t.pickup_date           AS date_key,
    t.weather_hour_key      AS hour_key,
    t.pickup_hour,
    t.pu_location_id        AS pu_zone_id,
    t.do_location_id        AS do_zone_id,
    t.passenger_count,
    t.trip_distance         AS distance_mi,
    t.duration_min,
    t.avg_speed_mph,
    t.fare_amount,
    t.tip_amount,
    t.tip_pct,
    t.total_amount,
    t.payment_type,
    t.co2_kg
FROM silver_trips t;
