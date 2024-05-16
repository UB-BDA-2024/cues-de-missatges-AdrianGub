-- transactional: false
CREATE MATERIALIZED VIEW table_month AS
SELECT time_bucket('1 month', last_seen) AS bucket,
    sensor_id,
    AVG(temperature) AS temperature,
    AVG(humidity) AS humidity,
    AVG(velocity) AS velocity,
    AVG(battery_level) AS battery_level
FROM sensor_data
GROUP BY bucket, sensor_id;