CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL DEFAULT now(),
    feed_rate NUMERIC(8,2),
    water_flow NUMERIC(8,2),
    rpm NUMERIC(6,2),
    power NUMERIC(8,2),
    health_alarm BOOLEAN DEFAULT false
);

INSERT INTO sensor_readings (feed_rate, water_flow, rpm, power, health_alarm)
SELECT
    120 + random() * 40,
    30 + random() * 20,
    9.5 + random() * 2,
    1800 + random() * 700,
    random() < 0.05
FROM generate_series(1, 500);
