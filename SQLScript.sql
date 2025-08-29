Create table WeatherDataBronze(
city VARCHAR(255),
timestamp TIMESTAMP,
temperature DOUBLE PRECISION,
humidity INTEGER,
pressure INTEGER,
wind_speed DOUBLE PRECISION,
weather VARCHAR(255)
);

Create table WeatherDataSilver(
city_id INTEGER,
row_id INTEGER,
city VARCHAR(255),
timestamp TIMESTAMP,
temperature DOUBLE PRECISION,
humidity INTEGER,
pressure INTEGER,
weather VARCHAR(255),
"month" INTEGER,
"year" INTEGER,
wind_speed DOUBLE PRECISION
);

Create table WeatherDataGold(
city VARCHAR(255),
"year" INTEGER,
"month" INTEGER,
avg_temperature DOUBLE PRECISION,
avg_humidity DOUBLE PRECISION,
avg_pressure DOUBLE PRECISION,
avg_wind_speed DOUBLE PRECISION,
stddev_temperature DOUBLE PRECISION,
z_score_temperature DOUBLE PRECISION,
is_anomaly_zscore VARCHAR(255),
is_anomaly_rule_based VARCHAR(255),
final_anomaly_flag VARCHAR(255)
);


