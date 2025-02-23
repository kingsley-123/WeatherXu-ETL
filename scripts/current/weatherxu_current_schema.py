# PLEASE RUN THIS SCRIPT ONLY ONCE.

import psycopg2  # type: ignore
from psycopg2.extras import execute_batch  # type: ignore

# PostgreSQL Connection Parameters
host = "localhost"
port = "5433"
dbname = "datahub"
user = "airflow"
password = "airflow"

# Database connection
conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
cursor = conn.cursor()

# Drop the weatherxu_current schema if it exists
cursor.execute("DROP SCHEMA IF EXISTS weatherxu_current CASCADE;")
cursor.execute("CREATE SCHEMA weatherxu_current;")
print("Schema 'weatherxu_current' dropped and recreated.")

# Create the dim_city table
create_dim_city_table = """
CREATE TABLE weatherxu_current.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL
);
"""
cursor.execute(create_dim_city_table)
print("Table 'weatherxu_current.dim_city' created.")

# Create the dim_date table
create_dim_date_table = """
CREATE TABLE weatherxu_current.dim_date (
    date_id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour_minute VARCHAR(10) NOT NULL,
    day_of_week VARCHAR(10) NOT NULL
);
"""
cursor.execute(create_dim_date_table)
print("Table 'weatherxu_current.dim_date' created.")

# Create the condition_dim table
create_dim_condition_table = """
CREATE TABLE weatherxu_current.dim_condition (
    condition_id SERIAL PRIMARY KEY,
    condition_name VARCHAR(50) NOT NULL
);
"""
cursor.execute(create_dim_condition_table)
print("Table 'weatherxu_current.dim_condition' created.")

# Create the weather_fact table
create_fact_weather_table = """
CREATE TABLE weatherxu_current.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES weatherxu_current.dim_city(city_id),
    date_id INT REFERENCES weatherxu_current.dim_date(date_id),
    condition_id INT REFERENCES weatherxu_current.dim_condition(condition_id),
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    pressure FLOAT,
    precip_intensity FLOAT,
    visibility FLOAT,
    uv_index FLOAT,
    cloud_cover FLOAT,
    dew_point FLOAT
);
"""
cursor.execute(create_fact_weather_table)
print("Table 'weatherxu_current.fact_weather' created.")

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("All tables created successfully!")