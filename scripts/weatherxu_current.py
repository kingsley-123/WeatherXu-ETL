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
    city_name VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
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
    hour INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
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
    dew_point FLOAT,
    PRIMARY KEY (city_id, date_id)
);
"""
cursor.execute(create_fact_weather_table)
print("Table 'weatherxu_current.fact_weather' created.")

# Insert sample data into the dim_city table
cities = [
    ('London', 51.5074, -0.1278),
    ('Birmingham', 52.4862, -1.8904),
    ('Manchester', 53.4808, -2.2426),
    ('Glasgow', 55.8642, -4.2518),
    ('Liverpool', 53.4084, -2.9916),
    ('Bristol', 51.4545, -2.5879),
    ('Sheffield', 53.3811, -1.4701),
    ('Leeds', 53.8008, -1.5491),
    ('Edinburgh', 55.9533, -3.1883),
    ('Leicester', 52.6369, -1.1398),
    ('Cardiff', 51.4816, -3.1791),
    ('Belfast', 54.5973, -5.9301),
    ('Newcastle', 54.9783, -1.6178),
    ('Brighton', 50.8225, -0.1372),
    ('Nottingham', 52.9548, -1.1581),
    ('Southampton', 50.9097, -1.4044),
    ('Plymouth', 50.3755, -4.1427),
    ('Aberdeen', 57.1497, -2.0943),
    ('Cambridge', 52.2053, 0.1218),
    ('Oxford', 51.7520, -1.2577)
]

insert_city_sql = """
INSERT INTO weatherxu_current.dim_city (city_name, latitude, longitude)
VALUES (%s, %s, %s);
"""
execute_batch(cursor, insert_city_sql, cities)
print("Data inserted into 'weatherxu_current.dim_city' successfully.")

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("All tables created and data inserted successfully!")