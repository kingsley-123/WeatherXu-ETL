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

create_dim_city_table = """
CREATE TABLE weatherxu_current.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_code VARCHAR(100),
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    country VARCHAR(100)
);
"""

cursor.execute(create_dim_city_table)
print("Table 'weatherxu_current.dim_city' created.")

# Data to insert into the table
city_data = [
    ('GBBIR', 'Birmingham', 52.4862, -1.8904, 'Great Britain'),
    ('GBLND', 'London', 51.5074, -0.1278, 'Great Britain'),
    ('GBMAN', 'Manchester', 53.4808, -2.2426, 'Great Britain'),
    ('GBGLG', 'Glasgow', 55.8642, -4.2518, 'Great Britain'),
    ('GBLIV', 'Liverpool', 53.4084, -2.9916, 'Great Britain'),
    ('GBBST', 'Bristol', 51.4545, -2.5879, 'Great Britain'),
    ('GBSHF', 'Sheffield', 53.3811, -1.4701, 'Great Britain'),
    ('GBLDS', 'Leeds', 53.8008, -1.5491, 'Great Britain'),
    ('GBEDH', 'Edinburgh', 55.9533, -3.1883, 'Great Britain'),
    ('GBLCE', 'Leicester', 52.6369, -1.1398, 'Great Britain')
]

# Inserting the data into the table
insert_query = """
INSERT INTO weatherxu_current.dim_city (city_code, city_name, latitude, longitude, country)
VALUES (%s, %s, %s, %s, %s);
"""

for city in city_data:
    cursor.execute(insert_query, city)
    
print("Data inserted into 'weatherxu_current.dim_city' table.")

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