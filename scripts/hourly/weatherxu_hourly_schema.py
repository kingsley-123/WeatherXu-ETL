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

# Drop the weatherxu_hourly schema if it exists
cursor.execute("DROP SCHEMA IF EXISTS weatherxu_hourly CASCADE;")
cursor.execute("CREATE SCHEMA weatherxu_hourly;")
print("Schema 'weatherxu_hourly' dropped and recreated.")

create_dim_city_table = """
CREATE TABLE weatherxu_hourly.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_code VARCHAR(100),
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    country VARCHAR(100)
);
"""

cursor.execute(create_dim_city_table)
print("Table 'weatherxu_hourly.dim_city' created.")

# Data to insert into the table
us_states = [
    ('AL', 'Alabama', 32.3668, -86.3000, 'United States'),
    ('AK', 'Alaska', 61.2181, -149.9003, 'United States'),
    ('AZ', 'Arizona', 34.0489, -111.0937, 'United States'),
    ('AR', 'Arkansas', 34.7465, -92.2896, 'United States'),
    ('CA', 'California', 36.7783, -119.4179, 'United States'),
    ('CO', 'Colorado', 39.5501, -105.7821, 'United States'),
    ('CT', 'Connecticut', 41.6032, -73.0877, 'United States'),
    ('DE', 'Delaware', 39.1582, -75.5244, 'United States'),
    ('DC', 'District of Columbia', 38.9072, -77.0369, 'United States'),
    ('FL', 'Florida', 30.4383, -84.2807, 'United States'),
    ('GA', 'Georgia', 33.7490, -84.3880, 'United States'),
    ('GU', 'Guam', 13.4443, 144.7937, 'United States'),
    ('HI', 'Hawaii', 21.3069, -157.8583, 'United States'),
    ('ID', 'Idaho', 44.0682, -114.7420, 'United States'),
    ('IL', 'Illinois', 39.7817, -89.6501, 'United States'),
    ('IN', 'Indiana', 39.7684, -86.1581, 'United States'),
    ('IA', 'Iowa', 41.8780, -93.0977, 'United States'),
    ('KS', 'Kansas', 39.0119, -98.4842, 'United States'),
    ('KY', 'Kentucky', 38.2527, -85.7585, 'United States'),
    ('LA', 'Louisiana', 30.9843, -91.9623, 'United States'),
    ('ME', 'Maine', 44.3106, -69.7795, 'United States'),
    ('MD', 'Maryland', 39.0458, -76.6413, 'United States'),
    ('MA', 'Massachusetts', 42.4072, -71.3824, 'United States'),
    ('MI', 'Michigan', 44.3148, -85.6024, 'United States'),
    ('MN', 'Minnesota', 46.7296, -94.6859, 'United States'),
    ('MS', 'Mississippi', 32.3547, -89.3985, 'United States'),
    ('MO', 'Missouri', 38.5767, -92.1735, 'United States'),
    ('MT', 'Montana', 46.8797, -110.3626, 'United States'),
    ('NE', 'Nebraska', 41.4925, -99.9018, 'United States'),
    ('NV', 'Nevada', 38.8026, -116.4194, 'United States'),
    ('NH', 'New Hampshire', 43.2081, -71.5376, 'United States'),
    ('NJ', 'New Jersey', 40.0583, -74.4057, 'United States'),
    ('NM', 'New Mexico', 34.5199, -105.8701, 'United States'),
    ('NY', 'New York', 40.7128, -74.0060, 'United States'),
    ('NC', 'North Carolina', 35.7822, -80.7935, 'United States'),
    ('ND', 'North Dakota', 47.5515, -101.0020, 'United States'),
    ('MP', 'Northern Mariana Islands', 15.0979, 145.6739, 'United States'),
    ('OH', 'Ohio', 39.9612, -82.9988, 'United States'),
    ('OK', 'Oklahoma', 35.4676, -97.5164, 'United States'),
    ('OR', 'Oregon', 43.8041, -120.5542, 'United States'),
    ('PA', 'Pennsylvania', 40.2732, -76.8867, 'United States'),
    ('PR', 'Puerto Rico', 18.2208, -66.5901, 'United States'),
    ('RI', 'Rhode Island', 41.8240, -71.4128, 'United States'),
    ('SC', 'South Carolina', 34.0007, -81.0348, 'United States'),
    ('SD', 'South Dakota', 43.9695, -99.9018, 'United States'),
    ('TN', 'Tennessee', 36.1627, -86.7816, 'United States'),
    ('TX', 'Texas', 31.9686, -99.9018, 'United States'),
    ('VI', 'Virgin Islands', 18.3358, -64.8963, 'United States'),
    ('VT', 'Vermont', 44.2601, -72.5754, 'United States'),
    ('VA', 'Virginia', 37.4316, -78.6569, 'United States'),
    ('WA', 'Washington', 47.7511, -120.7401, 'United States'),
    ('WV', 'West Virginia', 38.5976, -80.4549, 'United States'),
    ('WI', 'Wisconsin', 43.7844, -88.7879, 'United States'),
    ('WY', 'Wyoming', 43.0759, -107.2903, 'United States')
]

# Inserting the data into the table
insert_query = """
INSERT INTO weatherxu_hourly.dim_city (city_code, city_name, latitude, longitude, country)
VALUES (%s, %s, %s, %s, %s);
"""

for city in us_states:
    cursor.execute(insert_query, city)
    
print("Data inserted into 'weatherxu_hourly.dim_city' table.")

# Create the dim_date table
create_dim_date_table = """
CREATE TABLE IF NOT EXISTS weatherxu_hourly.dim_date (
    date_id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour_minute VARCHAR(10) NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    time_of_day VARCHAR(10) NOT NULL  
);
"""
cursor.execute(create_dim_date_table)
print("Table 'weatherxu_hourly.dim_date' created.")

# Create the condition_dim table
create_dim_condition_table = """
CREATE TABLE weatherxu_hourly.dim_condition (
    condition_id SERIAL PRIMARY KEY,
    condition_name VARCHAR(50) NOT NULL
);
"""
cursor.execute(create_dim_condition_table)
print("Table 'weatherxu_hourly.dim_condition' created.")

# Create the weather_fact table
create_fact_weather_table = """
CREATE TABLE weatherxu_hourly.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES weatherxu_hourly.dim_city(city_id),
    date_id INT REFERENCES weatherxu_hourly.dim_date(date_id),
    condition_id INT REFERENCES weatherxu_hourly.dim_condition(condition_id),
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
print("Table 'weatherxu_hourly.fact_weather' created.")

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("All tables created successfully!")