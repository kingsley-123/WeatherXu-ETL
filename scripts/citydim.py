# Please only run this file once because the data is static

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

# SQL to create the city_dim table
create_schema = """
CREATE SCHEMA IF NOT EXISTS weatherxu
"""

# Execute the SQL to create the table
cursor.execute(create_schema)

# SQL to create the city_dim table
create_citydim_table = """
CREATE TABLE IF NOT EXISTS weatherxu.city_dim (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);
"""

# Execute the SQL to create the table
cursor.execute(create_citydim_table)

# Insert sample data into the city_dim table
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

# SQL to insert data into the city_dim table
insert_sql = """
INSERT INTO weatherxu.city_dim (city_name, latitude, longitude)
VALUES (%s, %s, %s);
"""

# Use execute_batch to insert all rows at once
execute_batch(cursor, insert_sql, cities)  

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Table created and data inserted successfully using execute_batch!")