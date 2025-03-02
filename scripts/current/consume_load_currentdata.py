import psycopg2  # type: ignore
from psycopg2.extras import execute_batch  # type: ignore
import pandas as pd  # Ensure pandas is imported
from datetime import datetime, timezone
from kafka import KafkaConsumer
import json

# PostgreSQL Connection Parameters
host = "data-postgres" 
port = "5432"
dbname = "datahub"
user = "airflow"
password = "airflow"

def consume_weather_data():
    messages = []
    
    consumer = KafkaConsumer(
        'current_weatherxu',
        bootstrap_servers=['broker:29092'], 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',  # Start from the latest message
        consumer_timeout_ms=600000   # Timeout after 10 minutes if no new message
    )
    
    for message in consumer:
        messages.append(message.value)
        
        if len(messages) >= 232:
            break
    
    # Return all 232 records as a DataFrame
    df = pd.DataFrame(messages)
    
    return df

def load_date_data():
    date = consume_weather_data()
    date['datetime'] = pd.to_datetime(date['datetime'], unit='s', utc=True)
    date_df = date[['datetime']].drop_duplicates()
    date_df['datetime'] = pd.to_datetime(date_df['datetime'])
    date_df['date'] = date_df['datetime'].dt.date
    date_df['hour_minute'] = date_df['datetime'].dt.strftime('%H:%M')
    date_df['day_of_week'] = date_df['datetime'].dt.day_name()

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        # Use CASCADE to truncate dependent tables
        cursor.execute("TRUNCATE TABLE weatherxu_current.dim_date CASCADE;")

        # Reset the sequence for the fact_weather table
        cursor.execute("ALTER SEQUENCE weatherxu_current.dim_date_date_id_seq RESTART WITH 1;")
        
        # Prepare data for batch insertion
        date_data = [
            (row['datetime'], row['date'], row['hour_minute'], row['day_of_week'])
            for _, row in date_df.iterrows()
        ]

        # Insert query without conflict handling
        query = """
            INSERT INTO weatherxu_current.dim_date (datetime, date, hour_minute, day_of_week)
            VALUES (%s, %s, %s, %s);
        """

        # Execute batch insert
        execute_batch(cursor, query, date_data)
        conn.commit()
        print("Date data loaded successfully, existing data overwritten.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_condition_data():
    condition_df = consume_weather_data() 
    condition_df = condition_df[['condition']].drop_duplicates(subset='condition')

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        # Use CASCADE to truncate dependent tables
        cursor.execute("TRUNCATE TABLE weatherxu_current.dim_condition CASCADE;")

        # Reset the sequence for the fact_weather table
        cursor.execute("ALTER SEQUENCE weatherxu_current.dim_condition_condition_id_seq RESTART WITH 1;")
        
        # Prepare data for batch insertion (only condition column)
        condition_data = [(row['condition'],) for _, row in condition_df.iterrows()]

        # Query for insertion
        query = """
            INSERT INTO weatherxu_current.dim_condition (condition_name)
            VALUES (%s);
        """

        # Execute batch insert
        execute_batch(cursor, query, condition_data)
        conn.commit()
        print("Condition data loaded successfully, existing data overwritten.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_currentweather_data():
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cursor = conn.cursor()

    try:
        # Use CASCADE to truncate dependent tables
        cursor.execute("TRUNCATE TABLE weatherxu_current.fact_weather CASCADE;")

        # Reset the sequence for the fact_weather table
        cursor.execute("ALTER SEQUENCE weatherxu_current.fact_weather_id_seq RESTART WITH 1;")
        
        current_df = consume_weather_data() 

        cursor.execute("SELECT city_id, city_code, city_name, latitude, longitude, country FROM weatherxu_current.dim_city")
        city_mapping = {row[2]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT condition_id, condition_name FROM weatherxu_current.dim_condition")
        condition_mapping = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT date_id, date FROM weatherxu_current.dim_date")
        date_mapping = {row[1]: row[0] for row in cursor.fetchall()}

        weather_data = []
        for _, row in current_df.iterrows():
            city_id = city_mapping.get(row['city'], None)  
            condition_id = condition_mapping.get(row['condition'], None)  
            date_id = date_mapping.get(pd.to_datetime(row['datetime']).date(), None)  
            weather_data.append(( 
               city_id, date_id, condition_id, row['temperature'], row['humidity'],
               row['wind_speed'], row['pressure'], row['precip_intensity'], row['visibility'], 
               row['uv_index'], row['cloud_cover'], row['dew_point']
            ))

        query = """
            INSERT INTO weatherxu_current.fact_weather (
                city_id, date_id, condition_id, temperature, humidity, wind_speed, pressure, 
                precip_intensity, visibility, uv_index, cloud_cover, dew_point
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                condition_id = EXCLUDED.condition_id,
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                wind_speed = EXCLUDED.wind_speed,
                pressure = EXCLUDED.pressure,
                precip_intensity = EXCLUDED.precip_intensity,
                visibility = EXCLUDED.visibility,
                uv_index = EXCLUDED.uv_index,
                cloud_cover = EXCLUDED.cloud_cover,
                dew_point = EXCLUDED.dew_point;
        """

        execute_batch(cursor, query, weather_data)
        conn.commit()
        print("Current weather data loaded successfully, existing data overwritten.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Load the data
load_date_data()
load_condition_data()
load_currentweather_data()
