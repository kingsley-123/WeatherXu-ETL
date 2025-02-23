import psycopg2  # type: ignore
from psycopg2.extras import execute_batch  # type: ignore
import pandas as pd  # Ensure pandas is imported
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
        '48_hour_forecast_weatherxu',
        bootstrap_servers=['broker:29092'], 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=120000
    )
    
    for message in consumer:
        messages.append(message.value)
        
        if len(messages) >= 20:
            break
    
    df = pd.DataFrame(messages)
    
    return df

def load_date_data():
    date = consume_weather_data()
    date_df = date[['datetime']].drop_duplicates()
    date_df['datetime'] = pd.to_datetime(date_df['datetime'])
    date_df['date'] = date_df['datetime'].dt.date
    date_df['hour_minute'] = date_df['datetime'].dt.strftime('%H:%M')
    date_df['day_of_week'] = date_df['datetime'].dt.day_name()

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        # Prepare data for batch insertion
        date_data = [
            (row['datetime'], row['date'], row['hour_minute'], row['day_of_week'])
            for _, row in date_df.iterrows()
        ]

        # Insert query with conflict handling
        query = """
            INSERT INTO weatherxu_48.dim_date (datetime, date, hour_minute, day_of_week)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date_id) DO UPDATE 
            SET datetime = EXCLUDED.datetime,
                date = EXCLUDED.date,
                hour_minute = EXCLUDED.hour_minute,
                day_of_week = EXCLUDED.day_of_week;
        """

        # Execute batch insert
        execute_batch(cursor, query, date_data)
        conn.commit()
        print("date data loaded successfully")

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

        # Prepare data for batch insertion (only city column)
        condition_data = [(row['condition'],) for _, row in condition_df.iterrows()]

        # Query for insertion
        query = """
            INSERT INTO weatherxu_48.dim_condition (condition_name)
            VALUES (%s);
        """

        # Execute batch insert
        execute_batch(cursor, query, condition_data)
        conn.commit()
        print("condition data loaded successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_city_data():
    city_df = consume_weather_data() 
    city_df = city_df[['city']].drop_duplicates(subset='city')

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        # Prepare data for batch insertion (only city column)
        city_data = [(row['city'],) for _, row in city_df.iterrows()]

        # Query for insertion
        query = """
            INSERT INTO weatherxu_48.dim_city (city_name)
            VALUES (%s);
        """

        # Execute batch insert
        execute_batch(cursor, query, city_data)
        conn.commit()
        print("city data loaded successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_48weather_data():
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cursor = conn.cursor()

    try:
        current_df = consume_weather_data() 

        cursor.execute("SELECT city_id, city_name FROM weatherxu_48.dim_city")
        city_mapping = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT condition_id, condition_name FROM weatherxu_48.dim_condition")
        condition_mapping = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT date_id, date FROM weatherxu_48.dim_date")
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
            INSERT INTO weatherxu_48.fact_weather (
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
        print("current data loaded successfully")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

load_date_data()
load_condition_data()
load_city_data()
load_48weather_data()