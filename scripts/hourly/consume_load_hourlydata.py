import psycopg2  # type: ignore
from psycopg2.extras import execute_batch  # type: ignore
import pandas as pd 
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
    consumer = KafkaConsumer(
        'hourly_weatherxu',
        bootstrap_servers=['broker:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Stop consuming after 10 seconds of no messages
    )
    
    data_list = []
    try:
        for message in consumer:
            data_list.append(message.value)
    except Exception as e:
        print(f"Error consuming Kafka messages: {e}")
    finally:
        consumer.close()
    
    if not data_list:
        return None
    
    # Convert the list of dictionaries to a DataFrame
    return pd.DataFrame(data_list)

def main():
    # Get the data once
    weather_data = consume_weather_data()
    
    if weather_data is None or weather_data.empty:
        print("No data was consumed from Kafka. Exiting.")
        return
    
    # Check required columns
    required_columns = ['datetime', 'condition', 'city', 'temperature', 'humidity', 
                        'wind_speed', 'pressure', 'precip_intensity', 'visibility', 
                        'uv_index', 'cloud_cover', 'dew_point', 'time_of_day']
    
    for col in required_columns:
        if col not in weather_data.columns:
            print(f"Required column '{col}' is missing. Available columns: {weather_data.columns}")
            return
    
    # Process date dimension
    load_date_data(weather_data)
    
    # Process condition dimension
    load_condition_data(weather_data)
    
    # Process weather facts
    load_hourlyweather_data(weather_data)

def load_date_data(df):
    # Convert Unix timestamp to datetime
    df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
    date_df = df[['datetime', 'time_of_day']].drop_duplicates()
    date_df['date'] = date_df['datetime'].dt.date
    date_df['hour_minute'] = date_df['datetime'].dt.strftime('%H:%M')
    date_df['day_of_week'] = date_df['datetime'].dt.day_name()

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        # First ensure the column exists
        cursor.execute("""
            ALTER TABLE weatherxu_hourly.dim_date 
            ADD COLUMN IF NOT EXISTS time_of_day VARCHAR(10);
        """)
        
        # Then truncate and load data
        cursor.execute("TRUNCATE TABLE weatherxu_hourly.dim_date CASCADE;")
        cursor.execute("ALTER SEQUENCE weatherxu_hourly.dim_date_date_id_seq RESTART WITH 1;")
        
        date_data = [
            (row['datetime'], row['date'], row['hour_minute'], row['day_of_week'], row['time_of_day'])
            for _, row in date_df.iterrows()
        ]

        query = """
            INSERT INTO weatherxu_hourly.dim_date 
            (datetime, date, hour_minute, day_of_week, time_of_day)
            VALUES (%s, %s, %s, %s, %s);
        """

        execute_batch(cursor, query, date_data)
        conn.commit()
        print("Date data with time_of_day loaded successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def load_condition_data(df):
    condition_df = df[['condition']].drop_duplicates(subset='condition')
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()

        # Use CASCADE to truncate dependent tables
        cursor.execute("TRUNCATE TABLE weatherxu_hourly.dim_condition CASCADE;")

        # Reset the sequence for the fact_weather table
        cursor.execute("ALTER SEQUENCE weatherxu_hourly.dim_condition_condition_id_seq RESTART WITH 1;")
        
        # Prepare data for batch insertion (only condition column)
        condition_data = [(row['condition'],) for _, row in condition_df.iterrows()]

        # Query for insertion
        query = """
            INSERT INTO weatherxu_hourly.dim_condition (condition_name)
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

def load_hourlyweather_data(df):
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cursor = conn.cursor()
        
        # Use CASCADE to truncate dependent tables
        cursor.execute("TRUNCATE TABLE weatherxu_hourly.fact_weather CASCADE;")

        # Reset the sequence for the fact_weather table
        cursor.execute("ALTER SEQUENCE weatherxu_hourly.fact_weather_id_seq RESTART WITH 1;")
        
        # Use the DataFrame that was already passed in, not calling consume_weather_data() again
        current_df = df.copy()
        
        # Convert datetime to standard datetime format if it's still a Unix timestamp
        if isinstance(current_df['datetime'].iloc[0], (int, float)):
            current_df['datetime'] = pd.to_datetime(current_df['datetime'], unit='s')

        cursor.execute("SELECT city_id, city_code, city_name, latitude, longitude, country FROM weatherxu_hourly.dim_city")
        city_mapping = {row[2]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT condition_id, condition_name FROM weatherxu_hourly.dim_condition")
        condition_mapping = {row[1]: row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT date_id, date FROM weatherxu_hourly.dim_date")
        date_mapping = {row[1]: row[0] for row in cursor.fetchall()}

        weather_data = []
        for _, row in current_df.iterrows():
            city_id = city_mapping.get(row['city'], None)  
            condition_id = condition_mapping.get(row['condition'], None)  
            date_id = date_mapping.get(row['datetime'].date(), None)  
            
            weather_data.append(( 
               city_id, date_id, condition_id, row['temperature'], row['humidity'],
               row['wind_speed'], row['pressure'], row['precip_intensity'], row['visibility'], 
               row['uv_index'], row['cloud_cover'], row['dew_point']
            ))

        query = """
            INSERT INTO weatherxu_hourly.fact_weather (
                city_id, date_id, condition_id, temperature, humidity, wind_speed, pressure, 
                precip_intensity, visibility, uv_index, cloud_cover, dew_point
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        execute_batch(cursor, query, weather_data)
        conn.commit()
        print("hourly weather data loaded successfully, existing data overwritten.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Execute the main function
main()