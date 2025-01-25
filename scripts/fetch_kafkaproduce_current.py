import requests
from datetime import datetime, UTC
import psycopg2  # type: ignore
from confluent_kafka import Producer  # type: ignore
import json

# PostgreSQL connection parameters
postgres_host = "localhost"
postgres_port = "5433"
postgres_db = "datahub"
postgres_user = "airflow"
postgres_password = "airflow"

# API base URL and key
api_base_url = "https://api.weatherxu.com/v1/weather"
api_key = "3c394457b719f808a9c9abfdcc215336"

# Kafka configuration
kafka_broker = "localhost:9092"
kafka_topic = "current_weatherxu"

def fetch_cities_from_db():
    """Fetch city data from the database."""
    conn = psycopg2.connect(host=postgres_host, port=postgres_port, dbname=postgres_db, user=postgres_user, password=postgres_password)
    cursor = conn.cursor()
    cursor.execute("SELECT city_name, latitude, longitude FROM weatherxu_current.dim_city;")
    cities = [{"city": city[0], "lat": city[1], "lon": city[2]} for city in cursor.fetchall()]
    cursor.close()
    conn.close()
    return cities

def fetch_current_weather(city):
    """Fetch current weather data for a city."""
    try:
        response = requests.get(
            api_base_url,
            params={
                "lat": city["lat"],
                "lon": city["lon"],
                "api_key": api_key,
                "units": "metric",
                "parts": "currently",  # Corrected parameter
            },
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        print("API Response for", city["city"], ":", data)  # Log the API response
        if "data" in data and "currently" in data["data"]:  # Corrected key
            return data["data"]["currently"], data["data"]["dt"]  # Return both "currently" and "dt"
        else:
            print(f"Error: 'currently' key not found in API response for city {city['city']}")
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"API request failed for city {city['city']}: {e}")
        return None, None

def produce_to_kafka(producer, topic, key, value):
    """Produce a message to a Kafka topic."""
    try:
        producer.produce(topic=topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(f"Failed to produce message to Kafka: {e}")

def main():
    """Main function to fetch and produce weather data."""
    # Fetch cities from the database
    cities = fetch_cities_from_db()

    # Create a Kafka producer
    producer = Producer({"bootstrap.servers": kafka_broker})

    # Fetch and produce current weather data
    for city in cities:
        current_weather, dt = fetch_current_weather(city)
        if current_weather is None or dt is None:
            continue  # Skip this city if data is invalid

        # Format the datetime
        formatted_datetime = datetime.fromtimestamp(dt, UTC).strftime('%Y-%m-%d %H:%M:%S')

        # Prepare the message
        message = {
            "city": city["city"],
            "temperature": current_weather.get("temperature"),
            "humidity": current_weather.get("humidity"),
            "wind_speed": current_weather.get("windSpeed"),
            "pressure": current_weather.get("pressure"),
            "precip_intensity": current_weather.get("precipIntensity"),
            "visibility": current_weather.get("visibility"),
            "uv_index": current_weather.get("uvIndex"),
            "cloud_cover": current_weather.get("cloudCover"),
            "dew_point": current_weather.get("dewPoint"),
            "condition": current_weather.get("icon"),
            "datetime": formatted_datetime,
        }

        # Produce the message to Kafka
        produce_to_kafka(
            producer=producer,
            topic=kafka_topic,
            key=city["city"].encode("utf-8"),  # Use city name as the key
            value=json.dumps(message).encode("utf-8"),  # Serialize data as JSON
        )

    print("Current weather data produced to Kafka successfully!")

# Call the main function
main()