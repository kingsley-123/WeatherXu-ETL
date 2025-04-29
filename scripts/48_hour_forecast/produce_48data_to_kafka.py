import requests
import pandas as pd  # type: ignore
from datetime import datetime, timezone
from kafka import KafkaProducer  # type: ignore
import json

# API base URL and key
api_base_url = "https://api.weatherxu.com/v1/weather"
api_key = "3c394457b719f808a9c9abfdcc215336"

# kafka configuration
kafka_broker = "broker:29092"
kafka_topic = "48_hour_forecast_weatherxu"

# Define cities and base URL
uk_cities = [
('GBABE', 'Aberdeen', 57.1497, -2.0943, 'Great Britain'),
('GBBFS', 'Belfast', 54.5973, -5.9301, 'Great Britain'),
('GBBIR', 'Birmingham', 52.4800, -1.9025, 'Great Britain'),
('GBBRD', 'Bradford', 53.7960, -1.7594, 'Great Britain'),
('GBBST', 'Bristol', 51.4500, -2.5833, 'Great Britain'),
('GBCRF', 'Cardiff', 51.4800, -3.1800, 'Great Britain'),
('GBCOV', 'Coventry', 52.4081, -1.5106, 'Great Britain'),
('GBDND', 'Dundee', 56.4620, -2.9707, 'Great Britain'),
('GBEDH', 'Edinburgh', 55.9533, -3.1883, 'Great Britain'),
('GBGLG', 'Glasgow', 55.8642, -4.2518, 'Great Britain'),
('GBLDS', 'Leeds', 53.7997, -1.5492, 'Great Britain'),
('GBLCE', 'Leicester', 52.6333, -1.1333, 'Great Britain'),
('GBLIV', 'Liverpool', 53.4084, -2.9916, 'Great Britain'),
('GBLND', 'City', 51.5142, -0.0931, 'Great Britain'),
('GBMAN', 'Manchester', 53.4809, -2.2374, 'Great Britain'),
('GBNET', 'Newcastle upon Tyne', 54.9778, -1.6129, 'Great Britain'),
('GBNGM', 'Nottingham', 52.9536, -1.1505, 'Great Britain'),
('GBPLY', 'Plymouth', 50.3714, -4.1422, 'Great Britain'),
('GBPOR', 'Portsmouth', 50.8058, -1.0872, 'Great Britain'),
('GBSHF', 'Sheffield', 53.3811, -1.4701, 'Great Britain'),
('GBSTH', 'Southampton', 50.9000, -1.4000, 'Great Britain'),
('GBSTE', 'Stoke-on-Trent', 53.0000, -2.1833, 'Great Britain'),
('GBWSM', 'Westminster', 51.5000, -0.1333, 'Great Britain')
]

def fetch_48_hour_weather(city):
    """
    Fetch 48-hour weather forecast data for a given city.
    
    Args:
        city (dict): Dictionary containing city information with 'lat' and 'lon' keys
        
    Returns:
        list: List of hourly weather forecasts or None if request fails
    """
    params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "api_key": api_key,
        "units": "metric",
        "parts": "hourly",
    }

    try:
        response = requests.get(api_base_url, params=params)
        response.raise_for_status()
        data = response.json()
        weather_48 = data['data']['hourly']['data']
        return weather_48
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {city['city']}: {e}")
        return None

def produce_to_kafka(producer, topic, key, value):
    """
    Produce a message to a Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic (str): Name of the Kafka topic
        key (str): Message key
        value (dict): Message value
    """
    try:
        producer.send(topic=topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(f"Error producing message to Kafka: {e}")

def weather_48data():
    """
    Main function to gather weather data and produce it to Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    for city in uk_cities:
        hourly_forecasts = fetch_48_hour_weather(city)
        if hourly_forecasts is None:
            continue

        # Process each hour in the 48-hour forecast
        for hour_data in hourly_forecasts:
            # Convert timestamp to datetime
            formatted_datetime = datetime.fromtimestamp(
                hour_data["forecastStart"], 
                tz=timezone.utc
            ).strftime('%Y-%m-%d %H:%M:%S')

            message = {
                "city": city["city"],
                "temperature": hour_data.get("temperature"),
                "humidity": hour_data.get("humidity"),
                "wind_speed": hour_data.get("windSpeed"),
                "pressure": hour_data.get("pressure"),
                "precip_intensity": hour_data.get("precipIntensity"),
                "visibility": hour_data.get("visibility"),
                "uv_index": hour_data.get("uvIndex"),
                "cloud_cover": hour_data.get("cloudCover"),
                "dew_point": hour_data.get("dewPoint"),
                "condition": hour_data.get("icon"),
                "datetime": formatted_datetime,
            }

            produce_to_kafka(
                producer=producer,
                topic=kafka_topic,
                key=city["city"],
                value=message,
            )

    print("48 hour forecast weather data produced to Kafka successfully!")


weather_48data()