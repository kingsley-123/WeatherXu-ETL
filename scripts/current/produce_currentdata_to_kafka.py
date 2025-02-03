import requests
from datetime import datetime, timezone
import psycopg2  # type: ignore
from kafka import KafkaProducer  # type: ignore
import json

# API base URL and key
api_base_url = "https://api.weatherxu.com/v1/weather"
api_key = "3c394457b719f808a9c9abfdcc215336"

# kafka configuration
kafka_broker = "broker:29092"
kafka_topic = "current_weatherxu"

# define cities
uk_cities = [
    {"city": "London", "lat": 51.5074, "lon": -0.1278},
    {"city": "Birmingham", "lat": 52.4862, "lon": -1.8904},
    {"city": "Manchester", "lat": 53.4808, "lon": -2.2426},
    {"city": "Glasgow", "lat": 55.8642, "lon": -4.2518},
    {"city": "Liverpool", "lat": 53.4084, "lon": -2.9916},
    {"city": "Bristol", "lat": 51.4545, "lon": -2.5879},
    {"city": "Sheffield", "lat": 53.3811, "lon": -1.4701},
    {"city": "Leeds", "lat": 53.8008, "lon": -1.5491},
    {"city": "Edinburgh", "lat": 55.9533, "lon": -3.1883},
    {"city": "Leicester", "lat": 52.6369, "lon": -1.1398},
    {"city": "Cardiff", "lat": 51.4816, "lon": -3.1791},
    {"city": "Belfast", "lat": 54.5973, "lon": -5.9301},
    {"city": "Newcastle", "lat": 54.9783, "lon": -1.6178},
    {"city": "Brighton", "lat": 50.8225, "lon": -0.1372},
    {"city": "Nottingham", "lat": 52.9548, "lon": -1.1581},
    {"city": "Southampton", "lat": 50.9097, "lon": -1.4044},
    {"city": "Plymouth", "lat": 50.3755, "lon": -4.1427},
    {"city": "Aberdeen", "lat": 57.1497, "lon": -2.0943},
    {"city": "Cambridge", "lat": 52.2053, "lon": 0.1218},
    {"city": "Oxford", "lat": 51.7520, "lon": -1.2577}
]

def fetch_current_weather(city):
    # get current weather data for the city define above
    params = {
        "lat": city["lat"],
        "lon": city["lon"],
        "api_key": api_key,
        "units": "metric",
        "parts": "currently",
    }

    try:
        response = requests.get(api_base_url, params=params)
        response.raise_for_status()
        data = response.json()
        current_weather = data['data']['currently']
        dt = data['data']['dt']  
        return current_weather, dt
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {city['city']}: {e}")
        return None, None

def produce_to_kafka(producer, topic, key, value):
    # produce a message to a Kafka topic.
    try:
        producer.send(topic=topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(f"Error producing message to Kafka: {e}")

def current_data():
    # gather and produce weather data."""
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    for city in uk_cities:
        current_weather, dt = fetch_current_weather(city)
        if current_weather is None or dt is None:
            continue

        formatted_datetime = datetime.fromtimestamp(dt, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
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

        produce_to_kafka(
            producer=producer,
            topic=kafka_topic,
            key=city["city"],
            value=message,
        )

    print("Current weather data produced to Kafka successfully!")

current_data()