import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Any  # <-- Add this import
from kafka import KafkaProducer

# API base URL and key
api_base_url = "https://api.weatherxu.com/v1/weather"
api_key = "caa1e9d9aad1b714b639a94c80e275ce"

# Kafka configuration
kafka_broker = "broker:29092"
kafka_topic = "hourly_weatherxu"

# Define cities
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

def fetch_current_weather(city: tuple) -> Optional[Dict[str, Any]]:
    """Fetch weather data for a given city."""
    params: Dict[str, Any] = {
        "lat": city[2],
        "lon": city[3],
        "api_key": api_key,
        "units": "metric",
        "parts": "hourly" 
    }

    try:
        response = requests.get(api_base_url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching weather data for {city[1]}: {e}")
        return None

def filter_hourly_data(hourly_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter hourly data for 6am, 9am, 12pm, 3pm, 6pm, 9pm."""
    filtered_hours: List[Dict[str, Any]] = []
    for hour_data in hourly_data:
        dt: datetime = datetime.fromtimestamp(hour_data['forecastStart'])
        hour: int = dt.hour
        if hour in {6, 9, 12, 15, 18, 21}:
            filtered_hours.append(hour_data)
    return filtered_hours

def produce_to_kafka(
    producer: KafkaProducer,
    topic: str,
    key: str,
    value: Dict[str, Any]
) -> None:
    """Send a message to Kafka."""
    try:
        producer.send(topic=topic, key=key, value=value)
        producer.flush()
    except Exception as e:
        print(f"Error producing message to Kafka: {e}")

def consume_weather_data() -> None:
    """Fetch and produce filtered hourly weather data to Kafka."""
    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    for city in us_states:
        result: Optional[Dict[str, Any]] = fetch_current_weather(city)
        if not result:
            continue

        data: Dict[str, Any] = result.get('data', {})
        hourly_data: List[Dict[str, Any]] = data.get('hourly', {}).get('data', [])
        filtered_hourly: List[Dict[str, Any]] = filter_hourly_data(hourly_data)
        
        for hour_data in filtered_hourly:
            message: Dict[str, Any] = {
                "city": city[1],
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
                "datetime": hour_data.get("forecastStart"),
                "time_of_day": datetime.fromtimestamp(hour_data['forecastStart']).strftime("%I%p").lower()  # "6am", "9am", etc.
            }

            print(f"Producing hourly message for {city[1]} at {message['time_of_day']}")

            produce_to_kafka(
                producer=producer,
                topic=kafka_topic,
                key=f"{city[1]}_{hour_data['forecastStart']}",
                value=message
            )

    print("✅ Filtered hourly weather data produced to Kafka successfully!")


consume_weather_data()