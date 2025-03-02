import requests
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
    {"city": "Aberdeenshire", "lat": 57.1667, "lon": -2.6667},
    {"city": "Aberdeen", "lat": 57.1497, "lon": -2.0943},
    {"city": "Argyll and Bute", "lat": 56.4000, "lon": -5.5000},
    {"city": "Anglesey", "lat": 53.2684, "lon": -4.3298},
    {"city": "Angus", "lat": 56.7320, "lon": -2.9320},
    {"city": "Antrim", "lat": 54.7167, "lon": -6.2167},
    {"city": "Ards", "lat": 54.5833, "lon": -5.7333},
    {"city": "Armagh", "lat": 54.3500, "lon": -6.6500},
    {"city": "Bath and North East Somerset", "lat": 51.3470, "lon": -2.4767},
    {"city": "Blackburn with Darwen", "lat": 53.7500, "lon": -2.4833},
    {"city": "Bedford", "lat": 52.1350, "lon": -0.4667},
    {"city": "Barking and Dagenham", "lat": 51.5600, "lon": 0.1300},
    {"city": "Brent", "lat": 51.5500, "lon": -0.2500},
    {"city": "Bexley", "lat": 51.4500, "lon": 0.1500},
    {"city": "Belfast", "lat": 54.5973, "lon": -5.9301},
    {"city": "Bridgend", "lat": 51.5040, "lon": -3.5800},
    {"city": "Blaenau Gwent", "lat": 51.7800, "lon": -3.2000},
    {"city": "Birmingham", "lat": 52.4800, "lon": -1.9025},
    {"city": "Buckinghamshire", "lat": 51.8000, "lon": -0.8167},
    {"city": "Ballymena", "lat": 54.8633, "lon": -6.2783},
    {"city": "Ballymoney", "lat": 55.0704, "lon": -6.5178},
    {"city": "Bournemouth", "lat": 50.7200, "lon": -1.8800},
    {"city": "Banbridge", "lat": 54.3500, "lon": -6.2667},
    {"city": "Barnet", "lat": 51.6500, "lon": -0.2000},
    {"city": "Brighton and Hove", "lat": 50.8225, "lon": -0.1372},
    {"city": "Barnsley", "lat": 53.5500, "lon": -1.4800},
    {"city": "Bolton", "lat": 53.5800, "lon": -2.4300},
    {"city": "Blackpool", "lat": 53.8142, "lon": -3.0551},
    {"city": "Bracknell Forest", "lat": 51.4167, "lon": -0.7500},
    {"city": "Bradford", "lat": 53.7960, "lon": -1.7594},
    {"city": "Bromley", "lat": 51.4000, "lon": 0.0500},
    {"city": "Bristol", "lat": 51.4500, "lon": -2.5833},
    {"city": "Bury", "lat": 53.5933, "lon": -2.2972},
    {"city": "Cambridgeshire", "lat": 52.2000, "lon": 0.1167},
    {"city": "Caerphilly", "lat": 51.5784, "lon": -3.2185},
    {"city": "Central Bedfordshire", "lat": 52.0000, "lon": -0.5000},
    {"city": "Ceredigion", "lat": 52.2194, "lon": -3.9321},
    {"city": "Craigavon", "lat": 54.4500, "lon": -6.3667},
    {"city": "Cheshire East", "lat": 53.1667, "lon": -2.4667},
    {"city": "Cheshire West and Chester", "lat": 53.2000, "lon": -2.7167},
    {"city": "Carrickfergus", "lat": 54.7167, "lon": -5.8033},
    {"city": "Mid Ulster", "lat": 54.6433, "lon": -6.7483},
    {"city": "Calderdale", "lat": 53.7200, "lon": -1.9800},
    {"city": "Clackmannanshire", "lat": 56.1167, "lon": -3.7500},
    {"city": "Coleraine", "lat": 55.1333, "lon": -6.6667},
    {"city": "Cumbria", "lat": 54.5772, "lon": -2.7975},
    {"city": "Camden", "lat": 51.5500, "lon": -0.1667},
    {"city": "Carmarthenshire", "lat": 51.8557, "lon": -4.3107},
    {"city": "Cornwall", "lat": 50.4167, "lon": -4.7500},
    {"city": "Coventry", "lat": 52.4081, "lon": -1.5106},
    {"city": "Cardiff", "lat": 51.4800, "lon": -3.1800},
    {"city": "Croydon", "lat": 51.3667, "lon": -0.0833},
    {"city": "Castlereagh", "lat": 54.5833, "lon": -5.8833},
    {"city": "Conwy", "lat": 53.2800, "lon": -3.8300},
    {"city": "Darlington", "lat": 54.5270, "lon": -1.5533},
    {"city": "Derbyshire", "lat": 53.1333, "lon": -1.5000},
    {"city": "Denbighshire", "lat": 53.1840, "lon": -3.4240},
    {"city": "Derby", "lat": 52.9217, "lon": -1.4769},
    {"city": "Devon", "lat": 50.7083, "lon": -3.5200},
    {"city": "Dungannon", "lat": 54.5083, "lon": -6.7667},
    {"city": "Dumfries and Galloway", "lat": 55.0700, "lon": -3.6000},
    {"city": "Doncaster", "lat": 53.5200, "lon": -1.1300},
    {"city": "Dundee", "lat": 56.4620, "lon": -2.9707},
    {"city": "Dorset", "lat": 50.7500, "lon": -2.3333},
    {"city": "Down", "lat": 54.3333, "lon": -5.7167},
    {"city": "Derry", "lat": 54.9958, "lon": -7.3074},
    {"city": "Dudley", "lat": 52.5000, "lon": -2.0833},
    {"city": "Durham", "lat": 54.7667, "lon": -1.5667},
    {"city": "Ealing", "lat": 51.5167, "lon": -0.3000},
    {"city": "East Ayrshire", "lat": 55.5000, "lon": -4.3667},
    {"city": "Edinburgh", "lat": 55.9533, "lon": -3.1883},
    {"city": "East Dunbartonshire", "lat": 55.9400, "lon": -4.2000},
    {"city": "East Lothian", "lat": 55.9500, "lon": -2.7500},
    {"city": "Eilean Siar", "lat": 57.7600, "lon": -7.0100},
    {"city": "Enfield", "lat": 51.6500, "lon": -0.0833},
    {"city": "East Renfrewshire", "lat": 55.7700, "lon": -4.3300},
    {"city": "East Riding of Yorkshire", "lat": 53.8500, "lon": -0.4333},
    {"city": "Essex", "lat": 51.7500, "lon": 0.5833},
    {"city": "East Sussex", "lat": 50.9000, "lon": 0.3333},
    {"city": "Falkirk", "lat": 55.9999, "lon": -3.7833},
    {"city": "Fermanagh", "lat": 54.3500, "lon": -7.6333},
    {"city": "Fife", "lat": 56.2500, "lon": -3.2000},
    {"city": "Flintshire", "lat": 53.1667, "lon": -3.1500},
    {"city": "Gateshead", "lat": 54.9500, "lon": -1.6000},
    {"city": "Glasgow", "lat": 55.8642, "lon": -4.2518},
    {"city": "Gloucestershire", "lat": 51.8667, "lon": -2.2333},
    {"city": "Greenwich", "lat": 51.4833, "lon": 0.0000},
    {"city": "Gwynedd", "lat": 52.9300, "lon": -4.0500},
    {"city": "Halton", "lat": 53.3333, "lon": -2.7000},
    {"city": "Hampshire", "lat": 51.0500, "lon": -1.3167},
    {"city": "Havering", "lat": 51.5833, "lon": 0.1833},
    {"city": "Hackney", "lat": 51.5500, "lon": -0.0667},
    {"city": "Herefordshire", "lat": 52.0800, "lon": -2.7500},
    {"city": "Hillingdon", "lat": 51.5333, "lon": -0.4500},
    {"city": "Highland", "lat": 57.5000, "lon": -5.0000},
    {"city": "Hammersmith and Fulham", "lat": 51.4917, "lon": -0.2233},
    {"city": "Hounslow", "lat": 51.4667, "lon": -0.3667},
    {"city": "Hartlepool", "lat": 54.6863, "lon": -1.2129},
    {"city": "Hertfordshire", "lat": 51.8000, "lon": -0.2000},
    {"city": "Harrow", "lat": 51.5833, "lon": -0.3333},
    {"city": "Haringey", "lat": 51.6000, "lon": -0.1167},
    {"city": "Isles of Scilly", "lat": 49.9333, "lon": -6.3167},
    {"city": "Isle of Wight", "lat": 50.6942, "lon": -1.3164},
    {"city": "Islington", "lat": 51.5333, "lon": -0.1000},
    {"city": "Inverclyde", "lat": 55.9300, "lon": -4.7500},
    {"city": "Kensington and Chelsea", "lat": 51.5000, "lon": -0.1833},
    {"city": "Kent", "lat": 51.1833, "lon": 0.5000},
    {"city": "Kingston upon Hull", "lat": 53.7456, "lon": -0.3367},
    {"city": "Kirklees", "lat": 53.6000, "lon": -1.8000},
    {"city": "Kingston upon Thames", "lat": 51.4081, "lon": -0.3044},
    {"city": "Knowsley", "lat": 53.4500, "lon": -2.8500},
    {"city": "Lancashire", "lat": 53.8000, "lon": -2.6000},
    {"city": "Lambeth", "lat": 51.5000, "lon": -0.1167},
    {"city": "Leicester", "lat": 52.6333, "lon": -1.1333},
    {"city": "Leeds", "lat": 53.7997, "lon": -1.5492},
    {"city": "Leicestershire", "lat": 52.7700, "lon": -1.2000},
    {"city": "Lewisham", "lat": 51.4500, "lon": -0.0167},
    {"city": "Lincolnshire", "lat": 53.0333, "lon": -0.1833},
    {"city": "Liverpool", "lat": 53.4084, "lon": -2.9916},
    {"city": "Limavady", "lat": 55.0500, "lon": -6.9333},
    {"city": "City", "lat": 51.5142, "lon": -0.0931},
    {"city": "Larne", "lat": 54.8583, "lon": -5.8208},
    {"city": "Lisburn", "lat": 54.5167, "lon": -6.0500},
    {"city": "Luton", "lat": 51.8786, "lon": -0.4141},
    {"city": "Manchester", "lat": 53.4809, "lon": -2.2374},
    {"city": "Middlesbrough", "lat": 54.5767, "lon": -1.2355},
    {"city": "Medway", "lat": 51.3900, "lon": 0.5400},
    {"city": "Magherafelt", "lat": 54.7556, "lon": -6.6094},
    {"city": "Milton Keynes", "lat": 52.0333, "lon": -0.7000},
    {"city": "Midlothian", "lat": 55.8833, "lon": -3.1667},
    {"city": "Monmouthshire", "lat": 51.8117, "lon": -2.7167},
    {"city": "Merton", "lat": 51.4107, "lon": -0.2210},
    {"city": "Moray", "lat": 57.6498, "lon": -3.3161},
    {"city": "Merthyr Tydfil", "lat": 51.7500, "lon": -3.3833},
    {"city": "Moyle", "lat": 55.2000, "lon": -6.2500},
    {"city": "North Ayshire", "lat": 55.6400, "lon": -4.7600},
    {"city": "Northumberland", "lat": 55.2083, "lon": -2.0784},
    {"city": "North Down", "lat": 54.6500, "lon": -5.6667},
    {"city": "North East Lincolnshire", "lat": 53.5667, "lon": -0.0833},
    {"city": "Newcastle upon Tyne", "lat": 54.9778, "lon": -1.6129},
    {"city": "Norfolk", "lat": 52.6667, "lon": 1.0000},
    {"city": "Nottingham", "lat": 52.9536, "lon": -1.1505},
    {"city": "North Lanarkshire", "lat": 55.8280, "lon": -3.9218},
    {"city": "North Lincolnshire", "lat": 53.6167, "lon": -0.6500},
    {"city": "North Somerset", "lat": 51.3800, "lon": -2.7800},
    {"city": "Newtownabbey", "lat": 54.6833, "lon": -5.9000},
    {"city": "Northamptonshire", "lat": 52.2500, "lon": -0.9000},
    {"city": "Neath Port Talbot", "lat": 51.6600, "lon": -3.8046},
    {"city": "Nottinghamshire", "lat": 53.1000, "lon": -1.0167},
    {"city": "North Tyneside", "lat": 55.0186, "lon": -1.4858},
    {"city": "Newham", "lat": 51.5333, "lon": 0.0333},
    {"city": "Newport", "lat": 51.5917, "lon": -3.0000},
    {"city": "North Yorkshire", "lat": 54.0000, "lon": -1.5167},
    {"city": "Newry and Mourne", "lat": 54.1833, "lon": -6.3667},
    {"city": "Oldham", "lat": 53.5417, "lon": -2.1111},
    {"city": "Omagh", "lat": 54.6000, "lon": -7.3000},
    {"city": "Orkney", "lat": 59.0000, "lon": -3.0000},
    {"city": "Oxfordshire", "lat": 51.7500, "lon": -1.2500},
    {"city": "Pembrokeshire", "lat": 51.8333, "lon": -4.9667},
    {"city": "Perthshire and Kinross", "lat": 56.5833, "lon": -3.6000},
    {"city": "Plymouth", "lat": 50.3714, "lon": -4.1422},
    {"city": "Poole", "lat": 50.7200, "lon": -1.9800},
    {"city": "Portsmouth", "lat": 50.8058, "lon": -1.0872},
    {"city": "Powys", "lat": 52.6500, "lon": -3.3167},
    {"city": "Peterborough", "lat": 52.5833, "lon": -0.2500},
    {"city": "Redcar and Cleveland", "lat": 54.6000, "lon": -1.0700},
    {"city": "Rochdale", "lat": 53.6167, "lon": -2.1500},
    {"city": "Rhondda, Cynon, Taff", "lat": 51.6500, "lon": -3.4667},
    {"city": "Redbridge", "lat": 51.5833, "lon": 0.0833},
    {"city": "Reading", "lat": 51.4542, "lon": -0.9731},
    {"city": "Renfrewshire", "lat": 55.8465, "lon": -4.5321},
    {"city": "Richmond upon Thames", "lat": 51.4613, "lon": -0.3037},
    {"city": "Rotherham", "lat": 53.4333, "lon": -1.3567},
    {"city": "Rutland", "lat": 52.6500, "lon": -0.6333},
    {"city": "Sandwell", "lat": 52.5060, "lon": -2.0147},
    {"city": "South Ayrshire", "lat": 55.2800, "lon": -4.6500},
    {"city": "Scottish Borders", "lat": 55.5486, "lon": -2.7861},
    {"city": "Suffolk", "lat": 52.1833, "lon": 1.0000},
    {"city": "Sefton", "lat": 53.5000, "lon": -3.0000},
    {"city": "South Gloucestershire", "lat": 51.5333, "lon": -2.4167},
    {"city": "Sheffield", "lat": 53.3811, "lon": -1.4701},
    {"city": "Merseyside", "lat": 53.4000, "lon": -3.0000},
    {"city": "Shropshire", "lat": 52.7000, "lon": -2.7500},
    {"city": "Stockport", "lat": 53.4083, "lon": -2.1500},
    {"city": "Salford", "lat": 53.4833, "lon": -2.2931},
    {"city": "Slough", "lat": 51.5111, "lon": -0.5950},
    {"city": "South Lanarkshire", "lat": 55.5500, "lon": -3.7000},
    {"city": "Sunderland", "lat": 54.9061, "lon": -1.3831},
    {"city": "Solihull", "lat": 52.4130, "lon": -1.7780},
    {"city": "Somerset", "lat": 51.0833, "lon": -3.0000},
    {"city": "Southend-on-Sea", "lat": 51.5500, "lon": 0.7167},
    {"city": "Surrey", "lat": 51.2500, "lon": -0.4167},
    {"city": "Strabane", "lat": 54.8333, "lon": -7.4667},
    {"city": "Stoke-on-Trent", "lat": 53.0000, "lon": -2.1833},
    {"city": "Stirling", "lat": 56.1167, "lon": -3.9367},
    {"city": "Southampton", "lat": 50.9000, "lon": -1.4000},
    {"city": "Sutton", "lat": 51.3674, "lon": -0.1963},
    {"city": "Staffordshire", "lat": 52.8667, "lon": -2.0500},
    {"city": "Stockton-on-Tees", "lat": 54.5667, "lon": -1.3167},
    {"city": "South Tyneside", "lat": 54.9667, "lon": -1.4333},
    {"city": "Swansea", "lat": 51.6200, "lon": -3.9400},
    {"city": "Swindon", "lat": 51.5556, "lon": -1.7784},
    {"city": "Southwark", "lat": 51.5000, "lon": -0.0833},
    {"city": "Tameside", "lat": 53.4800, "lon": -2.0800},
    {"city": "Telford and Wrekin", "lat": 52.7167, "lon": -2.5000},
    {"city": "Thurrock", "lat": 51.5000, "lon": 0.4167},
    {"city": "Torbay", "lat": 50.4500, "lon": -3.5500},
    {"city": "Torfaen", "lat": 51.7000, "lon": -3.0400},
    {"city": "Trafford", "lat": 53.4167, "lon": -2.3500},
    {"city": "Tower Hamlets", "lat": 51.5167, "lon": -0.0333},
    {"city": "Vale of Glamorgan", "lat": 51.4047, "lon": -3.4783},
    {"city": "Warwickshire", "lat": 52.2800, "lon": -1.5800},
    {"city": "West Berkshire", "lat": 51.4296, "lon": -1.3000},
    {"city": "West Dunbartonshire", "lat": 55.9500, "lon": -4.5000},
    {"city": "Waltham Forest", "lat": 51.5917, "lon": -0.0300},
    {"city": "Wigan", "lat": 53.5433, "lon": -2.6280},
    {"city": "Wiltshire", "lat": 51.3167, "lon": -1.9667},
    {"city": "Wakefield", "lat": 53.6833, "lon": -1.4981},
    {"city": "Walsall", "lat": 52.5800, "lon": -1.9800},
    {"city": "West Lothian", "lat": 55.9000, "lon": -3.5500},
    {"city": "Wolverhampton", "lat": 52.5833, "lon": -2.1333},
    {"city": "Wandsworth", "lat": 51.4500, "lon": -0.1833},
    {"city": "Royal Borough of Windsor and Maidenhead", "lat": 51.5150, "lon": -0.6545},
    {"city": "Wokingham", "lat": 51.4111, "lon": -0.8339},
    {"city": "Worcestershire", "lat": 52.2000, "lon": -2.2000},
    {"city": "Halton", "lat": 53.3330, "lon": -2.7000},
    {"city": "Warrington", "lat": 53.3900, "lon": -2.5976},
    {"city": "Wrexham", "lat": 53.0500, "lon": -3.0000},
    {"city": "Westminster", "lat": 51.5000, "lon": -0.1333},
    {"city": "West Sussex", "lat": 50.9333, "lon": -0.4667},
    {"city": "York", "lat": 53.9600, "lon": -1.0873},
    {"city": "Shetland Islands", "lat": 60.3000, "lon": -1.3000}
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
            "datetime": current_weather.get("dt"),
        }

        produce_to_kafka(
            producer=producer,
            topic=kafka_topic,
            key=city["city"],
            value=message,
        )

    print("Current weather data produced to Kafka successfully!")

current_data()