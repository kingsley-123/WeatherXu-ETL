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

# Drop the weatherxu_current schema if it exists
cursor.execute("DROP SCHEMA IF EXISTS weatherxu_current CASCADE;")
cursor.execute("CREATE SCHEMA weatherxu_current;")
print("Schema 'weatherxu_current' dropped and recreated.")

create_dim_city_table = """
CREATE TABLE weatherxu_current.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_code VARCHAR(100),
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    country VARCHAR(100)
);
"""

cursor.execute(create_dim_city_table)
print("Table 'weatherxu_current.dim_city' created.")

# Data to insert into the table
city_data = [
('GBABD', 'Aberdeenshire', 57.1667, -2.6667, 'Great Britain'),
('GBABE', 'Aberdeen', 57.1497, -2.0943, 'Great Britain'),
('GBAGB', 'Argyll and Bute', 56.4000, -5.5000, 'Great Britain'),
('GBAGY', 'Anglesey', 53.2684, -4.3298, 'Great Britain'),
('GBANS', 'Angus', 56.7320, -2.9320, 'Great Britain'),
('GBANT', 'Antrim', 54.7167, -6.2167, 'Great Britain'),
('GBARD', 'Ards', 54.5833, -5.7333, 'Great Britain'),
('GBARM', 'Armagh', 54.3500, -6.6500, 'Great Britain'),
('GBBAS', 'Bath and North East Somerset', 51.3470, -2.4767, 'Great Britain'),
('GBBBD', 'Blackburn with Darwen', 53.7500, -2.4833, 'Great Britain'),
('GBBDF', 'Bedford', 52.1350, -0.4667, 'Great Britain'),
('GBBDG', 'Barking and Dagenham', 51.5600, 0.1300, 'Great Britain'),
('GBBEN', 'Brent', 51.5500, -0.2500, 'Great Britain'),
('GBBEX', 'Bexley', 51.4500, 0.1500, 'Great Britain'),
('GBBFS', 'Belfast', 54.5973, -5.9301, 'Great Britain'),
('GBBGE', 'Bridgend', 51.5040, -3.5800, 'Great Britain'),
('GBBGW', 'Blaenau Gwent', 51.7800, -3.2000, 'Great Britain'),
('GBBIR', 'Birmingham', 52.4800, -1.9025, 'Great Britain'),
('GBBKM', 'Buckinghamshire', 51.8000, -0.8167, 'Great Britain'),
('GBBLA', 'Ballymena', 54.8633, -6.2783, 'Great Britain'),
('GBBLY', 'Ballymoney', 55.0704, -6.5178, 'Great Britain'),
('GBBMH', 'Bournemouth', 50.7200, -1.8800, 'Great Britain'),
('GBBNB', 'Banbridge', 54.3500, -6.2667, 'Great Britain'),
('GBBNE', 'Barnet', 51.6500, -0.2000, 'Great Britain'),
('GBBNH', 'Brighton and Hove', 50.8225, -0.1372, 'Great Britain'),
('GBBNS', 'Barnsley', 53.5500, -1.4800, 'Great Britain'),
('GBBOL', 'Bolton', 53.5800, -2.4300, 'Great Britain'),
('GBBPL', 'Blackpool', 53.8142, -3.0551, 'Great Britain'),
('GBBRC', 'Bracknell Forest', 51.4167, -0.7500, 'Great Britain'),
('GBBRD', 'Bradford', 53.7960, -1.7594, 'Great Britain'),
('GBBRY', 'Bromley', 51.4000, 0.0500, 'Great Britain'),
('GBBST', 'Bristol', 51.4500, -2.5833, 'Great Britain'),
('GBBUR', 'Bury', 53.5933, -2.2972, 'Great Britain'),
('GBCAM', 'Cambridgeshire', 52.2000, 0.1167, 'Great Britain'),
('GBCAY', 'Caerphilly', 51.5784, -3.2185, 'Great Britain'),
('GBCBF', 'Central Bedfordshire', 52.0000, -0.5000, 'Great Britain'),
('GBCGN', 'Ceredigion', 52.2194, -3.9321, 'Great Britain'),
('GBCGV', 'Craigavon', 54.4500, -6.3667, 'Great Britain'),
('GBCHE', 'Cheshire East', 53.1667, -2.4667, 'Great Britain'),
('GBCHW', 'Cheshire West and Chester', 53.2000, -2.7167, 'Great Britain'),
('GBCKF', 'Carrickfergus', 54.7167, -5.8033, 'Great Britain'),
('GBCKT', 'Mid Ulster', 54.6433, -6.7483, 'Great Britain'),
('GBCLD', 'Calderdale', 53.7200, -1.9800, 'Great Britain'),
('GBCLK', 'Clackmannanshire', 56.1167, -3.7500, 'Great Britain'),
('GBCLR', 'Coleraine', 55.1333, -6.6667, 'Great Britain'),
('GBCMA', 'Cumbria', 54.5772, -2.7975, 'Great Britain'),
('GBCMD', 'Camden', 51.5500, -0.1667, 'Great Britain'),
('GBCMN', 'Carmarthenshire', 51.8557, -4.3107, 'Great Britain'),
('GBCON', 'Cornwall', 50.4167, -4.7500, 'Great Britain'),
('GBCOV', 'Coventry', 52.4081, -1.5106, 'Great Britain'),
('GBCRF', 'Cardiff', 51.4800, -3.1800, 'Great Britain'),
('GBCRY', 'Croydon', 51.3667, -0.0833, 'Great Britain'),
('GBCSR', 'Castlereagh', 54.5833, -5.8833, 'Great Britain'),
('GBCWY', 'Conwy', 53.2800, -3.8300, 'Great Britain'),
('GBDAL', 'Darlington', 54.5270, -1.5533, 'Great Britain'),
('GBDBY', 'Derbyshire', 53.1333, -1.5000, 'Great Britain'),
('GBDEN', 'Denbighshire', 53.1840, -3.4240, 'Great Britain'),
('GBDER', 'Derby', 52.9217, -1.4769, 'Great Britain'),
('GBDEV', 'Devon', 50.7083, -3.5200, 'Great Britain'),
('GBDGN', 'Dungannon', 54.5083, -6.7667, 'Great Britain'),
('GBDGY', 'Dumfries and Galloway', 55.0700, -3.6000, 'Great Britain'),
('GBDNC', 'Doncaster', 53.5200, -1.1300, 'Great Britain'),
('GBDND', 'Dundee', 56.4620, -2.9707, 'Great Britain'),
('GBDOR', 'Dorset', 50.7500, -2.3333, 'Great Britain'),
('GBDOW', 'Down', 54.3333, -5.7167, 'Great Britain'),
('GBDRY', 'Derry', 54.9958, -7.3074, 'Great Britain'),
('GBDUD', 'Dudley', 52.5000, -2.0833, 'Great Britain'),
('GBDUR', 'Durham', 54.7667, -1.5667, 'Great Britain'),
('GBEAL', 'Ealing', 51.5167, -0.3000, 'Great Britain'),
('GBEAY', 'East Ayrshire', 55.5000, -4.3667, 'Great Britain'),
('GBEDH', 'Edinburgh', 55.9533, -3.1883, 'Great Britain'),
('GBEDU', 'East Dunbartonshire', 55.9400, -4.2000, 'Great Britain'),
('GBELN', 'East Lothian', 55.9500, -2.7500, 'Great Britain'),
('GBELS', 'Eilean Siar', 57.7600, -7.0100, 'Great Britain'),
('GBENF', 'Enfield', 51.6500, -0.0833, 'Great Britain'),
('GBERW', 'East Renfrewshire', 55.7700, -4.3300, 'Great Britain'),
('GBERY', 'East Riding of Yorkshire', 53.8500, -0.4333, 'Great Britain'),
('GBESS', 'Essex', 51.7500, 0.5833, 'Great Britain'),
('GBESX', 'East Sussex', 50.9000, 0.3333, 'Great Britain'),
('GBFAL', 'Falkirk', 55.9999, -3.7833, 'Great Britain'),
('GBFER', 'Fermanagh', 54.3500, -7.6333, 'Great Britain'),
('GBFIF', 'Fife', 56.2500, -3.2000, 'Great Britain'),
('GBFLN', 'Flintshire', 53.1667, -3.1500, 'Great Britain'),
('GBGAT', 'Gateshead', 54.9500, -1.6000, 'Great Britain'),
('GBGLG', 'Glasgow', 55.8642, -4.2518, 'Great Britain'),
('GBGLS', 'Gloucestershire', 51.8667, -2.2333, 'Great Britain'),
('GBGRE', 'Greenwich', 51.4833, 0.0000, 'Great Britain'),
('GBGWN', 'Gwynedd', 52.9300, -4.0500, 'Great Britain'),
('GBHAL', 'Halton', 53.3333, -2.7000, 'Great Britain'),
('GBHAM', 'Hampshire', 51.0500, -1.3167, 'Great Britain'),
('GBHAV', 'Havering', 51.5833, 0.1833, 'Great Britain'),
('GBHCK', 'Hackney', 51.5500, -0.0667, 'Great Britain'),
('GBHEF', 'Herefordshire', 52.0800, -2.7500, 'Great Britain'),
('GBHIL', 'Hillingdon', 51.5333, -0.4500, 'Great Britain'),
('GBHLD', 'Highland', 57.5000, -5.0000, 'Great Britain'),
('GBHMF', 'Hammersmith and Fulham', 51.4917, -0.2233, 'Great Britain'),
('GBHNS', 'Hounslow', 51.4667, -0.3667, 'Great Britain'),
('GBHPL', 'Hartlepool', 54.6863, -1.2129, 'Great Britain'),
('GBHRT', 'Hertfordshire', 51.8000, -0.2000, 'Great Britain'),
('GBHRW', 'Harrow', 51.5833, -0.3333, 'Great Britain'),
('GBHRY', 'Haringey', 51.6000, -0.1167, 'Great Britain'),
('GBIOS', 'Isles of Scilly', 49.9333, -6.3167, 'Great Britain'),
('GBIOW', 'Isle of Wight', 50.6942, -1.3164, 'Great Britain'),
('GBISL', 'Islington', 51.5333, -0.1000, 'Great Britain'),
('GBIVC', 'Inverclyde', 55.9300, -4.7500, 'Great Britain'),
('GBKEC', 'Kensington and Chelsea', 51.5000, -0.1833, 'Great Britain'),
('GBKEN', 'Kent', 51.1833, 0.5000, 'Great Britain'),
('GBKHL', 'Kingston upon Hull', 53.7456, -0.3367, 'Great Britain'),
('GBKIR', 'Kirklees', 53.6000, -1.8000, 'Great Britain'),
('GBKTT', 'Kingston upon Thames', 51.4081, -0.3044, 'Great Britain'),
('GBKWL', 'Knowsley', 53.4500, -2.8500, 'Great Britain'),
('GBLAN', 'Lancashire', 53.8000, -2.6000, 'Great Britain'),
('GBLBH', 'Lambeth', 51.5000, -0.1167, 'Great Britain'),
('GBLCE', 'Leicester', 52.6333, -1.1333, 'Great Britain'),
('GBLDS', 'Leeds', 53.7997, -1.5492, 'Great Britain'),
('GBLEC', 'Leicestershire', 52.7700, -1.2000, 'Great Britain'),
('GBLEW', 'Lewisham', 51.4500, -0.0167, 'Great Britain'),
('GBLIN', 'Lincolnshire', 53.0333, -0.1833, 'Great Britain'),
('GBLIV', 'Liverpool', 53.4084, -2.9916, 'Great Britain'),
('GBLMV', 'Limavady', 55.0500, -6.9333, 'Great Britain'),
('GBLND', 'City', 51.5142, -0.0931, 'Great Britain'),
('GBLRN', 'Larne', 54.8583, -5.8208, 'Great Britain'),
('GBLSB', 'Lisburn', 54.5167, -6.0500, 'Great Britain'),
('GBLUT', 'Luton', 51.8786, -0.4141, 'Great Britain'),
('GBMAN', 'Manchester', 53.4809, -2.2374, 'Great Britain'),
('GBMDB', 'Middlesbrough', 54.5767, -1.2355, 'Great Britain'),
('GBMDW', 'Medway', 51.3900, 0.5400, 'Great Britain'),
('GBMFT', 'Magherafelt', 54.7556, -6.6094, 'Great Britain'),
('GBMIK', 'Milton Keynes', 52.0333, -0.7000, 'Great Britain'),
('GBMLN', 'Midlothian', 55.8833, -3.1667, 'Great Britain'),
('GBMON', 'Monmouthshire', 51.8117, -2.7167, 'Great Britain'),
('GBMRT', 'Merton', 51.4107, -0.2210, 'Great Britain'),
('GBMRY', 'Moray', 57.6498, -3.3161, 'Great Britain'),
('GBMTY', 'Merthyr Tydfil', 51.7500, -3.3833, 'Great Britain'),
('GBMYL', 'Moyle', 55.2000, -6.2500, 'Great Britain'),
('GBNAY', 'North Ayshire', 55.6400, -4.7600, 'Great Britain'),
('GBNBL', 'Northumberland', 55.2083, -2.0784, 'Great Britain'),
('GBNDN', 'North Down', 54.6500, -5.6667, 'Great Britain'),
('GBNEL', 'North East Lincolnshire', 53.5667, -0.0833, 'Great Britain'),
('GBNET', 'Newcastle upon Tyne', 54.9778, -1.6129, 'Great Britain'),
('GBNFK', 'Norfolk', 52.6667, 1.0000, 'Great Britain'),
('GBNGM', 'Nottingham', 52.9536, -1.1505, 'Great Britain'),
('GBNLK', 'North Lanarkshire', 55.8280, -3.9218, 'Great Britain'),
('GBNLN', 'North Lincolnshire', 53.6167, -0.6500, 'Great Britain'),
('GBNSM', 'North Somerset', 51.3800, -2.7800, 'Great Britain'),
('GBNTA', 'Newtownabbey', 54.6833, -5.9000, 'Great Britain'),
('GBNTH', 'Northamptonshire', 52.2500, -0.9000, 'Great Britain'),
('GBNTL', 'Neath Port Talbot', 51.6600, -3.8046, 'Great Britain'),
('GBNTT', 'Nottinghamshire', 53.1000, -1.0167, 'Great Britain'),
('GBNTY', 'North Tyneside', 55.0186, -1.4858, 'Great Britain'),
('GBNWM', 'Newham', 51.5333, 0.0333, 'Great Britain'),
('GBNWP', 'Newport', 51.5917, -3.0000, 'Great Britain'),
('GBNYK', 'North Yorkshire', 54.0000, -1.5167, 'Great Britain'),
('GBNYM', 'Newry and Mourne', 54.1833, -6.3667, 'Great Britain'),
('GBOLD', 'Oldham', 53.5417, -2.1111, 'Great Britain'),
('GBOMH', 'Omagh', 54.6000, -7.3000, 'Great Britain'),
('GBORK', 'Orkney', 59.0000, -3.0000, 'Great Britain'),
('GBOXF', 'Oxfordshire', 51.7500, -1.2500, 'Great Britain'),
('GBPEM', 'Pembrokeshire', 51.8333, -4.9667, 'Great Britain'),
('GBPKN', 'Perthshire and Kinross', 56.5833, -3.6000, 'Great Britain'),
('GBPLY', 'Plymouth', 50.3714, -4.1422, 'Great Britain'),
('GBPOL', 'Poole', 50.7200, -1.9800, 'Great Britain'),
('GBPOR', 'Portsmouth', 50.8058, -1.0872, 'Great Britain'),
('GBPOW', 'Powys', 52.6500, -3.3167, 'Great Britain'),
('GBPTE', 'Peterborough', 52.5833, -0.2500, 'Great Britain'),
('GBRCC', 'Redcar and Cleveland', 54.6000, -1.0700, 'Great Britain'),
('GBRCH', 'Rochdale', 53.6167, -2.1500, 'Great Britain'),
('GBRCT', 'Rhondda, Cynon, Taff', 51.6500, -3.4667, 'Great Britain'),
('GBRDB', 'Redbridge', 51.5833, 0.0833, 'Great Britain'),
('GBRDG', 'Reading', 51.4542, -0.9731, 'Great Britain'),
('GBRFW', 'Renfrewshire', 55.8465, -4.5321, 'Great Britain'),
('GBRIC', 'Richmond upon Thames', 51.4613, -0.3037, 'Great Britain'),
('GBROT', 'Rotherham', 53.4333, -1.3567, 'Great Britain'),
('GBRUT', 'Rutland', 52.6500, -0.6333, 'Great Britain'),
('GBSAW', 'Sandwell', 52.5060, -2.0147, 'Great Britain'),
('GBSAY', 'South Ayrshire', 55.2800, -4.6500, 'Great Britain'),
('GBSCB', 'Scottish Borders', 55.5486, -2.7861, 'Great Britain'),
('GBSFK', 'Suffolk', 52.1833, 1.0000, 'Great Britain'),
('GBSFT', 'Sefton', 53.5000, -3.0000, 'Great Britain'),
('GBSGC', 'South Gloucestershire', 51.5333, -2.4167, 'Great Britain'),
('GBSHF', 'Sheffield', 53.3811, -1.4701, 'Great Britain'),
('GBSHN', 'Merseyside', 53.4000, -3.0000, 'Great Britain'),
('GBSHR', 'Shropshire', 52.7000, -2.7500, 'Great Britain'),
('GBSKP', 'Stockport', 53.4083, -2.1500, 'Great Britain'),
('GBSLF', 'Salford', 53.4833, -2.2931, 'Great Britain'),
('GBSLG', 'Slough', 51.5111, -0.5950, 'Great Britain'),
('GBSLK', 'South Lanarkshire', 55.5500, -3.7000, 'Great Britain'),
('GBSND', 'Sunderland', 54.9061, -1.3831, 'Great Britain'),
('GBSOL', 'Solihull', 52.4130, -1.7780, 'Great Britain'),
('GBSOM', 'Somerset', 51.0833, -3.0000, 'Great Britain'),
('GBSOS', 'Southend-on-Sea', 51.5500, 0.7167, 'Great Britain'),
('GBSRY', 'Surrey', 51.2500, -0.4167, 'Great Britain'),
('GBSTB', 'Strabane', 54.8333, -7.4667, 'Great Britain'),
('GBSTE', 'Stoke-on-Trent', 53.0000, -2.1833, 'Great Britain'),
('GBSTG', 'Stirling', 56.1167, -3.9367, 'Great Britain'),
('GBSTH', 'Southampton', 50.9000, -1.4000, 'Great Britain'),
('GBSTN', 'Sutton', 51.3674, -0.1963, 'Great Britain'),
('GBSTS', 'Staffordshire', 52.8667, -2.0500, 'Great Britain'),
('GBSTT', 'Stockton-on-Tees', 54.5667, -1.3167, 'Great Britain'),
('GBSTY', 'South Tyneside', 54.9667, -1.4333, 'Great Britain'),
('GBSWA', 'Swansea', 51.6200, -3.9400, 'Great Britain'),
('GBSWD', 'Swindon', 51.5556, -1.7784, 'Great Britain'),
('GBSWK', 'Southwark', 51.5000, -0.0833, 'Great Britain'),
('GBTAM', 'Tameside', 53.4800, -2.0800, 'Great Britain'),
('GBTFW', 'Telford and Wrekin', 52.7167, -2.5000, 'Great Britain'),
('GBTHR', 'Thurrock', 51.5000, 0.4167, 'Great Britain'),
('GBTOB', 'Torbay', 50.4500, -3.5500, 'Great Britain'),
('GBTOF', 'Torfaen', 51.7000, -3.0400, 'Great Britain'),
('GBTRF', 'Trafford', 53.4167, -2.3500, 'Great Britain'),
('GBTWH', 'Tower Hamlets', 51.5167, -0.0333, 'Great Britain'),
('GBVGL', 'Vale of Glamorgan', 51.4047, -3.4783, 'Great Britain'),
('GBWAR', 'Warwickshire', 52.2800, -1.5800, 'Great Britain'),
('GBWBK', 'West Berkshire', 51.4296, -1.3000, 'Great Britain'),
('GBWDU', 'West Dunbartonshire', 55.9500, -4.5000, 'Great Britain'),
('GBWFT', 'Waltham Forest', 51.5917, -0.0300, 'Great Britain'),
('GBWGN', 'Wigan', 53.5433, -2.6280, 'Great Britain'),
('GBWIL', 'Wiltshire', 51.3167, -1.9667, 'Great Britain'),
('GBWKF', 'Wakefield', 53.6833, -1.4981, 'Great Britain'),
('GBWLL', 'Walsall', 52.5800, -1.9800, 'Great Britain'),
('GBWLN', 'West Lothian', 55.9000, -3.5500, 'Great Britain'),
('GBWLV', 'Wolverhampton', 52.5833, -2.1333, 'Great Britain'),
('GBWND', 'Wandsworth', 51.4500, -0.1833, 'Great Britain'),
('GBWNM', 'Royal Borough of Windsor and Maidenhead', 51.5150, -0.6545, 'Great Britain'),
('GBWOK', 'Wokingham', 51.4111, -0.8339, 'Great Britain'),
('GBWOR', 'Worcestershire', 52.2000, -2.2000, 'Great Britain'),
('GBWRL', 'Halton', 53.3330, -2.7000, 'Great Britain'),
('GBWRT', 'Warrington', 53.3900, -2.5976, 'Great Britain'),
('GBWRX', 'Wrexham', 53.0500, -3.0000, 'Great Britain'),
('GBWSM', 'Westminster', 51.5000, -0.1333, 'Great Britain'),
('GBWSX', 'West Sussex', 50.9333, -0.4667, 'Great Britain'),
('GBYOR', 'York', 53.9600, -1.0873, 'Great Britain'),
('GBZET', 'Shetland Islands', 60.3000, -1.3000, 'Great Britain')
]

# Inserting the data into the table
insert_query = """
INSERT INTO weatherxu_current.dim_city (city_code, city_name, latitude, longitude, country)
VALUES (%s, %s, %s, %s, %s);
"""

for city in city_data:
    cursor.execute(insert_query, city)
    
print("Data inserted into 'weatherxu_current.dim_city' table.")

# Create the dim_date table
create_dim_date_table = """
CREATE TABLE weatherxu_current.dim_date (
    date_id SERIAL PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour_minute VARCHAR(10) NOT NULL,
    day_of_week VARCHAR(10) NOT NULL
);
"""
cursor.execute(create_dim_date_table)
print("Table 'weatherxu_current.dim_date' created.")

# Create the condition_dim table
create_dim_condition_table = """
CREATE TABLE weatherxu_current.dim_condition (
    condition_id SERIAL PRIMARY KEY,
    condition_name VARCHAR(50) NOT NULL
);
"""
cursor.execute(create_dim_condition_table)
print("Table 'weatherxu_current.dim_condition' created.")

# Create the weather_fact table
create_fact_weather_table = """
CREATE TABLE weatherxu_current.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT REFERENCES weatherxu_current.dim_city(city_id),
    date_id INT REFERENCES weatherxu_current.dim_date(date_id),
    condition_id INT REFERENCES weatherxu_current.dim_condition(condition_id),
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
print("Table 'weatherxu_current.fact_weather' created.")

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("All tables created successfully!")