from airflow import DAG    # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore
import requests
import psycopg2  # type: ignore
import urllib3
import logging
import pandas as pd  # type: ignore
import numpy as np
import hashlib
import json

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
# default parameter
default_args = {
    'owner': 'Augy',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}


def fetch_weather_data():
    """
    Fetches weather data of a country through an api and convert all value
    to string and convert it to hash. Checks if the data has changed by
    comparing a hash of extracted values and hsah of what is stored in
    database. If the data is new, it updates, if not skips to save time.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    CITIES = json.loads(Variable.get("CITIES"))
    API_KEY = Variable.get("API_KEY")
    conn = psycopg2.connect(
        host=Variable.get("DB_HOST"),
        database=Variable.get("DB_NAME"),
        user=Variable.get("DB_USER"),
        password=Variable.get("DB_PASSWORD"),
        port=Variable.get("DB_PORT")
    )
    cursor = conn.cursor()

    #  generating hash for the fetched weather data
    def generate_hash(values_tuple):
        """hash is created for a particular row, when code runs 2nd time
        the hash is compared(1st generated and 2nd generated hasg),
        if hash is same then there is no need to update all data.
        Hash is same only if data is same.
        If different hash, row is updated
        """
        raw = "|".join(map(str, values_tuple))   # flatten clean
        return hashlib.md5(raw.encode()).hexdigest()

    def download_and_store(city):
        # api fetches weather data and give us all the data we need
        url = (
            f"https://api.weatherapi.com/v1/current.json?"
            f"key={API_KEY}&q={city}&aqi=yes"
        )

        try:
            response = requests.get(url, verify=False)
            data = response.json()
        except Exception as e:
            logging.error(f"Err on {city}: {e}")
            raise RuntimeError("Failed to fetch city.") from e

        location = data["location"]
        current = data["current"]
        air = current["air_quality"]

        values = (
            location["name"],
            location["region"],
            location["country"],
            current["temp_c"],
            current["condition"]["text"],
            current["wind_kph"],
            current["pressure_mb"],
            current["precip_mm"],
            current["humidity"],
            current["cloud"],
            current["uv"],
            air["co"],
            air["no2"],
            air["pm2_5"],
            air["us-epa-index"]
        )

        city_name = location["name"]
        new_hash = generate_hash(values)
        cursor.execute(
            "SELECT data_hash FROM weather_data WHERE city = %s",
            (city_name,)
            )
        row = cursor.fetchone()

        if row:
            # if city alredy exist in DB
            # if same hash then no change or no update in data
            old_hash = row[0]
            if old_hash == new_hash:
                logging.info(f"No change for {city_name}")
                return 0

            # if hash dont match, data is updated
            cursor.execute("""
                UPDATE weather_data SET
                    region=%s, country=%s, temperature_c=%s, condition=%s,
                    wind_kph=%s, pressure_mb=%s, precipitation_mm=%s,
                    humidity=%s, cloud=%s, uv=%s, co=%s, no2=%s, pm25=%s,
                    us_epa_index=%s,data_hash=%s
                WHERE city=%s
            """, values[1:] + (new_hash, city_name))
            conn.commit()
            logging.info(f"Updated: {city_name}")
        else:
            # if city dont exist in DB
            cursor.execute("""
            INSERT INTO weather_data (
                city, region, country, temperature_c, condition,
                wind_kph, pressure_mb, precipitation_mm,
                humidity, cloud, uv, co, no2, pm25, us_epa_index, data_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values + (new_hash,))
            conn.commit()
            logging.info(f"Inserted new city: {city_name}")


    # looping through 100 countryes stored in airflow variable as json
    for city in CITIES:
        download_and_store(city)

    cursor.close()
    conn.close()
    logging.info("Weather data sync done.")


def transform_weather_data():
    """
    Weather data is fetched from postgress, some transformations are perfomed
    Transformations : filling null with 0, filling vacant region with city-
    names, adding new columns like "wind_pressure_ratio", "PollutionIndex"
    If cloud more than 50 then Is_Cloud is 1 , otherwise it is 0.
    These data are then stored back to postgress tabel
    """
    try:
        # Connect to Postgres
        conn = psycopg2.connect(
            host=Variable.get("DB_HOST"),
            port=Variable.get("DB_PORT"),
            user=Variable.get("DB_USER"),
            password=Variable.get("DB_PASSWORD"),
            database=Variable.get("DB_NAME")
        )
    except Exception as e:
        raise RuntimeError("Failed to connect to Postgres.") from e

    # Fetch data from weather_data tabel
    df = pd.read_sql("SELECT * FROM weather_data ORDER BY id ASC;", conn)   # type: ignore
    df = df.drop(columns=['data_hash'], errors='ignore')
    if df.empty:
        logging.info(" No data found in weather_data table.")
        conn.close()
        exit()

    logging.info(f" Loaded {len(df)} rows from weather_data")

    # Fill empty space with 0
    df.fillna({
        'humidity': 0,
        'wind_kph': 0,
        'pressure_mb': 0,
        'precipitation_mm': 0,
        'cloud': 0,
        'uv': 0,
        'co': 0,
        'no2': 0,
        'pm25': 0,
    }, inplace=True)

    """ Transformations in data
    if region is null, city is added to that column,
    wind_pressure_ration , PollutionIndex is added.
    if cloud is more than 50 Is_Cloud 1 is given
    """
    df['region'] = df['region'].replace('', np.nan)
    df['region'] = df['region'].fillna(df['city'])

    df['wind_pressure_ratio'] = (
        df['wind_kph'] / (df['pressure_mb'] + 1)).round(5)

    df['PollutionIndex'] = (
        (0.5 * df['pm25'] + 0.3 * df['no2'] + 0.2 * df['co'])).round(2)

    df['Is_Cloud'] = df['cloud'].apply(lambda x: 1 if x > 50 else 0)

    cols = [
        'humidity', 'wind_kph', 'pressure_mb', 'precipitation_mm',
        'cloud', 'uv', 'co', 'no2', 'pm25'
    ]

    df['country_avg'] = (
        df.groupby('country')[cols]
        .transform('mean')
        .mean(axis=1)
        .round(2)
    )

    # Clear all data from tabel, Create new table with all columns
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS final_res;")
        cur.execute("""
            CREATE TABLE final_res (
                id SERIAL PRIMARY KEY,
                city TEXT,
                region TEXT,
                country TEXT,
                humidity FLOAT,
                wind_kph FLOAT,
                pressure_mb FLOAT,
                precipitation_mm FLOAT,
                cloud FLOAT,
                uv FLOAT,
                co FLOAT,
                no2 FLOAT,
                pm25 FLOAT,
                temperature_c FLOAT,
                condition TEXT,
                us_epa_index FLOAT,
                wind_pressure_ratio FLOAT,
                PollutionIndex FLOAT,
                Is_Cloud INT,
                country_avg FLOAT
            );
        """)
        conn.commit()

    #  Insert data to 2nd table "final_res"
    df_columns = list(df.columns)
    columns = ",".join(df_columns)
    values = ",".join(["%s" for col in df_columns])
    insert_stmt = f"INSERT INTO final_res ({columns}) VALUES ({values})"

    # looping throught each rows and taking their values and put to tabel
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(insert_stmt, tuple(row))
        conn.commit()

    logging.info(f"Successfully saved {len(df)} rows to final_res!")
    # closing connection
    if conn:
        conn.close()
        logging.info(" Connection closed.")


# setting up orchestration duration, start time
with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    description='Fetch + Transform weather data hourly',
    schedule_interval='@hourly',
    start_date=datetime(2025, 10, 28),
    catchup=False,
) as dag:
    # calling fetch_weather_data function from fetch.py
    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    # calling transform_weather_data function transformations.py
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data
    )

    # setting up order
    fetch_task >> transform_task  # type: ignore
