from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/48_hour_forecast')))

def produce_48data():
    try:
        from produce_48data_to_kafka import weather_48data  # type: ignore
        return weather_48data()
    except Exception as e:
        logging.error(f"Error in produce_currentdata: {str(e)}")
        raise

def consume_48data():
    try:
        from consume_load_48data import consume_weather_data  # type: ignore
        return consume_weather_data()
    except Exception as e:
        logging.error(f"Error in consume_currentdata: {str(e)}")
        raise

def load_date_data():
    try:
        from consume_load_48data import load_date_data  # type: ignore
        return load_date_data()
    except Exception as e:
        logging.error(f"Error in load_date_data: {str(e)}")
        raise

def load_condition_data():
    try:
        from consume_load_48data import load_condition_data  # type: ignore
        return load_condition_data()
    except Exception as e:
        logging.error(f"Error in load_condition_data: {str(e)}")
        raise

def load_city_data():
    try:
        from consume_load_48data import load_city_data  # type: ignore
        return load_city_data()
    except Exception as e:
        logging.error(f"Error in load_city_data: {str(e)}")
        raise

def load_48weather_data():
    try:
        from consume_load_48data import load_48weather_data  # type: ignore
        return load_48weather_data()
    except Exception as e:
        logging.error(f"Error in load_currentweather_data: {str(e)}")
        raise

# Function to create PythonOperators
def create_python_operator(task_id, python_callable):
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        execution_timeout=timedelta(minutes=15),
    )

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "48_weatherxu",
    default_args=default_args,
    schedule_interval="0 0 */2 * *",  # Every 48 hours
    catchup=False
) as dag:
  
    # Create tasks using the create_python_operator function
    produce_48data_to_kafka = create_python_operator('produce_current_to_kafka', produce_48data)
    consume_48data_from_kafka = create_python_operator('consume_current_from_kafka', consume_48data)
    load_date_data = create_python_operator('load_date', load_date_data)
    load_condition_data = create_python_operator('load_condition', load_condition_data)
    load_city_data = create_python_operator('load_city', load_city_data)
    load_48weather_data = create_python_operator('load_currentweather', load_48weather_data)

    # Task dependencies
    produce_48data_to_kafka >> consume_48data_from_kafka >> [load_date_data, load_condition_data, load_city_data] >> load_48weather_data
