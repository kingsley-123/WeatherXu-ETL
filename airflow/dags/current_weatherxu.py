from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/current')))

def produce_currentdata():
    try:
        from produce_currentdata_to_kafka import current_data  # type: ignore
        return current_data()
    except Exception as e:
        logging.error(f"Error in produce_currentdata: {str(e)}")
        raise

def consume_currentdata():
    try:
        from consume_load_currentdata import consume_weather_data  # type: ignore
        return consume_weather_data()
    except Exception as e:
        logging.error(f"Error in consume_currentdata: {str(e)}")
        raise

def load_date_data():
    try:
        from consume_load_currentdata import load_date_data  # type: ignore
        return load_date_data()
    except Exception as e:
        logging.error(f"Error in load_date_data: {str(e)}")
        raise

def load_condition_data():
    try:
        from consume_load_currentdata import load_condition_data  # type: ignore
        return load_condition_data()
    except Exception as e:
        logging.error(f"Error in load_condition_data: {str(e)}")
        raise

def load_city_data():
    try:
        from consume_load_currentdata import load_city_data  # type: ignore
        return load_city_data()
    except Exception as e:
        logging.error(f"Error in load_city_data: {str(e)}")
        raise

def load_currentweather_data():
    try:
        from consume_load_currentdata import load_currentweather_data  # type: ignore
        return load_currentweather_data()
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
    "current_weatherxu",
    default_args=default_args,
    schedule_interval="0 */2 * * *",  # Every 2 hours
    catchup=False
) as dag:
  
    # Create tasks using the create_python_operator function
    produce_current_to_kafka = create_python_operator('produce_current_to_kafka', produce_currentdata)
    consume_currentdata_from_kafka = create_python_operator('consume_current_from_kafka', consume_currentdata)
    load_date_data = create_python_operator('load_date', load_date_data)
    load_condition_data = create_python_operator('load_condition', load_condition_data)
    load_city_data = create_python_operator('load_city', load_city_data)
    load_currentweather_data = create_python_operator('load_currentweather', load_currentweather_data)

    # Task dependencies
    produce_current_to_kafka >> consume_currentdata_from_kafka >> [load_date_data, load_condition_data, load_city_data] >> load_currentweather_data
