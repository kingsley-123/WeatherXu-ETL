from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/current')))

def produce_currentdata():
    try:
        from produce_currentdata_to_kafka import consume_weather_data  # type: ignore
        return consume_weather_data()
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

def load_data():
    try:
        from consume_load_currentdata import main  # type: ignore
        return main()
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
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
    "start_date": datetime(2025, 4, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "current_weatherxu",
    default_args=default_args,
    schedule_interval="0 */3 * * *",  # Every 3 hours
    catchup=False
) as dag:
  
    # Create tasks using the create_python_operator function
    produce_current_to_kafka = create_python_operator('produce_current_to_kafka', produce_currentdata)
    consume_currentdata_from_kafka = create_python_operator('consume_current_from_kafka', consume_currentdata)
    load_data_task = create_python_operator('load_data', load_data)

    produce_current_to_kafka >> consume_currentdata_from_kafka >> load_data_task