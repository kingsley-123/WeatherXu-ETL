from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/hourly')))

def produce_hourlydata():
    try:
        from produce_hourlydata_to_kafka import consume_weather_data  # type: ignore
        return consume_weather_data()
    except Exception as e:
        logging.error(f"Error in produce_hourlydata: {str(e)}")
        raise

def consume_hourlydata():
    try:
        from consume_load_hourlydata import consume_weather_data  # type: ignore
        return consume_weather_data()
    except Exception as e:
        logging.error(f"Error in consume_hourlydata: {str(e)}")
        raise

def load_data():
    try:
        from consume_load_hourlydata import main  # type: ignore
        return main()
    except Exception as e:
        logging.error(f"Error in load_data: {str(e)}")
        raise

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hourly_weatherxu",  # Changed from "current_weatherxu" to reflect hourly nature
    default_args=default_args,
    schedule_interval="0 */6 * * *",  # Every 6 hours
    catchup=False
) as dag:
  
    # Create operator instances directly
    produce_task = PythonOperator(
        task_id='produce_hourlydata_to_kafka',
        python_callable=produce_hourlydata,
        execution_timeout=timedelta(minutes=15)
    )

    consume_task = PythonOperator(
        task_id='consume_hourlydata_from_kafka',
        python_callable=consume_hourlydata,
        execution_timeout=timedelta(minutes=15)
    )

    load_task = PythonOperator(
        task_id='load_hourlydata',
        python_callable=load_data,
        execution_timeout=timedelta(minutes=15)
    )

    # Set dependencies using bitshift operators
    produce_task >> consume_task >> load_task