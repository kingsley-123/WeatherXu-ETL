from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts/daily')))

def produce_dailydata():
    try:
        from produce_dailydata_to_kafka import consume_weather_data  # type: ignore
        return consume_weather_data()
    except Exception as e:
        logging.error(f"Error in produce_dailydata: {str(e)}")
        raise

def consume_dailydata():
    try:
        from consume_load_dailydata import consume_weather_data  # type: ignore
        return consume_weather_data()
    except Exception as e:
        logging.error(f"Error in consume_dailydata: {str(e)}")
        raise

def load_data():
    try:
        from consume_load_dailydata import main  # type: ignore
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
    "daily_weatherxu",  
    default_args=default_args,
    schedule_interval="0 0 * * 0", # Every Sunday at 00:00 
    catchup=False
) as dag:
  
    # Create operator instances directly
    produce_task = PythonOperator(
        task_id='produce_dailydata_to_kafka',
        python_callable=produce_dailydata,
        execution_timeout=timedelta(minutes=15)
    )

    consume_task = PythonOperator(
        task_id='consume_dailydata_from_kafka',
        python_callable=consume_dailydata,
        execution_timeout=timedelta(minutes=15)
    )

    load_task = PythonOperator(
        task_id='load_dailydata',
        python_callable=load_data,
        execution_timeout=timedelta(minutes=15)
    )

    # Set dependencies using bitshift operators
    produce_task >> consume_task >> load_task

