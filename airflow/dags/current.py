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

def load_currentdata():
    try:
        from consume_load_currentdata import main  # type: ignore
        return main()
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

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
  
    # Create operator instances
    produce_task = PythonOperator(
        task_id='produce_currentdata_to_kafka',
        python_callable=produce_currentdata,
        execution_timeout=timedelta(minutes=15)
    )
    
    consume_task = PythonOperator(
        task_id='consume_currentdata_from_kafka',
        python_callable=consume_currentdata,
        execution_timeout=timedelta(minutes=15)
    )
    
    load_task = PythonOperator(
        task_id='load_currentdata',
        python_callable=load_currentdata,
        execution_timeout=timedelta(minutes=15)
    )

    # Set dependencies using bitshift operators
    produce_task >> consume_task >> load_task