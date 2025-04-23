FROM apache/airflow:3.0.0  

# Install Kafka Python client
RUN pip install kafka-python
