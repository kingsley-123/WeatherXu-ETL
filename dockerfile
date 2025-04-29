FROM apache/airflow:2.6.3

# Install Kafka Python client
RUN pip install kafka-python
