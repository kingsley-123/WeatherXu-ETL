# WeatherXu-ETL

## 📘 Project Overview

### Objective

This project addresses the need for reliable, real-time weather data ingestion, processing, storage, and visualization. The main goal was to design and implement a scalable data engineering pipeline that could efficiently collect data from the **Weatherxu API**, process it in real-time using **Apache Kafka**, and make it readily available for analytics and business intelligence reporting in **Power BI**.

### Business Problem

Many organizations especially those in logistics, agriculture, aviation, and energy—rely heavily on accurate weather forecasts to make operational decisions. However, these businesses often face challenges like:

* **Scattered or unstructured weather data** from external APIs.
* **Delayed access** to insights due to inefficient data processing.
* **Manual integration** of weather data into dashboards or reports.
* **Inability to scale** as data volume or frequency increases.

### Goal

The goal of this project was to automate the end-to-end data flow—from ingestion of raw weather data to insightful visualizations—using a modern, containerized architecture. The pipeline ensures that decision-makers always have access to fresh and actionable weather insights, enabling them to:

* Proactively respond to adverse weather conditions.
* Optimize routes, resources, and operations.
* Improve forecasting accuracy with historical data trends.

---

## 𝐓𝐨𝐨𝐥𝐬 & 𝐓𝐞𝐜𝐡𝐧𝐢𝐪𝐮𝐞𝐬 𝐔𝐬𝐞𝐝

| Category             | Tool/Technology | Purpose                                                                           |
| -------------------- | --------------- | --------------------------------------------------------------------------------- |
| **Containerization** | Docker          | Containerized deployment of all services for isolated and consistent environments |
| **Orchestration**    | Apache Airflow  | Scheduled and monitored workflow execution of data pipeline                       |
| **Ingestion**        | Weatherxu API   | Source of real-time weather data                                                  |
| **Streaming**        | Apache Kafka    | Real-time data streaming and decoupled message brokering                          |
| **Processing**       | Python 3.x      | ETL logic and Kafka consumers                                                     |
| **Database**         | PostgreSQL      | Structured storage of transformed weather data                                    |
| **BI & Reporting**   | Power BI        | Visualization and analytics dashboard for insights                                |
| **Version Control**  | Git             | Code version tracking and collaboration                                           |

---

## 𝐏𝐫𝐨𝐣𝐞𝐜𝐭 𝐀𝐫𝐜𝐡𝐢𝐭𝐞𝐜𝐭𝐮𝐫𝐞

![Image](https://github.com/user-attachments/assets/e8448bba-99d2-451c-ba23-82a2be5eba89)

## 𝐏𝐫𝐨𝐜𝐞𝐬𝐬 𝐎𝐯𝐞𝐫𝐯𝐢𝐞𝐰

1. **Data Ingestion**

   * Airflow triggers a Python-based Kafka producer to make API calls to Weatherxu at scheduled intervals.
   * Retrieved JSON responses are sent to a Kafka topic.

2. **Real-Time Streaming**

   * Apache Kafka acts as the message broker, ensuring that data is continuously streamed and decoupled from the producer and consumer logic.

3. **Data Transformation**

   * A Kafka consumer (Python script) reads from the Kafka topic.
   * It performs necessary cleaning, transformation, and validation of weather fields such as temperature, humidity, wind speed, etc.

4. **Data Storage**

   * The cleaned data is written to a PostgreSQL database, structured by timestamp, location, and weather parameters.

5. **Visualization**

   * Power BI connects directly to PostgreSQL or its scheduled exports.
   * Dashboards display trends, patterns, and alerts using near real-time data.

6. **Monitoring & Scheduling**

   * Airflow DAGs manage task dependencies, trigger schedules, and provide logs for execution flow and monitoring.

## Airflow UI
![Image](https://github.com/user-attachments/assets/c97bdb22-7d6d-4301-a3ca-6398a7b62df0)

## Database Schema Diagram
![Image](https://github.com/user-attachments/assets/db0c4ae8-435a-403e-8cc4-c45d1e636247)

## Power BI
![Image](https://github.com/user-attachments/assets/08d5e44f-2de9-4fac-bac4-86eba33301dd)
![Image](https://github.com/user-attachments/assets/7a107535-b8fc-476b-9b36-610521933beb)

# Deployment
1. Docker Containers:
   - Create separate containers for Airflow, PostgreSQL, and Python scripts.
   - Use docker-compose to orchestrate multi-container deployment.

2. Environment Setup:
   - Define environment variables for database credentials, API endpoints, and Airflow configurations.

3. Git Repository:
   - Store all code, including Airflow DAGs, Python scripts, and Docker configurations, in a version-controlled Git repository.

