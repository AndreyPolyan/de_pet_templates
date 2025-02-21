# **Data Engineering Pet Projects**

This repository contains a collection of Data Engineering projects that demonstrate expertise in modern data architectures, ETL/ELT workflows, streaming data processing, and analytical data warehouses. Each project showcases a different aspect of Data Engineering, utilizing a variety of industry-standard tools and frameworks.

## **Project Overview**

| **Project**  | **Tech Stack**  | **Description**  |
|-------------|----------------|------------------|
| **Project 1: Inmon Data Warehouse with BI** | Docker, Python, PostgreSQL, Airflow, MongoDB, Apache Superset | A classical Inmon-style Data ** (3-layer architecture: **STG, DDS, CDM**) designed to integrate multiple data sources. This project includes automated ETL workflows managed by Apache Airflow data transformation processes, and BI reporting in Apache Superset to facilitate data-driven decision-making. |
| **Project 2: Real-Time Streaming with Microservices** | Docker, Python, Flask, Kafka, PostgreSQL | A real-time streaming pipeline that processes data using event-driven microservices. This project simulates real-time data ingestion, transformations, and enrichment on the fly using Python-based services that consume and process Kafka events, ultimately storing results in **PostgreSQL**. |
| **Project 3: Kimball Data Warehouse with dbt** | Docker, Python, Vertica, dbt, Airflow, S3 | A Kimball-style Data Warehouse with a two-layer structure (**staging and marts**) implemented in Vertica Data is ingested from S3-type storage, Airflow orchestrates EL processes and dbt is used for data transformation into analytical marts. |
| **Project 4: Data Processing on HDFS with Spark** | HDFS, Apache Spark, Airflow | A Big Data processing pipeline using **Apache Spark** on **HDFS**. This project focuses on batch data processing, distributed computation, and data transformation to generate structured datamarts from raw data stored in HDFS. |
| **Project 5: Streaming Analytics with Spark Streaming** | Apache Spark Streaming, Kafka | A real-time analytics pipeline utilizing **Apache Spark Streaming** to process and analyze Kafka events in real time. The project demonstrates stateful streaming window-based aggregation and low-latency processing**techniques to derive actionable insights from streaming data. |

---

## **Repository Structure**
Each project is self-contained, including:
- **Source Code**: Scripts for data processing, ingestion, and transformations.
- **Infrastructure as Code (if applicable)**: Docker and configuration files for setting up the environment.
- **Documentation (if applicable)**: Step-by-step instructions to deploy and run the projects.

These projects serve as practical examples of data engineering workflows, covering batch and real-time processing, data modeling, and ETL orchestration.
