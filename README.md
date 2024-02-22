# Data Pipeline with Reddit, Airflow, S3, Snowflake, Snowpipe and Snowsight

This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into a Snowflake data warehouse. The pipeline leverages a combination of tools and services including Apache Airflow, Amazon S3 and snowflake services.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)

## Overview

The pipeline is designed to:

1. Extract data from Reddit using its API.
2. Store the raw data into an S3 bucket from Airflow.
3. Transform the data
4. Load the transformed data into Snowflake for for analytics and querying.

## Architecture
![RedditDataEngineering.png](assets%2FRedditDataEngineering.png)
1. **Reddit API**: Source of the data.
2. **Apache Airflow**: Orchestrates the ETL process and manages task distribution.
3. **Amazon S3**: Transformed Reddit data storage.
6. **Snowflake**: SQL-based data transformation.
7. **Snowsight**: Data warehousing and analytics.

## Prerequisites
- AWS Account with appropriate permissions for S3.
- Reddit API credentials.
- Docker Installation
- Python 3.9 or higher

## System Setup
1. Clone the repository.
   ```bash
    git clone https://github.com/
   ```
2. Create a virtual environment.
   ```bash
    python3 -m venv venv
   ```
3. Activate the virtual environment.
   ```bash
    source venv/bin/activate
   ```
4. Install the dependencies.
   ```bash
    pip install -r requirements.txt
   ```
5. Rename the configuration file and the credentials to the file.
   ```bash
    mv config/config.conf.example config/config.conf
   ```
6. Starting the containers
   ```bash
    docker-compose up -d
   ```
7. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```
