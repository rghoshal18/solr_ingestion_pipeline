# Solr Ingestion Pipeline

## Overview

The Solr Ingestion Pipeline is an Airflow-based data ingestion solution that automates the process of loading data 
into a Solr collection from files hosted on Google Drive and sending a email notification on completion. It consists 
of a main DAG (`solr_ingestion_pipeline`) and a sub-DAG (`entity_ingestion`) for handling entity-specific data ingestion.

## Project Structure

- **mnt/airflow/dags/**: Contains the main DAG script (`solr_ingestion_pipeline.py`) and the sub-DAG factory 
script (`entity_ingestion/entity_ingestion_sub_dag.py`).
- **scripts/**: Holds the ETL (Extract, Transform, Load) script (`etl.py`) used for data ingestion.

## Requirements

The following software is required for the complete workflow (from git clone to the running Docker Container). 
The specified versions are the tested ones. Other versions should also work.

 * Git 2.39.3
 * Docker 24.0.7

## Installation and Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/rghoshal18/solr_ingestion_pipeline.git
   cd solr_ingestion_pipeline

2. Set the .env file
   
   ```.env

   ```

3. Start the application:

   ```bash
   sh start.sh
   ```
   This should start all the required services for the airflow. 
   The first installation would take some time

