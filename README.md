
# Creating a Data Pipelines Solution using Airflow ELT S3 Redshift  on AWS for the Music Streaming Service Sparkify

## Project Description 

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in AW3 S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


![Alt text](https://github.com/marciopintomotta/AWS_ELT_Data_Warehouse_S3_2_Redshift_Sparkify/blob/master/Sparkify_S3_to_Redshift_ELT.png "a Redshift ELT")



## Project Motivation

My goal was building an high grade data pipelines solution with Airflow that is dynamic and built from reusable tasks, which can be monitored, and allow easy backfills.

![Alt text](https://github.com/marciopintomotta/Airflow_ETL_Data_Pipelines_AWS_S3_Redshift_Sparkify/blob/master/dag.png " Dag")


![Alt text](https://github.com/marciopintomotta/Airflow_ETL_Data_Pipelines_AWS_S3_Redshift_Sparkify/blob/master/dag_all.png " Dag")


## Project Struct 

The project contains the following Struct:

- dags: This folder contains the Python files for Airflow DAGs.
   
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.


## Project Main Files 



