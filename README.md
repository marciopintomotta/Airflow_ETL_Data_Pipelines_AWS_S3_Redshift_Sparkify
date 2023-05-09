
# Creating a Data Pipelines Solution using Airflow ELT S3 Redshift  on AWS for the Music Streaming Service Sparkify

## Project Description 

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in AW3 S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


![Alt text](https://github.com/marciopintomotta/AWS_ELT_Data_Warehouse_S3_2_Redshift_Sparkify/blob/master/Sparkify_S3_to_Redshift_ELT.png "a Redshift ELT")



## Project Motivation

My goal was building an high grade data pipelines solution with Airflow that is dynamic and built from reusable tasks, which can be monitored, and allow easy backfills.

![Alt text](https://github.com/marciopintomotta/Airflow_ETL_Data_Pipelines_AWS_S3_Redshift_Sparkify/blob/master/dag.png " Dag")


![Alt text](https://github.com/marciopintomotta/Airflow_ETL_Data_Pipelines_AWS_S3_Redshift_Sparkify/blob/master/dag_all.png " Dag")


## Project Main Files 

Airflow_ETL_Data_Pipelines_AWS_S3_Redshift_Sparkify/
 *  dags/udac_example_dag.py Python script responsible for running the all the tasks in the ETL data pipeline of Sparkify
 * plugins/operators/stage_redshift.py  Python script responsible for loading data from S3 to Stage area in Redshift.
 *  plugins/operators/load_fact.py Python script responsible for loading fact table.
 *  plugins/operators/load_dimension.py Python script responsible for loading dimensional tables.
 *  plugins/operators/data_quality.py   Python script responsible for running the checks in data in the Redshift tables. 
 
