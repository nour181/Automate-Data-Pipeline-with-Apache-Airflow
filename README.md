# Automate-Data-Pipeline-with-Apache-Airflow

### Project Introduction:

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### Project Overview:

Make ETL Pipelines to ETL data from S3 Buckets to a AWS Redshift Database, we will create a data pipeline to create tables, stage data from S3 to staging tables, load data into dimensional and fact tables and in the end test data quality of all tables to ensure that every thing is okay


![DAG Overview](https://user-images.githubusercontent.com/54687935/211743274-6b901c2a-40bf-4eb8-9916-1bbce14e6ed4.PNG)


![Tree View](https://user-images.githubusercontent.com/54687935/211743391-6fc6caf7-25da-4780-bcdd-a2afd127f327.PNG)


### Create Data Operators

we will create 5 Custom Operators:
1) create_table.py >>> Operator to create dimensional and fact tables   CreateTableOperator

2) stage_redshift.py >>> Loading data into stage tables (staging_songs, staging_events)    StageToRedshiftOperator

3) load_dimension.py >>> Loading data into diminsional tables (songs, time, artists, users)    LoadDimensionOperator

4) load_fact.py >>> Loading data into fact table (songplays)     LoadFactOperator

5) data_quality >>> Testing the data quality of each table to ensure that data are loaded correctly and there is no table contains zero records    DataQualityOperator


![operators](https://user-images.githubusercontent.com/54687935/211745215-ea93f57f-10d3-404f-a09d-4a3ab2e448d4.PNG)




