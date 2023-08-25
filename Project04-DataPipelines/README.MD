# Project: Data Pipelines

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The pipeline will move data from AWS S3 to AWS Redshift where it will be processed into Fact and Dimension Tables to facilitate analytics peformed by downstream consumers.  The source data consists of JSON logs that tell about user acitivity in the application and JSON metadata about the songs the users listen to.

## Prerequisites

### Permissions and Connections
* IAM User in AWS
* Redshift Serverless configured to allow access from Airflow, and access to S3 buckets
* Connection between Airflow and AWS
* Connection between Airflow and AWS Redshift Serverless

### Datasets
* Log data                  s3://udacity-dend/log_data
* Song data                 s3://udacity-dend/song_data
* log_json_path.json file   s3://udacity-dend/log_json_path.json

## Airflow DAGs

A prerequisite of this pipeline is the existance of the nessesary tables to load data:
* staging_events
* staging_songs
* songplays
* songs
* artists
* users
* time

These tables can be created directly in Redshift using the statements in 'final_project_create_tables_sql.py' or optionally, you can run the included DAG 'final_projects_create_tables.py' to set up your enviornment.

### Final_project DAG
#### Tasks
* Begin_execution:  Dummy operator to capture DAG start
* Stage_events:  Copy event data from S3 to Redshift stage table
* Stage_songs:  Copy song data from S3 to Redshift stage table
* Load_songplays_fact_table:  Transform and Load data from stage to fact table
* Load_user_dim_table:  Transform and Load data to users dimension table
* Load_song_dim_table:  Transform and Load data to song dimension table
* Load_artist_dim_table:  Transform and Load data to artist dimension table
* Load_time_dim_table:  Transform and Load data to time dimension table
* Run_data_quality_checks:  Run data quality checks on fact and dimension tables to ensure they are populated
* End_execution:  Dummy operator to catpure DAG end

## Project Struture
* /dags/udacity/common/
    * final_project_sql_statements.py:  Redshift SQL queries used by the final_project DAG
    * final_project_create_tables_sql.py:  OPTIONAL:  Redshift SQL queries used to create tables required for DAG.  Can also be mannual created in Redshift

* /plugins/final_project_operators/
    * stage_redshift.py:  Operator to copy from S3 to Redshift Stage Tables
    * load_fact.py:  Operator to read from stage and load fact table(s)
    * load_dimension.py:  Operator to read from stage/fact tables and load dimension table(s)
    * data_quality.py:  Operator to perform data quality checks on list of tables

* /dags/project/
    * final_project.py:  DAG to copy data from S3 to Redshift Stage Tables, Load Data to Fact and Dimension Tables, and perform data qulaity checks.
    * final_project_create_tables.py:  OPTIONAL: DAG to set up your enviornment by creating the nessesary tables.  Designed to be run ad-hoc.

## Images
Proof DAG runs sucessfully:
![DAG Sucess](https://github.com/MarkMendes/Udacity-Data-Engineer-Nano-Degree/blob/main/Project04-DataPipelines/img/Graph%20View.PNG)

Graph View:
![DAG Graph](https://github.com/MarkMendes/Udacity-Data-Engineer-Nano-Degree/blob/main/Project04-DataPipelines/img/Graph%20View.PNG)

Proof Data Quality Operator runs correctly:
![DQ Log](https://github.com/MarkMendes/Udacity-Data-Engineer-Nano-Degree/blob/main/Project04-DataPipelines/img/Data%20Quality%20Logs%20Sucess.PNG)



