# Background
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Description
We will build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

In this project, we will create a cloud data warehouse on AWS to build an ETL pipeline for a database hosted on Redshift. We will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Datasets
Our two datasets reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

We will use the log data json path file `s3://udacity-dend/log_json_path.json` to extract the data from the folders since we do not have a common prefix in the folders.

# Database Schema
Using the song and event datasets, we create a star schema optimized for queries on song play analysis. This includes the following tables

## Fact Table
- *songplays* - records in event data associated with song plays i.e. records with page `NextSong`

## Dimension Tables
- *users* - users in the app
- *songs* - songs in music database
- *artists* - artists in music database
- *time* - timestamps of records in *songplays* broken down into specific units

## Staging Tables
- *staging_events* - stating table for event data
- *staging_songs* - staging table for song data

We create the database scheme using the `drop` and `create` SQL queries through `create_tables.py`

# ETL Pipeline
We use `etl.py` to pull data from the S3 buckets into staging tables, then move the data into the target database tables. 

1. Exract the song and log data both from S3 buckets.
2. Load the song and long data into staging tables.
3. Insert the data into the target tables within the star schema.

# Execution Steps
We will create the cloud cluster configured with security roles and groups to access the endpoints and S3 buckets through the console. Then, run the python scripts from the terminal to create the tables and ETL the data from the files. 

## Cluster Creation
1. Configure a new Redshift cluster with the following parameters
    - node_type = dc2.large
    - nodes = 4
    - IAM Role = myRedshiftRole
    - VPC Security Group = redshift_security_group
2. Update `dwh.cfg` with relevant configuration parameters

## Script Execution
1. Run `python3 create_tables.py`
2. Run `python3 etl.py`
