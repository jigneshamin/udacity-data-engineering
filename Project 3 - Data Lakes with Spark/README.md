# Background
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Description
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

# Datasets

Our two datasets reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

# Database Schema
Using the song and event datasets, we create a star schema optimized for queries on song play analysis. This includes the following tables

## Fact Table
- *songplays* - records in event data associated with song plays i.e. records with page `NextSong`

## Dimension Tables
- *users* - users in the app
- *songs* - songs in music database
- *artists* - artists in music database
- *time* - timestamps of records in *songplays* broken down into specific units

# ETL Pipeline
We use `etl.py` to pull data from the S3 buckets into staging tables, then move the data into the target database tables. 

1. Load AWS credentials (dl.cfg)
2. Read Sparkify data from S3
3. Using Spark, transform the data and load into the database
4. Load data back into S3

# Script Execution
1. Run `python3 etl.py`
