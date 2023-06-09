﻿### Challenge

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

The image below represents the final DAG to be developed after creating your own custom operators.

![example_dag](images/example-dag.png)

### Dataset

For this project, you'll be working with two datasets. Here are the s3 links for each:

- Log data: s3://udacity-dend/log_data
- Song data: s3://udacity-dend/song_data

For the song data, I decided to used only s3://udacity-dend/song_data/A/A/A/ due to insufficient space for storing all the data. The source files will be used to create the following tables:

- staging_events
- staging_songs
- songplays fact table: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- users dimensional table: user_id, first_name, last_name, gender, level
- songs dimensional table: song_id, title, artist_id, year, duration
- artists dimensional table: artist_id, name, location, lattitude, longitude
- time dimensional table: start_time, hour, day, week, month, year, weekday

### Instructions

1. Start Airflow with your local installation.
    
2. Create your own resources:
    - bucket to copy data from Udacity's repository
    - IAM user for Airflow to access S3 and Redshift
    - Redshift cluster or serverless

    It is also possible to directly create these last 2 using the dwh.cfg and setup.ipynb files provided in this folder.

3. Create the tables that will be filled with data when running the dag using the create_tables queries. This can be done directly in Redshift console.

4. Open Airlfow and run the DAG
