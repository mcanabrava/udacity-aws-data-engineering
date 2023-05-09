## Introduction

The purpose of this file is to organize and demonstrate steps taken to create the Spark Lakehouse for STEDI in AWS.

## Assets
### Buckets

- Data was copied from Udacity to udacity-spark-aws-test-bucket using AWS CLI. This bucket contains the subfolders accelerometer/, customer/, and step_trainer/ each with it's respective landing and trusted zones.

### Glue Tables

- accelerometer_landing
- customer_landing

<br></br>
(only customer records that agreed to share their data)
- accelerometer_trusted
- customer_trusted 

<br></br>
(only customer data that has accelerometer data)
- customers_curated

<br></br>
(step trainer records data for curated customers)
- step_trainer_trusted

<br></br>
(aggregated table with data for each step trainer reading and associated accelerometer data for the same timestamp)
- machine_learning_curated

### Glue Scripts

The Glue Scripts folder contains PySpark scripts to perform the ETL jobs to move data across buckets, supporting the creation of the tables.

## Instructions

1. Assuming data is already available in the bucket, create two Glue Tables in the console for the accelerometer and customer landing data. The scripts for generating the tables can be found in the SQL Scripts folder.

You will need to create an Athena DB to query the tables and their results shoud look as following:

customer_landing

![customer_landing](/images/customer_landing.png)

accelerometer_landing

![accelerometer_landing](/images/accelerometer_landing.png)

2. To filter customer records that agreed to share their data, let's now create two Glue Jobs in the console and generate the trusted tables for the landing ones we already have.

The end results should look as follows:

![customer_trusted](/images/customer_trusted.png)

An additional check can be performed to ensure only appropriate data is being used using the query below:

![verifying_nulls_customer_trusted](/images/verifying_nulls_customer_trusted.png)


The scripts for the Glue Jobs can be found in the Glue Scripts folder.

3. Now, we should create a curated table for customers who also have accelerometer data using a Glue Job as we did before. The job should look as follows:

![customer_curated_job](/images/customer_curated_job.png)

In this job, we are also eliminating duplicates based on the email column and filtering timestamp >= 'shareWithResearchAsOfDate' for each customer so we can filter out any readings that were prior to the research consent date. This will ensure consent was in place at the time that data was gathered. This helps in the case that in the future the customer revokes consent. We can be sure that the data we used for research was used when consent was in place for that particular data.

We can create the tables directly through Athena from S3 or using a Glue Crawler. The second approach takes a bit longer as crawlers have to go through the data to define a schema but they are able to automatically create it without prompting you for the schema definition.

The original table without removing the duplicates would display > 600k records given the way the join was done, but filtering for distinct customers should give us way fewer records.

![customers_curated_distinct](/images/customers_curated_distinct.png)

4. To get the additional files provided, we should navigate to the directory they are located in AWS Cloushell and run the following command: 

aws s3 cp . s3://udacity-spark-aws-test-bucket/step_trainer/landing/ --recursive

This will automatically create a new bucket and copy all the files to this bucket

5. We are now able to create a Glue Table step_trainer_trusted, create the step_trainer_trusted.py job and query the data in Athena to verify step trainer data for this set of customers.

Querying the distinct serial numbers of the new step_trainer_trusted table should give us around ~30k records.

![step_trainer_trusted_distinct](/images/step_trainer_trusted_distinct.png)

6. Finally, we should create an aggreagated table containing each of Step Trainer records and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data. For this, we need to use the previously created table accelerometer_trusted together with the step_trainer_trusted joining them on the timestamp column.


![final_job](/images/final_job.png)

![final_results](/images/final_results.png)
