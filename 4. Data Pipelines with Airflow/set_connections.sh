!/bin/bash
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get aws_credentials -o json 

# TO-DO: Update the following command with the URI and un-comment it:
airflow connections add aws_credentials --conn-uri 'aws://KEY:SECRET'
#
# TO-DO: Update the following command with the URI and un-comment it:
airflow connections add redshift --conn-uri 'redshift://awsuser:airflow-cluster.cqrkiu1bhedl.us-east-1.redshift.amazonaws.com:5439/dev'

# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
airflow variables set s3_bucket udacity-dend-airflow-marcelo

# TO-DO: un-comment the below line:
airflow variables set s3_prefix data-pipelines