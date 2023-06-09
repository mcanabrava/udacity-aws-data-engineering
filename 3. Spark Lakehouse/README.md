### Challenge

In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

### Dataset

1. Customer Records (from fulfillment and the STEDI website)
AWS S3 Bucket URI - s3://cd0030bucket/customers/

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

2. Step Trainer Records (data from the motion sensor):
AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

- sensorReadingTime
- serialNumber
- distanceFromObject

3. Accelerometer Records (from the mobile app):
AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

- timeStamp
- user
- x
- y
- z

### Instructions

As most of this project was done using AWS CLI and console, screenshots will be provided in the [Case Solution and Analysis file](https://github.com/mcanabrava/udacity-aws-data-engineering-nanodegree/blob/main/3.%20Spark%20Lakehouse/Case%20Solution%20and%20Analysis) to highlight steps taken to achieve the desired result and complete the project.
