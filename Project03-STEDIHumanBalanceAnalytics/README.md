# Project 03: STEDI Human Balance Analytics

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.  The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

This project will create a lakehouse by placing raw files from the STEDI app into a landing zone, clean the data into a trusted zone, and curate the date for use by the STEDI data science team in a curated zone.  The lakehouse will leverage AWS S3, Glue, and Athena.

## Structure

- scripts:  A folder containing AWS Glue python scripts and SQL table creation scripts.
- images:  A folder containing screenshots of AWS Athena queries

## Project Data

1. Customer Records (from fulfillment and the STEDI website):

https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter/customer
AWS S3 Bucket URI - s3://cd0030bucket/customers/

contains the following fields:

serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate
customername
email
lastupdatedate
phone
sharewithfriendsasofdate

2. Step Trainer Records (data from the motion sensor):
https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter/step_trainer
AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

contains the following fields:

sensorReadingTime
serialNumber
distanceFromObject

3. Accelerometer Records (from the mobile app):
https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter/accelerometer
AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

contains the following fields:

timeStamp
user
x
y
z


## Project Solution
This project will build a lakehouse for the STEDI data team.  The lakehouse will consist of three zones:
- landing: raw data
- trusted: landing zone data that has been filtered and cleansed
- curated: data from the trusted zone with business level aggregation

In each layer data will be stored as JSON in AWS S3 buckets.  AWS Athena tables will be created to allow querying of this data.

1.  Copy data from source repository to S3 landing zones
2.  customer_landing_to_trusted.py will remove customer records who have not aggreed to share their data for research purposes.  It will place the filtered customer data into the customer landing zone.
3.  accelerometer_landing_to_trusted.py will leverage the customer landing zone data to filter accelerometer data only for customers that have agreed to share their data for research.
4.  customer_landing_to_curated.py will leverage the customer trusted zone and acceleromter trusted zone to create curated customer data that onyl containes cusomters who have agreed to share their data for research and who have acceleromter data available.
5.  step_trainer_landing_to_trusted will leverage the customer trusted zone to filter step trainer data only for customers that have agreed to share their data for research.
6.  trainer_trusted_to_curated will create an aggregated table of step trainer data and assocaited acceleromter data by leveraging the step trainer trusted and acceleromter trusted zones.

Checks
1. accelerometer_landing.png evidences that we have sucessfuly brought in raw acceleromter data
2. customer_landing.png evidences that we have sucessfuly brought in raw customer data
3. customer_trusted.png evidences that we have sucessfuly filtered out customers in the customer trusted zone that have opted out of sharing their data for research 

