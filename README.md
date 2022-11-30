# Finance-Complaint 


# Sensor-Fault-Detection

### Problem Statement
Complaints can give us insights into problems people are experiencing in the marketplace and help us to undestand the reason and do necessary modification in exisiting financial product if required.



### Solution Proposed 
By understanding existing complaints registered against financial products we can create an ML model that can help us to identify newly registered complaints whether they are problematic or not and accordingly company can take quick action to resolve the issue, and satisfy the customer's need.

The problem is to identify registered complaint will be disputed by customer or not.
## Tech Stack Used
1. Python 
2. PySpark
3. PySpark ML
4. Airflow as Scheduler
5. MongoDB


## Infrastructure Required.

1. GCP Compute Engine
2. S3 Bucket
3. Artifact Registry

## Dashboarding
1. Grafana
2. Prometheus
3. Node Exporter
4. Promtail
5. Loki

## How to run?

## WorkFLow setup

Create .env file

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
MONGO_DB_URL=
TRAINING=1
PREDICTION=1
```
1- Trigger
0- Bypass

Build docker image
```
docker build -t tc:lts .
```

Lauch docker image

```
docker run -it -v $(pwd)/finance_artifact:/app/finance_artifact  --env-file=$(pwd)/.env fc:lts
```

Steps to run project in local system


1. Build docker image
   ```
   docker build -t fc:lts .
   ```
2. Set envment variable
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export MONGO_DB_URL=
export AWS_DEFAULT_REGION="ap-south-1"
export IMAGE_NAME=fc:lts
```
3. To start your application
```
docker-compose up
```
4. To stop your application
```
docker-compose down
``` 



In your local system to setup airflow 


AIRFLOW SETUP

## How to setup airflow

Set airflow directory
```
export AIRFLOW_HOME="/home/avnish/census_consumer_project/census_consumer_complaint/airflow"
```

To install airflow 
```
pip install apache-airflow
```

To configure databse
```
airflow db init
```

To create login user for airflow
```
airflow users create  -e avnish@ineuron.ai -f Avnish -l Yadav -p admin -r Admin  -u admin
```
To start scheduler
```
airflow scheduler
```
To launch airflow server
```
airflow webserver -p <port_number>
```

Update in airflow.cfg
```
enable_xcom_pickling = True
```
