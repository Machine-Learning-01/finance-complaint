# Finance-Complaint 

## Architectures

## WorkFLow setup
### Step-1
Setup secrets 
```bash

```
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

Steps to run project in local system


1. Build docker image
   ```
   docker build -t fc:lts .
   ```
2. Set envment variable
```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
MONGO_DB_URL=
IMAGE_NAME=fc:lts
```
3. To start your application
```
docker-compose up
```
4. To stop your application
```
docker-compose down
``` 