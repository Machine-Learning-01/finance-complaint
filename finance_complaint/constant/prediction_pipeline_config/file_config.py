import os
from datetime import datetime
from finance_complaint.constant import TIMESTAMP

S3_DATA_BUCKET_NAME = "finance-cat-service"
ROOT_DATA_DIR_NAME = "finance_data"
ARCHIVE_DIR_NAME = "archive"
INPUT_DIR_NAME = "input"
PREDICTION_DIR_NAME = "prediction"
FAILED_DIR_NAME = "failed"
REGION_NAME = "ap-south-1"
PYSPARK_S3_URL_FORMAT = "s3a://"
INPUT_FILE_NAME = "finance_complaint"
PYSPARK_S3_ROOT = os.path.join(PYSPARK_S3_URL_FORMAT, S3_DATA_BUCKET_NAME)
ROOT_DATA_DIR = os.path.join(ROOT_DATA_DIR_NAME)
ARCHIVE_DIR = os.path.join(ROOT_DATA_DIR, ARCHIVE_DIR_NAME, TIMESTAMP)
INPUT_DIR = os.path.join(ROOT_DATA_DIR, INPUT_DIR_NAME, INPUT_FILE_NAME)
PREDICTION_DIR = os.path.join(ROOT_DATA_DIR, PREDICTION_DIR_NAME, TIMESTAMP)
FAILED_DIR = os.path.join(ROOT_DATA_DIR, FAILED_DIR_NAME, TIMESTAMP)
