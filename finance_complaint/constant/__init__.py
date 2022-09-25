from datetime import datetime
from finance_complaint.constant.data_ingestion import *
from finance_complaint.constant.data_validation import *
from finance_complaint.constant.data_transformation import *
from finance_complaint.constant.model_trainer import *
from finance_complaint.constant.model_evaluation import *
from finance_complaint.constant.model_pusher import *
import os

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
DATABASE_NAME = "finance-complaint-db"

S3_MODEL_DIR_KEY = "model-registry"
S3_MODEL_BUCKET_NAME = "finance-complaint"
MODEL_SAVED_DIR = "saved_models"
