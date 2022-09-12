from datetime import datetime
from finance_complaint.constant.data_ingestion import *
from finance_complaint.constant.data_validation import *
from finance_complaint.constant.data_transformation import *
from finance_complaint.constant.model_trainer import *
import os

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
