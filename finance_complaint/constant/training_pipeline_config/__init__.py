import os

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")

from finance_complaint.constant.training_pipeline_config.data_ingestion import *
from finance_complaint.constant.training_pipeline_config.data_validation import *
from finance_complaint.constant.training_pipeline_config.data_transformation import *
from finance_complaint.constant.training_pipeline_config.model_trainer import *
from finance_complaint.constant.training_pipeline_config.model_evaluation import *
from finance_complaint.constant.training_pipeline_config.model_pusher import *
