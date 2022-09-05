from datetime import datetime
import os

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

DATA_INGESTION_DIR = "data_ingestion"
DATA_INGESTION_DOWNLOADED_DATA_DIR = "downloaded_data"
DATA_INGESTION_FILE_NAME = "finance_complaint"
DATA_INGESTION_MASTER_DATA_DIR = "feature_store"
DATA_INGESTION_FAILED_DIR = "failed_dir"
DATA_INGESTION_METADATA_FILE_NAME = "meta_info.yaml"
DATA_INGESTION_MIN_START_DATE = "2011-12-01"

