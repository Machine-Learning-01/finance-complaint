from datetime import datetime
import os

PIPELINE_NAME = "finance-complaint"
PIPELINE_ARTIFACT_DIR = os.path.join(os.getcwd(), "finance_artifact")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

DATA_INGESTION_DIR = "data_ingestion"
DATA_INGESTION_DOWNLOADED_DATA_DIR = "downloaded_dir"
DATA_INGESTION_FILE_NAME = "finance_complaint"
DATA_INGESTION_CSV_DATA_DIR = "csv_dir"