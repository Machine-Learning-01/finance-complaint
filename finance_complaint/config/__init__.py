from time import strftime
from finance_complaint.entity.config_entity import DataIngestionConfig, PipelineConfig
from finance_complaint.constant import *
from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
import os, sys
import requests
import json
from datetime import datetime
from finance_complaint.entity.metadata_entity import DataIngestionMetadata


class FinanceConfig:

    def __init__(self, pipeline_name=PIPELINE_NAME, timestamp=TIMESTAMP):
        """
        Organization: iNeuron Intelligence Private Limited

        """
        self.timestamp = timestamp
        self.pipeline_name = pipeline_name
        self.pipeline_config = self.get_pipeline_config()

    def get_pipeline_config(self) -> PipelineConfig:
        """
        This function will provide pipeline config information


        returns > PipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])
        """
        try:
            artifact_dir = PIPELINE_ARTIFACT_DIR
            pipeline_config = PipelineConfig(pipeline_name=self.pipeline_name,
                                             artifact_dir=artifact_dir)

            logger.info(f"Pipeline configuration: {pipeline_config}")

            return pipeline_config
        except Exception as e:
            raise FinanceException(e, sys)

    def get_data_ingestion_config(self, from_date=DATA_INGESTION_MIN_START_DATE, to_date=None) \
            -> DataIngestionConfig:

        

        """
        from date can not be less than min start date

        if to_date is not provided automatically current date will become to date        
        
        """

        min_start_date= datetime.strptime(DATA_INGESTION_MIN_START_DATE,"%Y-%m-%d")
        from_date_obj = datetime.strptime(from_date,"%Y-%m-%d")
        if from_date_obj< min_start_date:
            from_date=DATA_INGESTION_MIN_START_DATE
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")

        """
        master directory for data ingestion
        we will store metadata information and ingested file to avoid redundant download
        """
        data_ingestion_master_dir =  os.path.join(self.pipeline_config.artifact_dir,
                                          DATA_INGESTION_DIR)
        
        #time based directory for each run
        data_ingestion_dir =os.path.join(data_ingestion_master_dir,
                                          self.timestamp)


        metadata_file_path=os.path.join(data_ingestion_master_dir,DATA_INGESTION_METADATA_FILE_NAME)


        data_ingestion_metadata = DataIngestionMetadata(metadata_file_path=metadata_file_path)

        if data_ingestion_metadata.is_metadata_file_present:
            metadata_info = data_ingestion_metadata.get_metadata_info()
            from_date=metadata_info.to_date



        data_ingestion_config = DataIngestionConfig(
            from_date=from_date,
            to_date=to_date,
            data_ingestion_dir=data_ingestion_dir,
            download_dir=os.path.join(data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR),
            file_name=DATA_INGESTION_FILE_NAME,
            data_dir=os.path.join(data_ingestion_master_dir, DATA_INGESTION_MASTER_DATA_DIR),
            failed_dir=os.path.join(data_ingestion_dir, DATA_INGESTION_FAILED_DIR),
            metadata_file_path=metadata_file_path

        )
        logger.info(f"Data ingestion config: {data_ingestion_config}")
        return data_ingestion_config
