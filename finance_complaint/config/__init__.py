from time import strftime
from finance_complaint.entity.config_entity import DataIngestionConfig, PipelineConfig
from finance_complaint.constant import *
from finance_complaint.logger import logger
from finance_complaint.exception import FinanceException
import os, sys
import requests
import json
from datetime import datetime


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

    def get_data_ingestion_config(self, from_date="2011-12-01", to_date=None,
                                  file_format: str = "json") \
            -> DataIngestionConfig:
        """

        from_date = "2012-11-31"
        to_date = "2015-11-31"
        file_format =
        ========================================================================================
        DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                                 "to_date",
                                                                 "file_format"
                                                                 "data_ingestion_dir",
                                                                 "downloaded_data"
                                                                 ])
        """
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")

        data_ingestion_dir = os.path.join(self.pipeline_config.artifact_dir,
                                          DATA_INGESTION_DIR,
                                          self.timestamp)

        data_ingestion_config = DataIngestionConfig(
            from_date=from_date,
            to_date=to_date,
            file_format=file_format,
            data_ingestion_dir=data_ingestion_dir,
            download_dir=os.path.join(data_ingestion_dir, DATA_INGESTION_DOWNLOADED_DATA_DIR),
            file_name=DATA_INGESTION_FILE_NAME,
            data_dir=os.path.join(data_ingestion_dir, DATA_INGESTION_DATA_DIR),
            failed_dir=os.path.join(data_ingestion_dir, DATA_INGESTION_FAILED_DIR)
        )
        logger.info(f"Data ingestion config: {data_ingestion_config}")
        return data_ingestion_config
