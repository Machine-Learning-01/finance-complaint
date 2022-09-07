from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.config import FinanceConfig
from finance_complaint.component.data_ingestion import DataIngestion
from finance_complaint.component.data_preprocessing import DataPreprocessing
from finance_complaint.entity.artifact_entity import DataIngestionArtifact

import sys
class Pipeline():

    def __init__(self,finance_config:FinanceConfig):
        self.finance_config:FinanceConfig=finance_config
        

    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            data_ingestion_config = self.finance_config.get_data_ingestion_config()
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingestion_artifact

        except Exception as e:
            raise FinanceException(e,sys) 

    def start_data_preprocessing(self,data_ingestion_artifact:DataIngestionArtifact):
        try:
            data_preprocessing_config = self.finance_config.get_data_preprocessing_config()
            data_preprocessing = DataPreprocessing(data_ingestion_artifact=data_ingestion_artifact,
                data_preprocessing_config=data_preprocessing_config)
            
            data_preprocessing.initiate_data_preprocessing()

        except Exception as e:
            raise FinanceException(e,sys)

    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()

            self.start_data_preprocessing(data_ingestion_artifact=data_ingestion_artifact)

        except Exception as e:
            raise FinanceException(e,sys)
