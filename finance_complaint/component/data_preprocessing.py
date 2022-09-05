from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from finance_complaint.entity.spark_manager import spark_session
from pyspark.sql import DataFrame
import os, sys
from typing import List


class DataPreprocessing:

    def __init__(self, data_ingestion_artifact: DataIngestionArtifact):

        self.data_ingestion_artifact: DataIngestionArtifact = data_ingestion_artifact

    @staticmethod
    def update_column_as_attribute(dataframe: DataFrame) -> DataFrame:
        """
        dataframe: pyspark dataframe
        """
        try:
            columns: List[str] = dataframe.columns
            for column in columns:
                setattr(dataframe, column, column)
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(
                self.data_ingestion_artifact.data_file_path
            )
            logger.info(f"Data frame is created using file: {self.data_ingestion_artifact.data_file_path}")
            logger.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_preprocessing(self):
        try:
            dataframe: DataFrame = self.read_data()
            dataframe:DataFrame = self.update_column_as_attribute(dataframe=dataframe)

        except Exception as e:
            raise FinanceException(e, sys)


if __name__=="__main__":

    data_file_path=  '/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/data_ingestion/feature_store/finance_complaint'
    data_ingestion_artifact = DataIngestionArtifact(data_file_path=data_file_path)
    data_preprocessing = DataPreprocessing(data_ingestion_artifact=data_ingestion_artifact)
    data_preprocessing.initiate_data_preprocessing()
