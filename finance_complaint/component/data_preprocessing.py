from binhex import FInfo
import re
from tkinter import E
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.artifact_entity import DataIngestionArtifact
from pyspark.sql.functions import col, desc
from finance_complaint.entity.config_entity import DataPreprocessingConfig
from finance_complaint.entity.spark_manager import spark_session
from pyspark.sql import DataFrame
import os, sys
from typing import List, Dict
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, TimestampType
from finance_complaint.entity.complaint_column import  ComplaintColumn
from collections import namedtuple

COMPLAINT_TABLE = "complaint"


class DataPreprocessing(ComplaintColumn):

    def __init__(self,
                 data_preprocessing_config: DataPreprocessingConfig, data_ingestion_artifact: DataIngestionArtifact,
                 table_name: str = COMPLAINT_TABLE

                 ):
        try:
            super().__init__()
            self.data_ingestion_artifact: DataIngestionArtifact = data_ingestion_artifact
            self.data_preprocessing_config = data_preprocessing_config
            self.table_name = table_name
        except Exception as e:
            raise FinanceException(e, sys) from e

    def create_complaint_table(self, dataframe: DataFrame) -> None:
        try:
            dataframe.createOrReplaceTempView(self.table_name)
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

    def get_missing_report(self, dataframe: DataFrame, ) -> List[List[Row]]:
        try:
            logger.info(f"Preparing missing reports for each column")
            missing_reports = []
            number_of_row = dataframe.count()
            for column in dataframe.columns:
                print(column)
                query = f"""
                select 
                {number_of_row} as  total_row,
                count(*) as null_row_{column},
                (count(*)*100)/{number_of_row} as missing_percentage,
                '{column}' as  column_name 
                from {self.table_name} 
                where {column} is null
                """
                logger.info(f"Query: {query}")
                response = spark_session.sql(query).collect()
                missing_reports.append(response)
                logger.info(f"Column: {column} missing report: {response}")
            logger.info(f"Missing report prepared.")
            return missing_reports

        except Exception as e:
            raise FinanceException(e, sys)

    def get_unwanted_and_high_missing_value_columns(self, dataframe: DataFrame, threshold: float = 0.2) -> List[str]:
        try:
            missing_reports: List[List[Row]] = self.get_missing_report(dataframe=dataframe)
            unwanted_column: List[str] = self.unwanted_columns
            for missing_report in missing_reports:
                missing_info = missing_report[0]
                if missing_info.missing_percentage > (threshold * 100):
                    unwanted_column.append(missing_info.column_name)
                    logger.info(f"{missing_info.column_name} has {missing_info.missing_percentage}")
            unwanted_column = list(set(unwanted_column))
            return unwanted_column

        except Exception as e:
            raise FinanceException(e, sys)

    @staticmethod
    def get_unique_values_of_each_column(dataframe: DataFrame) -> None:
        try:
            for column in dataframe.columns:
                n_unique: int = dataframe.select(col(column)).distinct().count()
                n_missing: int = dataframe.filter(col(column).isNull()).count()
                missing_percentage: float = (n_missing * 100) / dataframe.count()
                logger.info(f"Column: {column} contains {n_unique} value and missing perc: {missing_percentage} %.")
        except Exception as e:
            raise FinanceException(e, sys)

    @staticmethod
    def get_top_category(dataframe: DataFrame, columns: List[str]) -> Dict[str, str]:
        try:
            top_category = dict()
            for column in columns:
                category_count_by_desc: DataFrame = dataframe.groupBy(column).count().filter(
                    f'{column} is not null').sort(desc('count'))
                top_cat = category_count_by_desc.take(1)[0]
                logger.info(f"Column: [{top_cat}]: -->{top_cat}")
                top_category_value = category_count_by_desc.take(1)[0][column]
                top_category[column] = top_category_value

            return top_category
        except Exception as e:
            raise FinanceException(e, sys)

    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(filter(lambda x: x in self.required_columns,
                                  dataframe.columns))

            if len(columns) != len(self.required_columns):
                raise Exception(f"Required column missing\n\
                 Expected columns: {self.required_columns}\n\
                 Found columns: {columns}\
                 ")

        except Exception as e:
            raise FinanceException(e, sys)

    def add_derived_features(self, dataframe: DataFrame) -> DataFrame:
        try:
            dataframe = dataframe.withColumn(self.col_date_sent_to_company,
                                             col(self.col_date_sent_to_company).cast(TimestampType()))
            dataframe = dataframe.withColumn(self.col_date_received, col(self.col_date_received).cast(TimestampType()))
            dataframe = dataframe.withColumn("diff_in_days", (
                    col('date_sent_to_company').cast(LongType()) - col('date_received').cast(LongType())) / (
                                                     60 * 60 * 24))
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def handle_missing_values(self, dataframe: DataFrame) -> DataFrame:
        try:
            self.get_unique_values_of_each_column(dataframe=dataframe)

            # Fill missing values for categorical columns
            columns: List[str] = self.one_hot_encoding_features + self.frequency_encoding_features
            top_category = self.get_top_category(dataframe=dataframe, columns=columns)

            logger.info(top_category)

            difference_in_days: str = self.numerical_features[0]

            avg_days = \
                dataframe.filter(f"{difference_in_days} is not null").agg({difference_in_days: "avg"}).take(1)[0][0]

            # Fill missing values for Numerical feature
            top_category.update({difference_in_days: avg_days})
            dataframe = dataframe.na.fill(top_category)

            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def drop_row_without_target_label(self, dataframe: DataFrame) -> DataFrame:
        try:
            total_rows: int = dataframe.count()
            logger.info(f"Number of row: {total_rows} and column: {len(dataframe.columns)}")

            # Drop row if target value is unknown
            logger.info(f"Dropping rows without target value.")
            dataframe = dataframe.filter(f"{self.target_column}!= 'N/A'")
            logger.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")

            dropped_percentage_row: float = (dataframe.count() * 100) / total_rows
            logger.info(f"{dropped_percentage_row} % row removed.")
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_preprocessing(self):
        try:
            logger.info(f"Initiating data preprocessing.")
            dataframe: DataFrame = self.read_data()

            self.drop_row_without_target_label(dataframe=dataframe)

            # drop column if more than 20% of missing value present

            # derived features
            dataframe = self.add_derived_features(dataframe=dataframe)

            logger.info(f"Created temporary {COMPLAINT_TABLE} table")
            self.create_complaint_table(dataframe=dataframe)

            # impute missing value
            unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(dataframe=dataframe, )
            logger.info(f"Dropping feature: {','.join(unwanted_columns)}")
            dataframe: DataFrame = dataframe.drop(*unwanted_columns)

            logger.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")

            # validation to ensure that all require column available
            self.is_required_columns_exist(dataframe=dataframe)

            dataframe = self.handle_missing_values(dataframe=dataframe)

            logger.info("Saving preprocessed data.")
            print(f"Row: [{dataframe.count()}] Column: [{len(dataframe.columns)}]")
            print(f"Expected Column: {self.required_columns}\nPresent Columns: {dataframe.columns}")
            dataframe.write.parquet(self.data_preprocessing_config.preprocessed_data_file_path)
            # transform features.
        except Exception as e:
            raise FinanceException(e, sys)


if __name__ == "__main__":
    data_file_path = '/home/jovyan/work/finance_complaint/finance_artifact/data_ingestion/feature_store/finance_complaint'
    data_ingestion_artifact = DataIngestionArtifact(data_file_path=data_file_path)
    data_preprocessing = DataPreprocessing(data_ingestion_artifact=data_ingestion_artifact)
    data_preprocessing.initiate_data_preprocessing()
