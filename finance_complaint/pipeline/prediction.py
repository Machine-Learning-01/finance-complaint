from finance_complaint.constant.model import S3_MODEL_BUCKET_NAME, S3_MODEL_DIR_KEY
from finance_complaint.constant.prediction_pipeline_config.file_config import S3_DATA_BUCKET_NAME, PYSPARK_S3_ROOT
from finance_complaint.entity.config_entity import PredictionPipelineConfig
from finance_complaint.entity.schema import FinanceDataSchema
from pyspark.sql import DataFrame
from finance_complaint.cloud_storage import SimpleStorageService
from finance_complaint.exception import FinanceException
import os, sys
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.logger import logger
from typing import List, Tuple
from finance_complaint.entity.estimator import S3FinanceEstimator


class PredictionPipeline:

    def __init__(self, pipeline_config=PredictionPipelineConfig()) -> None:
        self.__pyspark_s3_root = PYSPARK_S3_ROOT
        self.pipeline_config: PredictionPipelineConfig = pipeline_config
        self.s3_storage: SimpleStorageService = SimpleStorageService(s3_bucket_name=S3_DATA_BUCKET_NAME,
                                                                     region_name=pipeline_config.region_name)
        self.schema: FinanceDataSchema = FinanceDataSchema()

    def read_file(self, file_path: str) -> DataFrame:
        try:
            file_path = self.get_pyspark_s3_file_path(dir_path=file_path)
            df =  spark_session.read.parquet(file_path)
            return df.limit(100)
        except Exception as e:
            raise FinanceException(e, sys)

    def write_file(self, dataframe: DataFrame, file_path: str) -> bool:
        try:

            if file_path.endswith("csv"):
                file_path = os.path.dirname(file_path)

            file_path = self.get_pyspark_s3_file_path(dir_path=file_path)
            print(file_path)
            logger.info(f"writing parquet file at : {file_path}")
            dataframe.write.parquet(file_path,mode="overwrite")
            return True
        except Exception as e:
            raise FinanceException(e, sys)

    def get_pyspark_s3_file_path(self, dir_path) -> str:
        return os.path.join(self.__pyspark_s3_root, dir_path)

    def is_valid_file(self, file_path) -> bool:
        """
        file_path
        """
        try:
            dataframe: DataFrame = self.read_file(file_path)
            columns = dataframe.columns
            missing_columns = []
            for column in self.schema.required_prediction_columns:
                if column not in columns:
                    missing_columns.append(column)
            if len(missing_columns) > 0:
                logger.info(f"Missing columns: {missing_columns}")
                logger.info(f"Existing columns: {columns}")
                return False
            return True
        except Exception as e:
            raise FinanceException(e, sys)

    def get_valid_files(self, file_paths: List[str]) -> Tuple[List[str], List[str]]:
        """
        Returns: Tuple containing two items
        item1: valid file name list
        item2: invalid file name list
        """
        try:
            valid_file_paths: List[str] = []
            invalid_file_paths: List[str] = []
            for file_path in file_paths:
                is_valid = self.is_valid_file(file_path=file_path)
                if is_valid:
                    valid_file_paths.append(file_path)
                else:
                    invalid_file_paths.append(file_path)
            return valid_file_paths, invalid_file_paths

        except Exception as e:
            raise FinanceException(e, sys)

    def start_batch_prediction(self):
        try:
            input_dir = self.pipeline_config.input_dir
            files= [input_dir]
            logger.info(f"Files: {files}")
            valid_files, invalid_files = self.get_valid_files(file_paths=files)
            invalid_files = valid_files
            if len(invalid_files) > 0:
                logger.info(f"{len(invalid_files)}: invalid file found:")
                failed_dir = self.pipeline_config.failed_dir
                for invalid_file in invalid_files:
                    logger.info(f"Moving invalid file {invalid_file} to failed dir: {failed_dir}")
                    #self.s3_storage.move(source_key=invalid_file, destination_dir_key=failed_dir)
                    

            if len(valid_files) == 0:
                logger.info(f"No valid file found.")
                return None

            estimator = S3FinanceEstimator(bucket_name=S3_MODEL_BUCKET_NAME, s3_key=S3_MODEL_DIR_KEY)
            for valid_file in valid_files:
                logger.info("Staring prediction of file: {valid_file}")
                dataframe: DataFrame = self.read_file(valid_file)
                #dataframe = dataframe.drop(self.schema.col_consumer_disputed)
                transformed_dataframe = estimator.transform(dataframe=dataframe)
                required_columns = self.schema.required_prediction_columns + [self.schema.prediction_label_column_name]
                logger.info(f"Saving required_columns: {required_columns}")
                transformed_dataframe=transformed_dataframe.select(required_columns)
                transformed_dataframe.show()
                prediction_file_path = os.path.join(self.pipeline_config.prediction_dir, os.path.basename(valid_file))
                logger.info(f"Writing prediction file at : [{self.pipeline_config.prediction_dir}] ")
                self.write_file(dataframe=transformed_dataframe, file_path=prediction_file_path)
                archive_file_path = os.path.join(self.pipeline_config.archive_dir, os.path.basename(valid_file))
                logger.info(f"Arching valid input files at: [{archive_file_path}]")
                self.write_file(dataframe=dataframe, file_path=archive_file_path)


        except Exception as e:
            raise FinanceException(e, sys)
