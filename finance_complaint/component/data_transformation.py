import os

from finance_complaint.entity.complaint_column import ComplaintColumn
from finance_complaint.exception import FinanceException
import sys
from pyspark.ml.feature import StandardScaler, VectorAssembler, OneHotEncoder, StringIndexer, Imputer
from pyspark.ml.pipeline import Pipeline, PipelineModel

from finance_complaint.entity.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from finance_complaint.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact
from finance_complaint.entity.config_entity import DataTransformationConfig
from pyspark.sql import DataFrame
from finance_complaint.ml.feature import FrequencyEncoder, FrequencyImputer, DerivedFeatureGenerator


class DataTransformation(ComplaintColumn):

    def __init__(self, data_validation_artifact: DataValidationArtifact,
                 data_transformation_config: DataTransformationConfig
                 ):
        try:
            super().__init__()
            self.data_val_artifact = data_validation_artifact
            self.data_tf_config = data_transformation_config
        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self) -> DataFrame:
        try:
            file_path = self.data_val_artifact.accepted_file_path
            dataframe: DataFrame = spark_session.read.parquet(file_path)
            dataframe.printSchema()
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def get_data_transformation_pipeline(self) -> Pipeline:
        try:

            stages = [

            ]

            # numerical column transformation

            # generating additional columns
            derived_feature = DerivedFeatureGenerator(inputCols=self.derived_input_features,
                                                      outputCols=self.derived_output_features)
            stages.append(derived_feature)

            # creating imputer to fill null values
            imputer = Imputer(inputCols=self.numerical_columns,
                              outputCols=self.im_numerical_columns)

            stages.append(imputer)

            frequency_imputer = FrequencyImputer(inputCols=self.categorical_columns,
                                                 outputCols=self.im_categorical_columns)
            stages.append(frequency_imputer)

            for im_one_hot_feature, string_indexer_col in zip(self.im_one_hot_encoding_features,
                                                              self.string_indexer_one_hot_features):
                string_indexer = StringIndexer(inputCol=im_one_hot_feature, outputCol=string_indexer_col)

                stages.append(string_indexer)

            one_hot_encoder = OneHotEncoder(inputCols=self.string_indexer_one_hot_features,
                                            outputCols=self.tf_one_hot_encoding_features)

            stages.append(one_hot_encoder)

            frequency_encoder = FrequencyEncoder(inputCols=self.im_frequency_encoding_features,
                                                 outputCols=self.tf_frequency_encoding_features)

            stages.append(frequency_encoder)

            vector_assembler = VectorAssembler(inputCols=self.input_features,
                                               outputCol=self.vector_assembler_output)

            stages.append(vector_assembler)

            standard_scaler = StandardScaler(inputCol=self.vector_assembler_output,
                                             outputCol=self.scaled_vector_input_features)

            stages.append(standard_scaler)

            pipeline = Pipeline(
                stages=stages
            )
            logger.info(f"Data transformation pipeline: [{pipeline}]")
            print(pipeline.stages)
            return pipeline

        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            dataframe: DataFrame = self.read_data()

            pipeline = self.get_data_transformation_pipeline()

            trained_pipeline = pipeline.fit(dataframe)
            transformed_dataframe = trained_pipeline.transform(dataframe)

            export_pipeline_file_path = self.data_tf_config.export_pipeline_dir

            # creating required directory
            os.makedirs(export_pipeline_file_path, exist_ok=True)
            os.makedirs(self.data_tf_config.transformed_data_dir, exist_ok=True)
            transformed_data_file_path = os.path.join(self.data_tf_config.transformed_data_dir,
                                                      self.data_tf_config.file_name
                                                      )

            logger.info(f"Saving transformation pipeline at: [{export_pipeline_file_path}]")
            trained_pipeline.save(export_pipeline_file_path)
            logger.info(f"Saving transformed data at: [{transformed_data_file_path}]")
            transformed_dataframe.write.parquet(transformed_data_file_path)

            data_tf_artifact = DataTransformationArtifact(
                exported_pipeline_file_path=export_pipeline_file_path,
                transformed_data_file_path=transformed_data_file_path

            )
            logger.info(f"Data transformation artifact: [{data_tf_artifact}]")
            return data_tf_artifact
        except Exception as e:
            raise FinanceException(e, sys)
