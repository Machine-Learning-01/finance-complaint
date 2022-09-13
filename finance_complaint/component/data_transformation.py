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
from pyspark.ml.feature import IDF, Tokenizer, HashingTF, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


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

            frequency_imputer = FrequencyImputer(inputCols=self.one_hot_encoding_features,
                                                 outputCols=self.im_categorical_columns)
            stages.append(frequency_imputer)

            for im_one_hot_feature, string_indexer_col in zip(self.im_one_hot_encoding_features,
                                                              self.string_indexer_one_hot_features):
                string_indexer = StringIndexer(inputCol=im_one_hot_feature, outputCol=string_indexer_col)

                stages.append(string_indexer)

            one_hot_encoder = OneHotEncoder(inputCols=self.string_indexer_one_hot_features,
                                            outputCols=self.tf_one_hot_encoding_features)

            stages.append(one_hot_encoder)

            tokenizer = Tokenizer(inputCol=self.tfidf_features[0], outputCol="words")
            stages.append(tokenizer)

            hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=40)

            stages.append(hashingTF)
            idf = IDF(inputCol="rawFeatures", outputCol=self.tf_tfidf_features[0])
            stages.append(idf)

            vector_assembler = VectorAssembler(inputCols=self.input_features,
                                               outputCol=self.vector_assembler_output)

            stages.append(vector_assembler)

            standard_scaler = StandardScaler(inputCol=self.vector_assembler_output,
                                             outputCol=self.scaled_vector_input_features)

            stages.append(standard_scaler)

            rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol=self.scaled_vector_input_features)
            stages.append(rf)

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

            train_dataframe, test_dataframe = dataframe.randomSplit([0.7, 0.3])
            pipeline = self.get_data_transformation_pipeline()

            labelIndexer = StringIndexer(inputCol=self.target_column, outputCol="indexedLabel")

            labelIndexerModel = labelIndexer.fit(train_dataframe)
            train_dataframe = labelIndexerModel.transform(train_dataframe)
            test_dataframe = labelIndexerModel.transform(test_dataframe)

            trained_pipeline = pipeline.fit(train_dataframe)
            transformed_trained_dataframe = trained_pipeline.transform(train_dataframe)

            labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                           labels=labelIndexerModel.labels)

            transformed_trained_dataframe = labelConverter.transform(transformed_trained_dataframe)

            transformed_trained_dataframe.select("predictedLabel", self.target_column,
                                                 self.scaled_vector_input_features).show(
                5)

            # Select (prediction, true label) and compute test error
            evaluator = MulticlassClassificationEvaluator(
                labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
            accuracy = evaluator.evaluate(transformed_trained_dataframe)
            print("Training Error = %g" % (1.0 - accuracy))
            print("Training Accuracy = %g" % (accuracy))

            transformed_test_dataframe = trained_pipeline.transform(test_dataframe)
            transformed_test_dataframe = labelConverter.transform(transformed_test_dataframe)

            required_column = self.required_columns
            required_column.append("predictedLabel")

            # transformed_dataframe.write.parquet("predicted")
            transformed_test_dataframe.select("predictedLabel", self.target_column,
                                              ).show(5)

            evaluator = MulticlassClassificationEvaluator(
                labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
            accuracy = evaluator.evaluate(transformed_test_dataframe)
            print("Test Error = %g" % (1.0 - accuracy))
            print("Test Accuracy = %g" % (accuracy))

            transformed_test_dataframe: DataFrame = transformed_test_dataframe.select(required_column
                                                                                      )
            export_pipeline_file_path = self.data_tf_config.export_pipeline_dir

            # creating required directory
            os.makedirs(export_pipeline_file_path, exist_ok=True)
            os.makedirs(self.data_tf_config.transformed_test_dir, exist_ok=True)
            os.makedirs(self.data_tf_config.transformed_train_dir, exist_ok=True)
            transformed_train_data_file_path = os.path.join(self.data_tf_config.transformed_train_dir,
                                                            self.data_tf_config.file_name
                                                            )
            transformed_test_data_file_path = os.path.join(self.data_tf_config.transformed_test_dir,
                                                           self.data_tf_config.file_name
                                                           )

            logger.info(f"Saving transformation pipeline at: [{export_pipeline_file_path}]")
            trained_pipeline.save(export_pipeline_file_path)
            logger.info(f"Saving transformed train data at: [{transformed_train_data_file_path}]")
            transformed_test_dataframe.write.parquet(transformed_train_data_file_path)

            logger.info(f"Saving transformed test data at: [{transformed_test_data_file_path}]")
            transformed_test_dataframe.write.parquet(transformed_test_data_file_path)

            data_tf_artifact = DataTransformationArtifact(
                transformed_train_file_path=transformed_test_data_file_path,
                transformed_test_file_path=transformed_test_data_file_path,
                exported_pipeline_file_path=export_pipeline_file_path,
                train_err=None,
                test_err=None,
                train_acc=None,
                test_acc=None

            )

            logger.info(f"Data transformation artifact: [{data_tf_artifact}]")
            return data_tf_artifact
        except Exception as e:
            raise FinanceException(e, sys)
