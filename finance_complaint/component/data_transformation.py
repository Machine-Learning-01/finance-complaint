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
from pyspark.sql.functions import col, rand

PREDICTION_COLUMN_NAME = "prediction"


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

    def get_data_transformation_pipeline(self, label_generator: IndexToString) -> Pipeline:
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
                                                 outputCols=self.im_one_hot_encoding_features)
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

            hashing_tf = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures", numFeatures=40)
            stages.append(hashing_tf)
            idf = IDF(inputCol=hashing_tf.getOutputCol(), outputCol=self.tf_tfidf_features[0])
            stages.append(idf)

            vector_assembler = VectorAssembler(inputCols=self.input_features,
                                               outputCol=self.vector_assembler_output)

            stages.append(vector_assembler)

            standard_scaler = StandardScaler(inputCol=self.vector_assembler_output,
                                             outputCol=self.scaled_vector_input_features)

            stages.append(standard_scaler)

            random_forest_clf = RandomForestClassifier(labelCol=self.target_indexed_label,
                                                       featuresCol=self.scaled_vector_input_features)
            stages.append(random_forest_clf)
            stages.append(label_generator)
            pipeline = Pipeline(
                stages=stages
            )

            logger.info(f"Data transformation pipeline: [{pipeline}]")
            print(pipeline.stages)
            return pipeline

        except Exception as e:
            raise FinanceException(e, sys)


    def get_balanced_shuffled_dataframe(self,dataframe: DataFrame) -> DataFrame:
        try:

            count_of_each_cat = dataframe.groupby(self.target_column).count().collect()
            label = []
            n_record = []
            for info in count_of_each_cat:
                n_record.append(info['count'])
                label.append(info[self.target_column])

            minority_row = min(n_record)
            n_per = [minority_row / record for record in n_record]

            selected_row = []
            for label, per in zip(label, n_per):
                print(label, per)
                temp_df, _ = dataframe.filter(col(self.target_column) == label).randomSplit([per, 1 - per])
                selected_row.append(temp_df)

            selected_df: DataFrame = None
            for df in selected_row:
                df.groupby(self.target_column).count().show()
                if selected_df is None:
                    selected_df = df
                else:
                    selected_df = selected_df.union(df)

            selected_df = selected_df.orderBy(rand())

            selected_df.groupby(self.target_column).count().show()
            return selected_df
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logger.info(f">>>>>>>>>>>Started data transformation <<<<<<<<<<<<<<<")
            dataframe: DataFrame = self.read_data()
            dataframe = self.get_balanced_shuffled_dataframe(dataframe=dataframe)
            logger.info(f"Number of row: [{dataframe.count()}] and column: [{len(dataframe.columns)}]")

            test_size = self.data_tf_config.test_size
            logger.info(f"Splitting dataset into train and test set using ration: {1 - test_size}:{test_size}")
            train_dataframe, test_dataframe = dataframe.randomSplit([1 - test_size, test_size])
            logger.info(f"Train dataset has number of row: [{train_dataframe.count()}] and"
                        f" column: [{len(train_dataframe.columns)}]")

            logger.info(f"Train dataset has number of row: [{train_dataframe.count()}] and"
                        f" column: [{len(train_dataframe.columns)}]")

            # Converting target column to index
            label_indexer = StringIndexer(inputCol=self.target_column,
                                          outputCol=self.target_indexed_label)
            label_indexer_model = label_indexer.fit(train_dataframe)

            # Get category from index
            label_generator = IndexToString(inputCol=PREDICTION_COLUMN_NAME,
                                            outputCol=f"{PREDICTION_COLUMN_NAME}_{self.target_column}",
                                            labels=label_indexer_model.labels)

            pipeline = self.get_data_transformation_pipeline(label_generator=label_generator)

            # Converting target column to integer value
            train_dataframe = label_indexer_model.transform(train_dataframe)
            test_dataframe = label_indexer_model.transform(test_dataframe)

            trained_pipeline = pipeline.fit(train_dataframe)
            transformed_trained_dataframe = trained_pipeline.transform(train_dataframe)

            transformed_trained_dataframe.select(
                [label_generator.getOutputCol(), self.target_column, ]).show(5)

            evaluator = MulticlassClassificationEvaluator(
                labelCol=self.target_indexed_label,
                predictionCol=PREDICTION_COLUMN_NAME,
                metricName="accuracy")

            train_accuracy = evaluator.evaluate(transformed_trained_dataframe)
            train_error = 1.0 - train_accuracy
            info = f"Training Error = {train_error}"
            print(info)
            logger.info(info)
            info = f"Training Accuracy ={train_accuracy}"
            logger.info(info)
            print(info)

            transformed_test_dataframe = trained_pipeline.transform(test_dataframe)
            transformed_test_dataframe.select(
                [label_generator.getOutputCol(), self.target_column, ]).show(5)

            required_column = self.required_columns
            required_column.append(label_generator.getOutputCol())

            evaluator = MulticlassClassificationEvaluator(
                labelCol=self.target_indexed_label, predictionCol=PREDICTION_COLUMN_NAME, metricName="accuracy")
            test_accuracy = evaluator.evaluate(transformed_test_dataframe)
            test_error = 1 - test_accuracy
            info = f"Testing Error = {test_error}"
            print(info)
            logger.info(info)
            info = f"Testing Accuracy = {test_accuracy}"
            logger.info(info)
            print(info)
            transformed_trained_dataframe = transformed_trained_dataframe.select(required_column)
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
                train_err=train_error,
                test_err=test_error,
                train_acc=train_accuracy,
                test_acc=test_accuracy
            )

            logger.info(f"Data transformation artifact: [{data_tf_artifact}]")
            return data_tf_artifact
        except Exception as e:
            raise FinanceException(e, sys)
