from finance_complaint.logger import logger
import os
import sys
from finance_complaint.exception import FinanceException
from finance_complaint.entity.config_entity import ModelTrainerConfig
from finance_complaint.entity.artifact_entity import ModelTrainerArtifact, DataTransformationArtifact
from pyspark.sql import DataFrame
from finance_complaint.entity.spark_manager import spark_session
import torch
from typing import List, Tuple
from finance_complaint.entity.complaint_column import ComplaintColumn
from petastorm.spark import SparkDatasetConverter, make_spark_converter
from torch import nn
from torch.optim import Adam
from torch.autograd import Variable
from pyspark.sql.functions import col, rand
from pyspark.sql.types import FloatType


class FinanceComplaintEstimator(nn.Module):

    def __init__(self, input_shape):
        super(FinanceComplaintEstimator, self).__init__()
        self.neural_network_layers = nn.Sequential(
            nn.Linear(input_shape, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128),
            nn.Dropout(0.2),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.BatchNorm1d(64),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.BatchNorm1d(32),
            nn.Dropout(0.2),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.BatchNorm1d(16),
            nn.Dropout(0.2),
            nn.Linear(16, 8),
            nn.ReLU(),
            nn.BatchNorm1d(8),
            nn.Linear(8, 4),
            nn.ReLU(),
            nn.BatchNorm1d(4),
            nn.Linear(4, 2),
            nn.ReLU(),
            nn.BatchNorm1d(2),
            nn.Linear(2, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        predicted_value = self.neural_network_layers(x)
        return predicted_value


class ModelTrainer(ComplaintColumn):

    def __init__(self, model_trainer_config: ModelTrainerConfig,
                 data_transformation_artifact: DataTransformationArtifact
                 ):

        try:
            super(ModelTrainer, self).__init__()
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact

        except Exception as e:
            raise FinanceException(e, sys)

    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(
                self.data_transformation_artifact.transformed_data_file_path)
            dataframe = dataframe.select([self.scaled_vector_input_features, self.col_consumer_disputed])
            total_row = dataframe.count()
            df_with_yes_label = dataframe.filter(col(self.col_consumer_disputed) == "Yes")
            number_of_yes = df_with_yes_label.count()

            df_with_no_label = dataframe.filter(col(self.col_consumer_disputed) == "No")

            number_of_no = df_with_no_label.count()

            ratio = number_of_yes / number_of_no

            selected_df_with_no_label, _ = df_with_no_label.randomSplit([ratio, 1 - ratio])

            dataframe = df_with_yes_label.union(selected_df_with_no_label)
            dataframe = dataframe.select("*").orderBy(rand())
            dataframe = dataframe.replace(self.target_value_mapping)

            dataframe = dataframe.withColumn(self.col_consumer_disputed,
                                             col(self.col_consumer_disputed).cast(FloatType()))

            dataframe.groupby(self.col_consumer_disputed).count().show()
            dataframe = dataframe.orderBy(rand())
            return dataframe
        except Exception as e:
            raise FinanceException(e, sys)

    def split_dataframe(self, dataframe: DataFrame) -> List[DataFrame]:
        try:
            ratio = self.model_trainer_config.split_ratio
            train_dataframe, test_dataframe = dataframe.randomSplit([1 - ratio, ratio])
            return [train_dataframe, test_dataframe]
        except Exception as e:
            raise FinanceException(e, sys)

    def configure_spark_dataset_convert(self, ) -> None:
        try:
            cache_dir = self.model_trainer_config.cache_dir
            spark_session.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF,
                                   cache_dir
                                   )
        except Exception as e:
            raise FinanceException(e, sys)

    def save_model(self, model: FinanceComplaintEstimator):
        try:

            model_path = self.model_trainer_config.export_model_path
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            torch.save(model.state_dict(), model_path)
        except Exception as e:
            raise FinanceException(e, sys)

    def compute_accuracy(self, model, dataset_loader, device) -> float:
        try:
            model.eval()
            accuracy = 0.0
            total = 0.0

            with torch.no_grad():
                for data in dataset_loader:
                    input_data = data[self.scaled_vector_input_features]
                    target_data = data[self.col_consumer_disputed]
                    input_data = input_data.to(device)
                    target_data = target_data.to(device)

                    output = model(input_data)
                    _, predicted = torch.max(output.data, 1)
                    total += target_data.shape[0]

                    accuracy += (predicted == target_data).sum().item()
            accuracy = (100 * accuracy / total)
            return accuracy
        except Exception as e:
            raise FinanceException(e, sys)

    def initiate_model_training(self, ) -> ModelTrainerArtifact:
        try:
            is_model_initialized = False
            dataframe = self.read_data()
            train_df, test_df = self.split_dataframe(dataframe=dataframe)
            train_df, eval_df = self.split_dataframe(dataframe=train_df)

            train_df.groupby(self.col_consumer_disputed).count().show()
            eval_df.groupby(self.col_consumer_disputed).count().show()
            test_df.groupby(self.col_consumer_disputed).count().show()
            self.configure_spark_dataset_convert()

            device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

            # device = "cpu"
            print("The model will be running on", device, "device")
            # Convert model parameters and buffers to CPU or Cuda

            batch_size = self.model_trainer_config.batch_size
            epochs = self.model_trainer_config.epoch
            train_converter = make_spark_converter(train_df)
            eval_converter = make_spark_converter(eval_df)
            test_converter = make_spark_converter(test_df)

            for epoch in range(epochs):
                running_loss = 0.0
                running_acc = 0.0

                with train_converter.make_torch_dataloader(batch_size=batch_size, ) as trainer_data_loader, \
                        eval_converter.make_torch_dataloader(batch_size=batch_size, ) as eval_data_loader:
                    length_of_trainer_data = len(train_converter)
                    length_of_eval_data = len(eval_converter)
                    steps_per_epoch = len(train_converter) // batch_size
                    print(f"Number of train_data: {length_of_trainer_data} and eval_data: {length_of_eval_data}")
                    print(f"Steps in each epoch: [{steps_per_epoch}]")
                    train_iter, eval_iter = iter(trainer_data_loader), iter(eval_data_loader)

                    for i, train_data in enumerate(train_iter):
                        input_data = train_data[self.scaled_vector_input_features]
                        target_data = train_data[self.col_consumer_disputed]
                        target_data = torch.reshape(target_data, (-1, 1))

                        # target_data = target_data.type(torch.LongTensor)
                        input_data = input_data.to(device)
                        target_data: torch.Tensor = target_data.to(device)
                        print(target_data.unique(return_counts=True))
                        input_shape = input_data.shape[1]
                        if not is_model_initialized:
                            best_accuracy: float = 0.0
                            finance_estimator = FinanceComplaintEstimator(input_shape=input_shape)
                            loss_fn = nn.BCELoss()
                            optimizer = Adam(finance_estimator.parameters(), lr=0.001, weight_decay=0.0001)
                            finance_estimator.to(device)
                            is_model_initialized = True

                        optimizer.zero_grad()
                        output = finance_estimator(input_data)
                        # print(f"output: {output.shape} target: {target_data.shape}")
                        loss = loss_fn(output, target_data)
                        loss.backward()

                        optimizer.step()
                        running_loss += loss.item()
                        if is_model_initialized:
                            accuracy = self.compute_accuracy(finance_estimator, eval_iter, device=device)
                            info = f" the eval score: >>>>>>>>>>>>>>>>>>>>[{accuracy} running loss:[{running_loss}]]"
                            logger.info(info)
                            print(info)
                        if accuracy > best_accuracy:
                            self.save_model(model=finance_estimator)

            with test_converter.make_torch_dataloader(batch_size=batch_size) as test_data_loader:
                length_of_test_data = len(test_converter)
                print(f"Number of test data: {length_of_test_data}")
                accuracy = self.compute_accuracy(model=finance_estimator,
                                                 dataset_loader=iter(test_data_loader),
                                                 device=device)

                info = f"Test score: [{accuracy}]"
                logger.info(info)
                print(info)

            model_trainer_artifact = ModelTrainerArtifact(
                exported_model_path=self.model_trainer_config.export_model_path)
            return model_trainer_artifact
        except Exception as e:
            raise FinanceException(e, sys)
