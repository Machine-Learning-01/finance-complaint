import sys

from finance_complaint.exception import FinanceException
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame

import shutil
import os
from finance_complaint.constant.model import MODEL_SAVED_DIR
import time
from typing import List, Optional
import re
from abc import abstractmethod, ABC
from finance_complaint.config.aws_connection_config import AWSConnectionConfig

class CloudEstimator(ABC):
    key = "model-registry"
    model_dir = "saved_models"
    compression_format = "zip"
    model_file_name = "model.zip"

    @abstractmethod
    def get_all_model_path(self, key) -> List[str]:
        """

        Args:
            key: cloud storage key where model is location
        Returns: return List of all model cloud storage key path

        """
        pass

    @abstractmethod
    def get_latest_model_path(self, key) -> str:
        """
        This function returns latest model path from cloud storage
        Args:
            key:

        Returns: complete key path of cloud storage to download model

        """
        pass

    @abstractmethod
    def decompress_model(self, zip_model_file_path, extract_dir):
        """
        This function extract downloaded zip model from cloud storage
        Args:
            zip_model_file_path: zipped model file location
            extract_dir: model will be extracted at extracted dir

        Returns:

        """
        pass

    @abstractmethod
    def compress_model_dir(self, model_dir):
        """
        This function help us to zip model so that model can
        be uploaded to cloud storage such as S3 bucket, Azure blob storage/
        Google cloud storage.

        Args:
            model_dir: Model dir is directory to compress

        Returns:

        """
        pass

    @abstractmethod
    def is_model_available(self, key) -> bool:
        """
        This function return false if no model available otherwise true
        Args:
            key: cloud storage key where model is location

        Returns:

        """
        pass

    @abstractmethod
    def save(self, model_dir, key):
        """
        This function save provide model dir to cloud
        It internally compress the model dir then upload it to cloud
        Args:
            model_dir: Model dir to compress
            key: cloud storage key where model will be saved

        Returns:

        """
        pass

    @abstractmethod
    def load(self, key, extract_dir) -> str:
        """
        This function download the latest  model if complete cloud storage key for model not provided else
        model will be downloaded using provided model key path in extract dir
        Args:
            key:
            extract_dir:

        Returns: Model Directory

        """
        pass

    @abstractmethod
    def transform(self, df) -> DataFrame:
        """
        This function is designed to be overridden by child class
        Args:
            df: Input dataframe

        Returns:

        """
        pass


class S3Estimator(CloudEstimator):

    def __init__(self, bucket_name, region_name="ap-south-1", **kwargs):
        """

        Args:
            bucket_name: s3 bucket name
            region_name: s3 bucket region
            **kwargs: other keyword argument
        """
        if len(kwargs) > 0:
            super().__init__(kwargs)

        aws_connect_config = AWSConnectionConfig(region_name=region_name)
        self.s3_client = aws_connect_config.s3_client
        self.resource =aws_connect_config.s3_resource

        response = self.s3_client.list_buckets()
        # Output the bucket names
        print('Existing buckets:')
        available_buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name not in available_buckets:
            location = {'LocationConstraint': region_name}
            self.s3_client.create_bucket(Bucket=bucket_name,
                                         CreateBucketConfiguration=location)
        self.bucket = self.resource.Bucket(bucket_name)
        self.bucket_name = bucket_name

        self.__model_dir = self.model_dir
        self.__timestamp = str(time.time())[:10]
        self.__model_file_name = self.model_file_name
        self.__compression_format = self.compression_format

    def __get_save_model_path(self, key: str = None) -> str:
        """
        This function prepare new cloud storage key to save the zipped mode
        Args:
            key:

        Returns: prepared complete cloud storage key to load model
        """
        if key is None:
            key = self.key
        if not key.endswith("/"):
            key = f"{key}/"
        return f"{key}{self.__model_dir}/{self.__timestamp}/{self.__model_file_name}"

    def get_all_model_path(self, key: str = None) -> List[str]:
        """
        This function return list of all available model key
        Args:
            key: cloud storage key where model is location
        Returns: return List of all model cloud storage key path

        """
        if key is None:
            key = self.key
        if not key.endswith("/"):
            key = f"{key}/"
        key = f"{key}{self.__model_dir}/"

        paths = []
        for key_summary in self.bucket.objects.filter(Prefix=key):
            if key_summary.key.endswith(self.__model_file_name):
                paths.append(key_summary.key)
        return paths

    def get_latest_model_path(self, key: str = None) -> Optional[str]:
        """

        Args:
            key: cloud storage key of saved model

        Returns: Return None if no model available else return the latest model key

        """
        if key is None:
            key = self.key
        if not key.endswith("/"):
            key = f"{key}/"
        key = f"{key}{self.__model_dir}/"
        timestamps = []
        for key_summary in self.bucket.objects.filter(Prefix=key):
            tmp_key = key_summary.key
            timestamp = re.findall(r'\d+', tmp_key)
            timestamps.extend(list(map(int, timestamp)))
            # timestamps.append(int(s3_key.replace(s3_key, key).replace(f"/{self.__model_file_name}")))
        if len(timestamps) == 0:
            return None
        timestamp = max(timestamps)

        model_path = f"{key}{timestamp}/{self.__model_file_name}"
        return model_path

    def decompress_model(self, zip_model_file_path, extract_dir) -> None:
        os.makedirs(extract_dir, exist_ok=True)
        shutil.unpack_archive(filename=zip_model_file_path,
                              extract_dir=extract_dir,
                              format=self.__compression_format)

    def compress_model_dir(self, model_dir) -> str:
        if not os.path.exists(model_dir):
            raise Exception(f"Provided model dir:{model_dir} not exist.")

        # preparing temp model zip file path
        tmp_model_file_name = os.path.join(os.getcwd(),
                                           f"tmp_{self.__timestamp}",
                                           self.__model_file_name.replace(f".{self.__compression_format}", ""))

        # remove tmp model zip file path is already present
        if os.path.exists(tmp_model_file_name):
            os.remove(tmp_model_file_name)

        # creating zip file of model dir at tmp model zip file path
        shutil.make_archive(base_name=tmp_model_file_name,
                            format=self.__compression_format,
                            root_dir=model_dir
                            )
        tmp_model_file_name = f"{tmp_model_file_name}.{self.__compression_format}"
        return tmp_model_file_name

    def save(self, model_dir, key):
        """

        This function save provide model dir to cloud
        It internally compress the model dir then upload it to cloud
        Args:
            model_dir: Model dir to compress
            key: cloud storage key where model will be saved

        Returns:


        """
        model_zip_file_path = self.compress_model_dir(model_dir=model_dir)
        save_model_path = self.__get_save_model_path(key=key)
        self.s3_client.upload_file(model_zip_file_path, self.bucket_name, save_model_path)
        shutil.rmtree(os.path.dirname(model_zip_file_path))

    def is_model_available(self, key) -> bool:
        """

        Args:
            key: Cloud storage to key to check if model available or not
        Returns:

        """
        return bool(len(self.get_all_model_path(key)))

    def load(self, key, extract_dir, ) -> str:
        """
        This function download the latest  model if complete cloud storage key for model not provided else
        model will be downloaded using provided model key path in extract dir
        Args:
            key:
            extract_dir:

        Returns: Model Directory

        """
        # obtaining latest model directory
        if not key.endswith(self.__model_file_name):
            model_path = self.get_latest_model_path(key=key, )
            if not model_path:
                raise Exception(f"Model is not available. please check your bucket {self.bucket_name}")
        else:
            model_path = key
        timestamp = re.findall(r'\d+', model_path)[0]
        extract_dir = os.path.join(extract_dir, timestamp)
        os.makedirs(extract_dir, exist_ok=True)
        # preparing file location to download model from s3 bucket
        download_file_path = os.path.join(extract_dir, self.__model_file_name)

        # download model from s3 into download file location
        self.s3_client.download_file(self.bucket_name, model_path, download_file_path)

        # unzipping file
        self.decompress_model(zip_model_file_path=download_file_path,
                              extract_dir=extract_dir
                              )

        # removing zip file after  unzip
        os.remove(download_file_path)

        model_dir = os.path.join(extract_dir, os.listdir(extract_dir)[0])
        return model_dir

    @abstractmethod
    def transform(self, df) -> DataFrame:
        pass


class FinanceComplaintEstimator:

    def __init__(self, **kwargs):
        try:
            if len(kwargs) > 0:
                super().__init__(**kwargs)

            self.model_dir = MODEL_SAVED_DIR
            self.loaded_model_path = None
            self.__loaded_model = None
        except Exception as e:
            raise FinanceException(e, sys)

    def get_model(self) -> PipelineModel:
        try:
            latest_model_path = self.get_latest_model_path()
            if latest_model_path != self.loaded_model_path:
                self.__loaded_model = PipelineModel.load(latest_model_path)
                self.loaded_model_path = latest_model_path
            return self.__loaded_model
        except Exception as e:
            raise FinanceException(e, sys)

    def get_latest_model_path(self, ):
        try:
            dir_list = os.listdir(self.model_dir)
            latest_model_folder = dir_list[-1]
            tmp_dir = os.path.join(self.model_dir, latest_model_folder)
            model_path = os.path.join(self.model_dir, latest_model_folder, os.listdir(tmp_dir)[-1])
            return model_path
        except Exception as e:
            raise FinanceException(e, sys)

    def transform(self, dataframe) -> DataFrame:
        try:
            model = self.get_model()
            return model.transform(dataframe)
        except Exception as e:
            raise FinanceException(e, sys)


class S3FinanceEstimator(FinanceComplaintEstimator, S3Estimator):

    def __init__(self, bucket_name, s3_key, region_name="ap-south-1"):
        super().__init__(bucket_name=bucket_name, region_name=region_name)
        self.s3_key = s3_key
        self.__loaded_latest_s3_model_path = None

    @property
    def new_latest_s3_model_path(self):
        return S3Estimator.get_latest_model_path(self, key=self.s3_key)

    def get_latest_model_path(self) -> str:
        s3_latest_model_path = self.new_latest_s3_model_path
        if self.__loaded_latest_s3_model_path != s3_latest_model_path:
            self.load(key=s3_latest_model_path, extract_dir=self.model_dir)
        return FinanceComplaintEstimator.get_latest_model_path(self)
