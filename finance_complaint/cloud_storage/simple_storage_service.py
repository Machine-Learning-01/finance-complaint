import os
import sys
from typing import List

from finance_complaint.config.aws_connection_config import AWSConnectionConfig
from finance_complaint.exception import FinanceException


class SimpleStorageService:

    def __init__(self, region_name, s3_bucket_name):
        aws_connection_config = AWSConnectionConfig(region_name=region_name)
        self.client = aws_connection_config.s3_client
        self.resource = aws_connection_config.s3_resource
        response = self.client.list_buckets()
        available_buckets = [bucket['Name'] for bucket in response['Buckets']]
        if s3_bucket_name not in available_buckets:
            location = {'LocationConstraint': region_name}
            self.client.create_bucket(Bucket=s3_bucket_name,
                                      CreateBucketConfiguration=location)
        self.bucket = self.resource.Bucket(s3_bucket_name)
        self.bucket_name = s3_bucket_name

    def list_files(self, key: str, extension: str = "csv") -> List[str]:
        try:
            if not key.endswith("/"):
                key = f"{key}/"
            paths = []
            for key_summary in self.bucket.objects.filter(Prefix=key):
                if key_summary.key.endswith(extension):
                    paths.append(key_summary.key)
            return paths
        except Exception as e:
            raise FinanceException(e, sys)

    def delete_file(self, key) -> bool:
        try:
            self.resource.Object(self.bucket_name, key).delete()
            return True
        except Exception as e:
            raise FinanceException(e, sys)

    def copy(self, source_key: str, destination_dir_key: str) -> bool:
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_key
            }

            self.client.copy(copy_source,
                             self.bucket_name,
                             os.path.join(destination_dir_key,
                                          os.path.dirname(source_key)))
            return True

        except Exception as e:
            raise FinanceException(e, sys)

    def move(self, source_key, destination_dir_key) -> bool:
        try:
            self.copy(source_key, destination_dir_key)
            return self.delete_file(key=source_key)
        except Exception as e:
            raise FinanceException(e, sys)

    def download_file(self, s3_key, local_file_path):
        try:
            self.client.download_file(self.bucket_name, s3_key, local_file_path)
        except Exception as e:
            raise FinanceException(e, sys) from e

    def upload_file(self, s3_key, local_file_path):
        try:
            self.client.upload_file(local_file_path, self.bucket_name, s3_key)
        except Exception as e:
            raise FinanceException(e, sys) from e
