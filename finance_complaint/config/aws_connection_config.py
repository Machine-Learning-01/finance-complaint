from finance_complaint.constant.env_var_key import AWS_SECRET_ACCESS_KEY_ENV_KEY, AWS_ACCESS_KEY_ID_ENV_KEY
import os
import boto3


class AWSConnectionConfig:

    def __init__(self, region_name):
        __access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY, )
        __secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY, )
        self.s3_resource = boto3.resource('s3',
                                          aws_access_key_id=__access_key_id,
                                          aws_secret_access_key=__secret_access_key,
                                          region_name=region_name
                                          )
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=__access_key_id,
                                      aws_secret_access_key=__secret_access_key,
                                      region_name=region_name
                                      )
