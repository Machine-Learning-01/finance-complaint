from finance_complaint.constant.environment.variable_key import AWS_ACCESS_KEY_ID_ENV_KEY,AWS_SECRET_ACCESS_KEY_ENV_KEY

import os
from dotenv import load_dotenv


if __name__=="__main__":
    load_dotenv()
    access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY, )
    secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY, )
    print(access_key_id,secret_access_key)
    from finance_complaint.config.spark_manager import spark_session
    file_path = '/config/workspace/finance_artifact/data_ingestion/feature_store/finance_complaint'
    df = spark_session.read.parquet(file_path)
    df.write.parquet('s3a://finance-cat-service/finance_data/input/finance_complaint',mode="overwrite")