import pymongo
import os
from finance_complaint.constant.environment.variable_key import MONGO_DB_URL_ENV_KEY
import certifi

ca = certifi.where()
from finance_complaint.constant.database import DATABASE_NAME


class MongodbClient:
    client = None

    def __init__(self, database_name=DATABASE_NAME) -> None:
        if MongodbClient.client is None:
            mongo_db_url = os.getenv(MONGO_DB_URL_ENV_KEY)
            if mongo_db_url is None:
                raise Exception(f"Environment key: {MONGO_DB_URL_ENV_KEY} is not set.")
            MongodbClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
        self.client = MongodbClient.client
        self.database = self.client[database_name]
        self.database_name = database_name
