import pymongo
import os

import certifi

ca = certifi.where()
from finance_complaint.constant import DATABASE_NAME


class MongodbClient:

    def __init__(self, database_name=DATABASE_NAME) -> None:
        self.client = pymongo.MongoClient(
            f"mongodb+srv://iNeuron:{os.getenv('MONGO_DB_PASSWORD', None)}@ineuron-ai-projects.7eh1w4s.mongodb.net"
            f"/?retryWrites=true&w=majority",
            tlsCAFile=ca)
        self.database = self.client[database_name]
        self.database_name = database_name
