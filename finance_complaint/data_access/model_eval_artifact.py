from finance_complaint.config.mongo_client import MongodbClient
from finance_complaint.entity.artifact_entity import ModelEvaluationArtifact


class ModelEvaluationArtifactData:

    def __init__(self):
        self.client = MongodbClient()
        self.collection_name = "model_eval_artifact"
        self.collection = self.client.database[self.collection_name]

    def save_eval_artifact(self, model_eval_artifact: ModelEvaluationArtifact):
        self.collection.insert_one(model_eval_artifact.to_dict())

    def get_eval_artifact(self, query):
        self.collection.find_one(query)

    def update_eval_artifact(self, query, model_eval_artifact: ModelEvaluationArtifact):
        self.collection.update_one(query, model_eval_artifact.to_dict())

    def remove_eval_artifact(self, query):
        self.collection.delete_one(query)

    def remove_eval_artifacts(self, query):
        self.collection.delete_many(query)

    def get_eval_artifacts(self, query):
        self.collection.find(query)
