from collections import namedtuple
from datetime import datetime

DataIngestionArtifact = namedtuple("DataIngestionArtifact",
                                   ["feature_store_file_path", "metadata_file_path", "download_dir"])
DataValidationArtifact = namedtuple("DataValidationArtifact", ["accepted_file_path", "rejected_dir"])
DataTransformationArtifact = namedtuple("DataTransformationArtifact",
                                        ["transformed_train_file_path", "exported_pipeline_file_path",
                                         "transformed_test_file_path",

                                         ])

PartialModelTrainerRefArtifact = namedtuple("PartialModelTrainerRefArtifact", ["trained_model_file_path",
                                                                               "label_indexer_model_file_path"
                                                                               ])

PartialModelTrainerMetricArtifact = namedtuple("PartialModelTrainerMetricArtifact", [
    "f1_score", "precision_score", "recall_score"
])


class ModelTrainerArtifact:

    def __init__(self, model_trainer_ref_artifact: PartialModelTrainerRefArtifact,
                 model_trainer_train_metric_artifact: PartialModelTrainerMetricArtifact,
                 model_trainer_test_metric_artifact: PartialModelTrainerMetricArtifact
                 ):
        self.model_trainer_ref_artifact = model_trainer_ref_artifact
        self.model_trainer_train_metric_artifact = model_trainer_train_metric_artifact
        self.model_trainer_test_metric_artifact = model_trainer_test_metric_artifact

    @staticmethod
    def construct_object(**kwargs):
        model_trainer_ref_artifact=PartialModelTrainerRefArtifact(**(kwargs['model_trainer_ref_artifact']))
        model_trainer_train_metric_artifact=PartialModelTrainerMetricArtifact(**(kwargs['model_trainer_train_metric_artifact']))
        model_trainer_test_metric_artifact=PartialModelTrainerMetricArtifact(**(kwargs['model_trainer_test_metric_artifact']))
        model_trainer_artifact = ModelTrainerArtifact(model_trainer_ref_artifact,model_trainer_train_metric_artifact,model_trainer_test_metric_artifact)
        return model_trainer_artifact


    def _asdict(self):
        try:
            response = dict()
            response['model_trainer_ref_artifact'] = self.model_trainer_ref_artifact._asdict()
            response['model_trainer_train_metric_artifact'] = self.model_trainer_train_metric_artifact._asdict()
            response['model_trainer_test_metric_artifact'] = self.model_trainer_test_metric_artifact._asdict()
            return response
        except Exception as e:
            raise e


ModelPusherArtifact = namedtuple("ModelEvaluationArtifact", ["model_pushed_dir"])


class ModelEvaluationArtifact:

    def __init__(self, model_accepted, changed_accuracy, trained_model_path, best_model_path, active,*args,**kwargs):
        self.model_accepted = model_accepted
        self.changed_accuracy = changed_accuracy
        self.trained_model_path = trained_model_path
        self.best_model_path = best_model_path
        self.active = active
        self.created_timestamp = datetime.now()

    def to_dict(self):
        return  self.__dict__
        

    def __str__(self):
        return str(self.to_dict())