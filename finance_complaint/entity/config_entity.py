from collections import namedtuple
from finance_complaint.constant.prediction_pipeline_config.file_config import ARCHIVE_DIR, INPUT_DIR, FAILED_DIR, \
    PREDICTION_DIR, REGION_NAME

TrainingPipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                         "to_date",
                                                         "data_ingestion_dir",
                                                         "download_dir",
                                                         "file_name",
                                                         "feature_store_dir",
                                                         "failed_dir",
                                                         "metadata_file_path",
                                                         "datasource_url"
                                                         ])

DataValidationConfig = namedtuple('DataValidationConfig', ["accepted_data_dir", "rejected_data_dir", "file_name"])

DataTransformationConfig = namedtuple('DataTransformationConfig', ['file_name', 'export_pipeline_dir',
                                                                   'transformed_train_dir', "transformed_test_dir",

                                                                   "test_size"
                                                                   ])

ModelTrainerConfig = namedtuple("ModelTrainerConfig", ["base_accuracy", "trained_model_file_path", "metric_list",
                                                       'label_indexer_model_dir', ])

ModelEvaluationConfig = namedtuple("ModelEvaluationConfig",
                                   ["model_evaluation_report_file_path", "threshold", "metric_list", "model_dir",
                                    "bucket_name"])

ModelPusherConfig = namedtuple("ModelPusherConfig", ["model_dir", "bucket_name"])


class PredictionPipelineConfig:

    def __init__(self, input_dir=INPUT_DIR,
                 prediction_dir=PREDICTION_DIR,
                 failed_dir=FAILED_DIR,
                 archive_dir=ARCHIVE_DIR,
                 region_name=REGION_NAME
                 ):
        self.input_dir = input_dir
        self.prediction_dir = prediction_dir
        self.failed_dir = failed_dir
        self.archive_dir = archive_dir
        self.region_name = region_name

    def to_dict(self):
        return self.__dict__
