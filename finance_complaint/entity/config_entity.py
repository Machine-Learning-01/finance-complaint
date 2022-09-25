from collections import namedtuple

PipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

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
                                   ["model_evaluation_report_file_path", "threshold", "metric_list","model_dir",
                                    "bucket_name"])

ModelPusherConfig = namedtuple("ModelPusherConfig", ["model_dir", "bucket_name"])
