from collections import namedtuple





PipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                         "to_date",
                                                         "data_ingestion_dir",
                                                         "download_dir",
                                                         "file_name",
                                                         "feature_store_dir",
                                                         "failed_dir",
                                                         "metadata_file_path"
                                                         ])

DataPreprocessingConfig = namedtuple('DataPreprocessing',["preprocessed_data_file_path","preprocessing_dir"])
