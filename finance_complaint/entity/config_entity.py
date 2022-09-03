from collections import namedtuple

PipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                         "to_date",
                                                         "file_format"
                                                         "data_ingestion_dir",
                                                         "downloaded_data"
                                                         ])
