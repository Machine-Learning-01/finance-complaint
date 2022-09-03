from collections import namedtuple

import enum
from enum import Enum



PipelineConfig = namedtuple("PipelineConfig", ["pipeline_name", "artifact_dir"])

DataIngestionConfig = namedtuple("DataIngestionConfig", ["from_date",
                                                         "to_date",
                                                         "file_format",
                                                         "data_ingestion_dir",
                                                         "download_dir",
                                                         "file_name",
                                                         "csv_data_dir"
                                                         ])
