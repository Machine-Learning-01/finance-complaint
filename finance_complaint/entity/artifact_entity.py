from collections import namedtuple


DataIngestionArtifact = namedtuple("DataIngestionArtifact", ["feature_store_file_path"])
DataPreprocessingArtifact = namedtuple("DataPreprocessingArtifact",["preprocessed_data_file_path"])