from typing import List
from pyspark.sql.types import TimestampType, StringType, FloatType, StructType, StructField
from finance_complaint.exception import FinanceException
import os, sys

from pyspark.sql import DataFrame
from typing import Dict


class FinanceDataSchema:

    def __init__(self):
        self.col_company_response: str = 'company_response'
        self.col_consumer_consent_provided: str = 'consumer_consent_provided'
        self.col_submitted_via = 'submitted_via'
        self.col_timely: str = 'timely'
        self.col_diff_in_days: str = 'diff_in_days'
        self.col_company: str = 'company'
        self.col_issue: str = 'issue'
        self.col_product: str = 'product'
        self.col_state: str = 'state'
        self.col_zip_code: str = 'zip_code'
        self.col_consumer_disputed: str = 'consumer_disputed'
        self.col_date_sent_to_company: str = "date_sent_to_company"
        self.col_date_received: str = "date_received"
        self.col_complaint_id: str = "complaint_id"
        self.col_sub_product: str = "sub_product"
        self.col_complaint_what_happened: str = "complaint_what_happened"
        self.col_company_public_response: str = "company_public_response"

    @property
    def dataframe_schema(self) -> StructType:
        try:
            schema = StructType([
                StructField(self.col_company_response, StringType()),
                StructField(self.col_consumer_consent_provided, StringType()),
                StructField(self.col_submitted_via, StringType()),
                StructField(self.col_timely, StringType()),
                StructField(self.col_date_sent_to_company, TimestampType()),
                StructField(self.col_date_received, TimestampType()),
                StructField(self.col_company, StringType()),
                StructField(self.col_issue, StringType()),
                StructField(self.col_product, StringType()),
                StructField(self.col_state, StringType()),
                StructField(self.col_zip_code, StringType()),
                StructField(self.col_consumer_disputed, StringType()),

            ])
            return schema

        except Exception as e:
            raise FinanceException(e, sys) from e

    @property
    def target_column(self) -> str:
        return self.col_consumer_disputed

    @property
    def one_hot_encoding_features(self) -> List[str]:
        features = [
            self.col_company_response,
            self.col_consumer_consent_provided,
            self.col_submitted_via,
        ]
        return features

    @property
    def im_one_hot_encoding_features(self) -> List[str]:
        return [f"im_{col}" for col in self.one_hot_encoding_features]

    @property
    def string_indexer_one_hot_features(self) -> List[str]:
        return [f"si_{col}" for col in self.one_hot_encoding_features]

    @property
    def tf_one_hot_encoding_features(self) -> List[str]:
        return [f"tf_{col}" for col in self.one_hot_encoding_features]

    @property
    def tfidf_features(self) -> List[str]:
        features = [
            self.col_issue
        ]
        return features

    @property
    def derived_input_features(self) -> List[str]:
        features = [
            self.col_date_sent_to_company,
            self.col_date_received
        ]
        return features

    @property
    def derived_output_features(self) -> List[str]:
        return [self.col_diff_in_days]

    @property
    def numerical_columns(self) -> List[str]:
        return self.derived_output_features

    @property
    def im_numerical_columns(self) -> List[str]:
        return [f"im_{col}" for col in self.numerical_columns]

    @property
    def tfidf_feature(self) -> List[str]:
        return [self.col_issue]

    @property
    def tf_tfidf_features(self) -> List[str]:
        return [f"tf_{col}" for col in self.tfidf_feature]

    @property
    def input_features(self) -> List[str]:
        in_features = self.tf_one_hot_encoding_features + self.im_numerical_columns + self.tf_tfidf_features
        return in_features

    @property
    def required_columns(self) -> List[str]:
        features = [self.target_column] + self.one_hot_encoding_features + self.tfidf_features + \
                   [self.col_date_sent_to_company, self.col_date_received]
        return features

    @property
    def required_prediction_columns(self) -> List[str]:
        features =  self.one_hot_encoding_features + self.tfidf_features + \
                   [self.col_date_sent_to_company, self.col_date_received]
        return features



    @property
    def unwanted_columns(self) -> List[str]:
        features = [
            self.col_complaint_id,
            self.col_sub_product, self.col_complaint_what_happened]

        return features

    @property
    def vector_assembler_output(self) -> str:
        return "va_input_features"

    @property
    def scaled_vector_input_features(self) -> str:
        return "scaled_input_features"

    @property
    def target_indexed_label(self) -> str:
        return f"indexed_{self.target_column}"

    @property
    def prediction_column_name(self) -> str:
        return "prediction"

    @property
    def prediction_label_column_name(self) -> str:
        return f"{self.prediction_column_name}_{self.target_column}"
