from typing import List
from pyspark.sql.types import TimestampType, StringType, FloatType, StructType, StructField
from finance_complaint.exception import FinanceException
import os, sys

from pyspark.sql import DataFrame


class ComplaintColumn:

    def __init__(self, ):
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

    @property
    def dataframe_schema(self) -> StructType:
        try:
            schema = StructType([
                StructField(self.col_company_response, StringType()),
                StructField(self.col_consumer_consent_provided, StringType()),
                StructField(self.col_submitted_via, StringType()),
                StructField(self.col_timely, StringType()),
                StructField(self.col_diff_in_days, FloatType()),
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
        features = [self.col_company_response,
                    self.col_consumer_consent_provided,
                    self.col_submitted_via,
                    self.col_timely
                    ]
        return features

    @property
    def numerical_features(self) -> List[str]:
        features = [self.col_diff_in_days]
        return features

    @property
    def frequency_encoding_features(self) -> List[str]:
        features = [self.col_company,
                    self.col_issue,
                    self.col_product,
                    self.col_state,
                    self.col_zip_code
                    ]
        return features

    @property
    def required_columns(self) -> List[str]:
        features = [self.target_column] + self.one_hot_encoding_features + self.frequency_encoding_features \
                   + self.numerical_features
        return features

    @property
    def unwanted_columns(self) -> List[str]:
        features = [
            self.col_date_sent_to_company, self.col_date_received, self.col_complaint_id,
            self.col_sub_product, self.col_complaint_what_happened]

        return features
