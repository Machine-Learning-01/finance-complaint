import os
import argparse
from finance_complaint.exception import FinanceException
from finance_complaint.pipeline import  PredictionPipeline
from finance_complaint.logger import logger
from finance_complaint.config import FinanceConfig
import sys


if __name__=="__main__":
    PredictionPipeline().start_batch_prediction()