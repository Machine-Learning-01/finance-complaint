import os
import argparse
from finance_complaint.exception import FinanceException
from finance_complaint.pipeline import  TrainingPipeline
from finance_complaint.logger import logger
from finance_complaint.config import FinanceConfig
import sys


if __name__=="__main__":
    TrainingPipeline().start()