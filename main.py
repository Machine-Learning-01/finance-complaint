import os

from finance_complaint.exception import FinanceException
from finance_complaint.pipeline import Pipeline
from finance_complaint.logger import logger
from finance_complaint.config import FinanceConfig
import sys
from finance_complaint.entity.artifact_entity import DataTransformationArtifact
if __name__ == "__main__":
    try:
        #print(os.getenv('avn'))
        # Creating an instance of the FinanceConfig class.
        finance_config = FinanceConfig()
        pipeline = Pipeline(finance_config)

        pipeline.start()

    except Exception as e:
        logger.info(FinanceException(e, sys))
