import os

from finance_complaint.component.data_ingestion import main
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        logger.exception(e)
