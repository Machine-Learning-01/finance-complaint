import os

from finance_complaint.component.data_ingestion import main,export_exiting_downloaded_data_to_parquet
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
if __name__ == "__main__":
    try:
        main()

        # download_dir=r"/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/data_ingestion/20220904_105712/downloaded_dir"
        # export_exiting_downloaded_data_to_parquet(download_dir=download_dir,
        #                                           file_name="finance",
        #                                           export_dir=os.getcwd()
        #                                           )

    except Exception as e:
        logger.exception(e)
