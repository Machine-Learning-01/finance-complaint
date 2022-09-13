import os

from finance_complaint.exception import FinanceException
from finance_complaint.pipeline import Pipeline
from finance_complaint.logger import logger
from finance_complaint.config import FinanceConfig
import sys
from finance_complaint.entity.artifact_entity import DataTransformationArtifact
if __name__ == "__main__":
    try:
        finance_config = FinanceConfig()
        pipeline = Pipeline(finance_config)

        pipeline.start()


        # from finance_complaint.config import FinanceConfig
        # time_stamp="20220913_090820"
        # config = FinanceConfig(timestamp=time_stamp)
        # model_trainer_config = config.get_model_trainer_config()
        # data_tft_artifact = DataTransformationArtifact(transformed_data_file_path=f"/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/data_transformation/{time_stamp}/transformed_data/finance_complaint",
        #                                        exported_pipeline_file_path=f"/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/data_transformation/{time_stamp}/pipeline")

        # model_trainer = ModelTrainer(data_transformation_artifact=data_tft_artifact,model_trainer_config = model_trainer_config)
        # model_trainer.initiate_model_training()
    except Exception as e:
        logger.info(FinanceException(e, sys))
