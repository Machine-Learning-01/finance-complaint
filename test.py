import os

from finance_complaint.entity.estimator import S3FinanceEstimator
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.logger import logger
s3_key="model-registry"

from finance_complaint.constant import TIMESTAMP
from finance_complaint.entity.complaint_column import  ComplaintColumn
from pyspark.sql import DataFrame
if __name__ == "__main__":
    try:
        filepath = f"prediction_files/file_{TIMESTAMP}"
        os.makedirs(os.path.dirname(filepath),exist_ok=True)
        column = ComplaintColumn()
        fp="/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/data_validation/20220924_123715/accepted_data/finance_complaint"
        model = S3FinanceEstimator(bucket_name="finance-complaint",s3_key=s3_key)
        path = model.new_latest_s3_model_path
        df =spark_session.read.parquet(fp)
        df:DataFrame = model.transform(df)
        df:DataFrame =df[column.required_columns+[f"{column.prediction_column_name}_{column.target_column}"]]
        df.write.parquet(filepath)
        df.show(5)
    except Exception as e:
        logger.exception(e)
        

# # from statistics import mode
# # from pyspark.sql import SparkSession
# #
# #
# # spark = SparkSession.builder.master('local[*]').appName('finance_complaint').getOrCreate()
# # hadoop_conf = spark._jsc.hadoopConfiguration()
# # hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# # hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
# # hadoop_conf.set("fs.s3n.awsSecretAccessKey", secret_key)
# # from finance_complaint.constant import TIMESTAMP
# # from pyspark.ml.pipeline import PipelineModel
# #
# # m_p = "/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/model_trainer/20220916_165305/trained_model/finance_estimator"
# # url = f's3n://finance-complaint/model/{TIMESTAMP}'
# #
# # if __name__ == '__main__':
# #     model = PipelineModel.load(m_p)
# #     model.save()
#
# # from finance_complaint.entity.estimator import FinanceComplaintEstimator
# # from finance_complaint.entity.spark_manager import spark_session
# # fp="/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/finance_artifact/data_validation/20220916_204751/accepted_data/finance_complaint"
# #
# # if __name__=="__main__":
# #     df =spark_session.read.parquet(fp)
# #
# #     est = FinanceComplaintEstimator(model_dir="saved_models")
# #
# #     pdf = est.transform(dataframe=df)
# #     pdf.show()
# import os
# from finance_complaint.entity.model_registry import S3ModelRegistry
# if __name__=="__main__":
#     fp="/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/saved_models/20220919_171228"
#     bucket_name=  "finance-complaint"
#     model_registry =  S3ModelRegistry(bucket_name=bucket_name)
#     testing=os.path.join("testing")
#
#     model_registry.get_all_model_path(s3_key="model_registry")
#
#
#     # #model_registry.save(model_dir=fp,s3_key="model_registry")
#     # #path = model_registry.get_load_model_path(s3_key="model_registry")
#     # model_registry.load(s3_key = 'model_registry',extract_dir="demo")
#     # # zp = model_registry.compress_model_dir(model_dir=fp)
#     # # zp="/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/finance_complaint/tmp_16639344373102982/model.zip"
#     # model_registry.decompress_model(zip_model_file_path=zp,extract_dir=testing)
