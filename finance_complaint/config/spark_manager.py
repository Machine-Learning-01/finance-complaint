from pyspark.sql import SparkSession
from finance_complaint.constant.env_var_key import AWS_ACCESS_KEY_ID_ENV_KEY,AWS_SECRET_ACCESS_KEY_ENV_KEY

import os
from pyspark.sql import SparkSession
access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY, )
secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY, )
#
# spark = SparkSession.builder.master('local[*]').appName('finance_complaint') .getOrCreate()
# hadoop_conf = spark._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# hadoop_conf.set("fs.s3n.awsAccessKeyId", access_key_id)
# hadoop_conf.set("fs.s3n.awsSecretAccessKey", secret_access_key)
#
# spark_session=spark
#


spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memoryOverhead", "8g") \
    .getOrCreate()



if __name__=="__main__":
    print(spark_session.sparkContext.sparkHome)