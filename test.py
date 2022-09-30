
from finance_complaint.constant.environment.variable_key import AWS_ACCESS_KEY_ID_ENV_KEY,AWS_SECRET_ACCESS_KEY_ENV_KEY

import os
from pyspark.sql import SparkSession
access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY, )
secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY, )

spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.executor.memoryOverhead", "10g") \
    .config("com.amazonaws.services.s3.enableV4",True)\
    .getOrCreate()

spark_session._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", access_key_id)
spark_session._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", secret_access_key)

spark_session._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark_session._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
spark_session._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark_session._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark_session._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "ap-south-1.amazonaws.com")
spark_session._jsc.hadoopConfiguration().set(" fs.s3.buffer.dir","tmp")
# sc = spark.sparkContext

# hadoop_conf = spark_session._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.access.key", access_key_id)
# hadoop_conf.set("fs.s3a.secret.key", secret_access_key)
# hadoop_conf.set("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com")
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# # spark_session.sparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4',True)

fp="/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/projects/finance_complaint/finance_artifact/data_ingestion/feature_store/finance_complaint"

fp="s3a://finance-complaint/finance_data/prediction/20220929_155055/finance_complaint"
# df=spark_session.read.parquet(fp)
# df.show()
# df=df.limit(100)
# df.write.parquet("s3a://finance-complaint/finance_data/input/finance_complaint")
df=spark_session.read.parquet(fp)
print(df.count())
df.show()

