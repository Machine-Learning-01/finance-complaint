
from finance_complaint.config.spark_manager import spark_session
file_path = "s3a://finance-cat-service/sensor_predictions.csv"

if __name__=="__main__":
    df = spark_session.read.csv(file_path)