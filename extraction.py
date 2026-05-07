import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('StockBatchIngestion') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

print("Spark Connected Successfully")

input_path = "file:///home/jovyan/work/data/stock_batches/"

# HDFS bronze layer for stock data
hdfs_output_path = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/stock_data/"

print(f"Reading batch data from: {input_path}")

try:
    # Read all CSV files in the landing zone
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)

    record_count = raw_df.count()

    if record_count > 0:
        raw_df.printSchema()

        print(f"Processing {record_count} records...")
        print(f"Writing to HDFS (Bronze Layer): {hdfs_output_path}")

        raw_df.write \
            .mode("append") \
            .format("parquet") \
            .save(hdfs_output_path)

        print("Batch ingestion complete. Data saved as Parquet.")
    else:
        print("No new data found in the landing zone.")

except Exception as e:
    print(f"Error during Spark processing: {e}")

finally:
    spark.stop()
    print("Spark Session Stopped.")
