import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('StockStarSchemaTransformation') \
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

HDFS_BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/stock_data/"
GOLD_BASE_PATH   = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

#file to remember last processed date_key
STATE_FILE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/state/last_date_key.txt"

def generate_static_date_dim(spark, start_date="2014-01-01", end_date="2030-12-31"):
    df = spark.sql(f"SELECT CAST('{start_date}' AS DATE) as start, CAST('{end_date}' AS DATE) as end")
    df = df.select(
        F.explode(
            F.sequence(F.to_date("start"), F.to_date("end"), F.expr("interval 1 day"))
        ).alias("date")
    )
    dim_date = df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").alias("day_of_week"),
        F.weekofyear("date").alias("week_of_year"),
        F.quarter("date").alias("quarter"),
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend")
    )
    return dim_date

try:
    df_bronze = spark.read.parquet(HDFS_BRONZE_PATH)
    print("Bronze stock data loaded.")

    df_bronze = df_bronze.withColumn("business_date", F.to_date("date"))  #prepare business data
    df_bronze = df_bronze.withColumn("date_key", F.date_format("business_date", "yyyyMMdd").cast("int")) #add the date_key integer column early so we can filter on it

    try:
        spark.read.parquet(f"{GOLD_BASE_PATH}dim_date")
        print("dim_date already exists. Skipping generation.")
    except Exception:
        print("dim_date not found. Generating static dimension...")
        static_date_table = generate_static_date_dim(spark)
        static_date_table.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_date")

    new_dim_symbol = df_bronze.select("symbol").distinct() \
        .withColumn("company_name", F.lit("Unknown")) \
        .withColumn("sector", F.lit("Unknown"))

    try:
        existing_dim_symbol = spark.read.parquet(f"{GOLD_BASE_PATH}dim_symbol")
        final_dim_symbol = existing_dim_symbol.unionByName(new_dim_symbol) \
            .dropDuplicates(["symbol"])
        print("Merging new symbols into existing dim_symbol...")
    except Exception:
        print("dim_symbol not found. Starting fresh.")
        final_dim_symbol = new_dim_symbol

    final_dim_symbol.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_symbol")
    print("dim_symbol saved.")

    #incremental logic with a state file
    #check if a state file exists 
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    state_path = spark._jvm.org.apache.hadoop.fs.Path(STATE_FILE_PATH)

    first_run = not fs.exists(state_path)
    last_date_key = 0

    if not first_run:
        # Read the last processed date_key from state file
        state_rdd = spark.sparkContext.textFile(STATE_FILE_PATH)
        last_date_key = int(state_rdd.collect()[0])
        print(f"Last processed date_key: {last_date_key}")
    else:
        print("State file not found. Performing full initial load.")

    #filter bronze layer data to only include new records (date_key > last_date_key)
    if first_run:
        df_new_fact = df_bronze   #take everything
    else:
        df_new_fact = df_bronze.filter(F.col("date_key") > last_date_key)
        new_count = df_new_fact.count()
        if new_count == 0:
            print("No new data to process. Transformation completed without changes.")
            spark.stop()
            exit(0)
        print(f"New records to process: {new_count}")

    #build the fact table DataFrame
    fact_daily_stock_prices = df_new_fact.select(
        "symbol", "date_key", "open", "high", "low", "close", "volume", "processing_ts"
    )

    #write fact table – append if incremental, overwrite if first load
    write_mode = "overwrite" if first_run else "append"
    print(f"Writing fact_daily_stock_prices with mode='{write_mode}'...")
    fact_daily_stock_prices.write.mode(write_mode).parquet(f"{GOLD_BASE_PATH}fact_daily_stock_prices")

    #update the state file with the maximum date_key from this batch
    max_date_key = df_new_fact.agg(F.max("date_key")).collect()[0][0]
    if max_date_key is not None:
        #write the new max date_key to a temp file and move it (overwrite)
        spark.sparkContext.parallelize([str(max_date_key)]).coalesce(1).saveAsTextFile(
            STATE_FILE_PATH + "_tmp"
        )
        # Move tmp file to final state path (overwrites)
        fs.delete(state_path, True)   # delete old state file
        tmp_path = spark._jvm.org.apache.hadoop.fs.Path(STATE_FILE_PATH + "_tmp")
        fs.rename(tmp_path, state_path)
        print(f"State updated. New last_date_key: {max_date_key}")

    print("Gold Layer (Star Schema) created/updated successfully.")

except Exception as e:
    print(f"Transformation failed: {e}")
    raise e

finally:
    spark.stop()
    print("Spark Session Stopped.")
