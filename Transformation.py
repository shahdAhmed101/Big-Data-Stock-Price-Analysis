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
    #read Parquet files from the bronze layer
    df_bronze = spark.read.parquet(HDFS_BRONZE_PATH)
    print("Bronze stock data loaded.")

    #prepare business date column
    df_bronze = df_bronze.withColumn("business_date", F.to_date("date"))

    # generate dim_date if it doesn't exist
    try:
        spark.read.parquet(f"{GOLD_BASE_PATH}dim_date")
        print("dim_date already exists. Skipping generation.")
    except Exception:
        print("dim_date not found. Generating static dimension (2014-2030)...")
        static_date_table = generate_static_date_dim(spark)
        static_date_table.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_date")

    #extract distinct symbols, add placeholder attributes
    new_dim_symbol = df_bronze.select("symbol").distinct() \
        .withColumn("company_name", F.lit("Unknown")) \
        .withColumn("sector", F.lit("Unknown"))

    try:
        existing_dim_symbol = spark.read.parquet(f"{GOLD_BASE_PATH}dim_symbol")
        # Merge new symbols into existing dimension
        final_dim_symbol = existing_dim_symbol.unionByName(new_dim_symbol) \
            .dropDuplicates(["symbol"])
        print("Merging new symbols into existing dim_symbol...")
    except Exception:
        print("dim_symbol not found. Starting fresh.")
        final_dim_symbol = new_dim_symbol

    final_dim_symbol.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}dim_symbol")
    print("dim_symbol saved.")

    fact_daily_stock_prices = df_bronze \
        .withColumn("date_key", F.date_format("business_date", "yyyyMMdd").cast("int")) \
        .select(
            "symbol",
            "date_key",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "processing_ts"   # optional audit column from simulator
        )

    print("Writing fact_daily_stock_prices to Gold Layer...")
    fact_daily_stock_prices.write.mode("overwrite").parquet(f"{GOLD_BASE_PATH}fact_daily_stock_prices")

    print("Gold Layer (Star Schema) created successfully.")

except Exception as e:
    print(f"Transformation failed: {e}")
    raise e

finally:
    spark.stop()
    print("Spark Session Stopped.")
