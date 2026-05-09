import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('SnowflakeLoader') \
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

# Snowflake connection details
sf_options = {
    "sfURL": "DJHONTE-NJ55843.snowflakecomputing.com",
    "sfUser": "SHAHDTAG",
    "sfPassword": "pAvZ8KTLMUkXnMn",
    "sfDatabase": "STOCK_DB",
    "sfSchema": "STAR_SCHEMA",
    "sfWarehouse": "STOCK_WH"
}

GOLD_BASE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/"

def table_exists_simple(table_name):
    full_table = f"{sf_options['sfDatabase']}.{sf_options['sfSchema']}.{table_name.upper()}"
    try:
        spark.read \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", full_table) \
            .load() \
            .limit(1) \
            .count()
        return True
    except Exception:
        return False

def load_dimension_atomic(table_name):
    snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils

    # Explicitly set the schema for the session (fixes the temp stage error)
    snowflake_utils.runQuery(sf_options, "USE SCHEMA STOCK_DB.STAR_SCHEMA")

    if table_name.lower() == "dim_date":
        if table_exists_simple("DIM_DATE"):
            print(f"--- SKIPPING {table_name}: Table already exists. ---")
            return
        else:
            print(f"{table_name} doesn't exist yet. Proceeding.")

    temp_table = f"{sf_options['sfDatabase']}.{sf_options['sfSchema']}.{table_name.upper()}_TEMP"
    final_table = f"{sf_options['sfDatabase']}.{sf_options['sfSchema']}.{table_name.upper()}"

    print(f"--- Atomic Load: {final_table} ---")
    df = spark.read.parquet(f"{GOLD_BASE_PATH}{table_name}")

    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", temp_table) \
        .mode("overwrite") \
        .save()

    swap_query = f"ALTER TABLE IF EXISTS {final_table} SWAP WITH {temp_table}"
    snowflake_utils.runQuery(sf_options, swap_query)

    drop_query = f"DROP TABLE IF EXISTS {temp_table}"
    snowflake_utils.runQuery(sf_options, drop_query)
    print(f"SUCCESS: {final_table} loaded atomically.")

def load_fact_append(table_name):
    snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils
    snowflake_utils.runQuery(sf_options, "USE SCHEMA STOCK_DB.STAR_SCHEMA")  # set schema

    target_table = f"{sf_options['sfDatabase']}.{sf_options['sfSchema']}.{table_name.upper()}"
    print(f"--- Append Load: {target_table} ---")
    df = spark.read.parquet(f"{GOLD_BASE_PATH}{table_name}")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", target_table) \
        .mode("append") \
        .save()
    print(f"SUCCESS: {target_table} appended.")

try:
    load_dimension_atomic("dim_date")
    load_dimension_atomic("dim_symbol")
    load_fact_append("fact_daily_stock_prices")
    print("ALL GOLD TABLES LOADED TO SNOWFLAKE!")
except Exception as e:
    print(f"Loading failed: {e}")
    raise e
finally:
    spark.stop()
