# Databricks notebook source
# Setting up some basic widgets
dbutils.widgets.text("source_container", "bronze-table", "")
dbutils.widgets.text("account_name", "capstone01", "")
dbutils.widgets.text("base_location", "dbfs:/mnt/files", "")
dbutils.widgets.text("target_container", "silver-table", "")

# COMMAND ----------

source_container = dbutils.widgets.get("source_container")
account_name = dbutils.widgets.get("account_name")
target_container = dbutils.widgets.get("target_container")
base_loc = dbutils.widgets.get("base_location")

# COMMAND ----------

source_container

# COMMAND ----------

dbutils.notebook.run('./mount_datalake', 60, {"container_name": source_container, "account_name": account_name})
#dbutils.notebook.run( $container_name=$source_container $account_name= $account_name

# COMMAND ----------

dbutils.notebook.run('./mount_datalake', 60, {"container_name": target_container, "account_name": account_name})
#dbutils.notebook.run( $container_name=$source_container $account_name= $account_name

# COMMAND ----------

from pyspark.sql.types import *
flight_details_schema = StructType(
    [
    StructField("Date", DateType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DepTime", StringType(), True),
    StructField("CRSDepTime", StringType(), True),
    StructField("ArrTime", StringType(), True),
    StructField("CRSArrTime", StringType(), True),
    StructField("UniqueCarrier", StringType(), True),
    StructField("FlightNum", IntegerType(), True),
    StructField("ActualElapsedTime", StringType(), True),
    StructField("CRSElapsedTime", StringType(), True),
    StructField("AirTime", StringType(), True),
    StructField("ArrDelay", StringType(), True),
    StructField("DepDelay", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", IntegerType(), True),
    StructField("Cancelled", BooleanType(), True),
    StructField("Diverted", BooleanType(), True),
    ]
  )

# COMMAND ----------

table_cols = ['Date','DayOfWeek', 'DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'ActualElapsedTime', 'CRSElapsedTime', 'ArrDelay', 'DepDelay', 'Origin', 'Dest', 'Distance', 'Cancelled', 'Diverted']

# COMMAND ----------

source_table = f"{base_loc}/{source_container}/fact_flight/"
target_table = f"{base_loc}/{target_container}/fact_flight/"

# COMMAND ----------

flight_df = spark.read.format("delta").load(source_table)

# COMMAND ----------

from pyspark.sql.functions import *
cols = ["Year", "Month", "DayOfMonth"]
df = flight_df.withColumn("Date",concat_ws("-",*cols).cast("date")).drop("Year", "Month", "DayOfMonth")

# COMMAND ----------

df = df.select(*table_cols)
df = df.replace("NA", "0")

# COMMAND ----------

float_cols = ['FlightNum','ArrDelay', 'DepDelay', 'Distance']
df = (
   df
   .select(
     *(c for c in df.columns if c not in float_cols),
     *(col(c).cast("float").alias(c) for c in float_cols)
   )
)

# COMMAND ----------

# from pyspark.sql.functions import col, isnan, when, count
# df_columns = [col for col in df.columns]
# df2 = df.select(
#   [
#     count(
#       when(
#         col(c).contains("NA")
#         | col(c).contains("NULL")
#         | (col(c) == "")
#         | col(c).isNull(),
#         c,
#       )
#     ).alias(c)
#     for c in df_columns
#   ]
# )
# df2.show()

# COMMAND ----------

try:
    df.write.format("delta").save(target_table)
except:
    df.write.mode("overwrite").format("delta").save(target_table)

# COMMAND ----------

#Registering delta table with hive database
spark.sql(f"CREATE DATABASE IF NOT EXISTS flight_db")

# COMMAND ----------

#Registering delta table with hive database
spark.sql(f"CREATE TABLE IF NOT EXISTS flight_db.silver_flight_details USING delta LOCATION '{target_table}'")

# COMMAND ----------

target_table

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/mnt/files/silver-table/fact_flight")

# COMMAND ----------

# %sql
# SELECT *
# FROM flight_db.silver_flight_details;
