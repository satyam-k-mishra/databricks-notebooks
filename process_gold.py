# Databricks notebook source
# Setting up some basic widgets
dbutils.widgets.text("source_container", "silver-table")
dbutils.widgets.text("account_name", "capstone01")
dbutils.widgets.text("base_location", "dbfs:/mnt/files")
dbutils.widgets.text("target_container", "gold-table")

# COMMAND ----------

source_container = dbutils.widgets.get("source_container")
account_name = dbutils.widgets.get("account_name")
target_container = dbutils.widgets.get("target_container")
base_loc = dbutils.widgets.get("base_location")

# COMMAND ----------

# MAGIC %run ./mount_datalake $container_name=$source_container $account_name= $account_name

# COMMAND ----------

# MAGIC %run ./mount_datalake $container_name=$source_container $account_name= $account_name

# COMMAND ----------

source_data = f"{base_loc}/{source_container}"
target_table = f"{base_loc}/{target_container}"

# COMMAND ----------

flight_df = spark.read.format("delta").load(source_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating flight_wise daily aggregation for the following columns:
# MAGIC 1. Average Arrival delay
# MAGIC 2. Average Distance
# MAGIC 3. Average Departure delay
# MAGIC 4. Minimum and Maximum for arrival delay and departure delay

# COMMAND ----------

import pyspark.sql.functions as F
df = flight_df.groupby("FlightNum", "Date").agg(
  F.mean("ArrDelay").alias("AvgArrDelay"),
  F.mean("Distance").alias("AvgDistance"),
  F.mean("DepDelay").alias("AvgDepDelay"),
  F.max("ArrDelay").alias("MaxArrDelay"),
  F.min("ArrDelay").alias("MinArrDelay"),
  F.max("DepDelay").alias("MaxDepDelay"),
  F.min("DepDelay").alias("MinDepDelay"),
)

# COMMAND ----------

# Overwriting the gold table with each execution. It will refresh the table. 
target_table = "dbfs:/mnt/files/gold-table/"
df.write.mode("overwrite").format("delta").save(target_table)

# COMMAND ----------

df = spark.read.format("delta").load(target_table)

# COMMAND ----------

df.count()

# COMMAND ----------

#Registering delta table with hive database
spark.sql(f"CREATE TABLE IF NOT EXISTS flight_db.gold_flight_details USING delta LOCATION '{target_table}'")

# COMMAND ----------

# MAGIC %md
# MAGIC Quering database in the gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_flight_details;
