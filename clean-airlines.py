# Databricks notebook source
#Setting up some basic widgets
dbutils.widgets.text("source_container", "")
dbutils.widgets.text("account_name", "capstone01")
source_container = dbutils.widgets.get("source_container")
account_name = dbutils.widgets.get("account_name")
dbutils.widgets.text("target_container", "")
target_container = dbutils.widgets.get("target_container")

# COMMAND ----------

# MAGIC 
# MAGIC %run ./mount_datalake $container_name=$source_container $account_name= $account_name

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

flight_details_schema = StructType(List(
    StructField(Year,StringType,true),
    StructField(Month,StringType,true),
    StructField(DayofMonth,StringType,true),
    StructField(DayOfWeek,StringType,true),
    StructField(DepTime,StringType,true),
    StructField(CRSDepTime,StringType,true),
    StructField(ArrTime,StringType,true),
    StructField(CRSArrTime,StringType,true),
    StructField(UniqueCarrier,StringType,true),
    StructField(FlightNum,StringType,true),
    
    StructField(TailNum,StringType,true),
    StructField(ActualElapsedTime,StringType,true),StructField(CRSElapsedTime,StringType,true),StructField(AirTime,StringType,true),StructField(ArrDelay,StringType,true),StructField(DepDelay,StringType,true),StructField(Origin,StringType,true),StructField(Dest,StringType,true),StructField(Distance,StringType,true),StructField(TaxiIn,StringType,true),StructField(TaxiOut,StringType,true),StructField(Cancelled,StringType,true),StructField(CancellationCode,StringType,true),StructField(Diverted,StringType,true),StructField(CarrierDelay,StringType,true),StructField(WeatherDelay,StringType,true),StructField(NASDelay,StringType,true),StructField(SecurityDelay,StringType,true),StructField(LateAircraftDelay,StringType,true)))

# COMMAND ----------

df = spark.read.format('parquet').option("header","true").option("inferSchema", "true").load("dbfs:/mnt/files/landing-zone/1988.parquet")

# COMMAND ----------

df.schema

# COMMAND ----------

from delta.tables import DeltaTable
flight_delta = DeltaTable.forPath(spark, "dbfs:/mnt/files/bronze-table/flight_details/")

# COMMAND ----------

# MAGIC %run ./mount_datalake $container_name=$target_container $account_name= $account_name

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/mnt/files/landing-zone'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_ingest LOCATION 'dbfs:/mnt/files/bronze_table/';
# MAGIC USE bronze_ingest;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_ingest.flight_details
# MAGIC USING PARQUET
# MAGIC LOCATION 'dbfs:/mnt/files/landing-zone/1988.parquet';

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE bronze_ingest.flight_details;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_ingest.flight_details;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze_ingest.flight_details
# MAGIC FROM 'dbfs:/mnt/files/raw/' 
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS('header' = 'true', 'sep' = '\t');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze_ingest.flight_details;

# COMMAND ----------

flight_details = spark.read.parquet("dbfs:/mnt/files/1988.parquet")
airports_data = spark.read.parquet("dbfs:/mnt/files/airports.parquet")
carriers_data = spark.read.parquet("dbfs:/mnt/files/carriers.parquet")
plane_details = spark.read.parquet("dbfs:/mnt/files/plane-data.parquet")

# COMMAND ----------

# Getting Insights of the flight details
flight_details.count()

# COMMAND ----------

flight_details.cache()

# COMMAND ----------

flight_details.select("FlightNum").distinct().count()

# COMMAND ----------

flight_details.filter("Cancelled == 1").count()

# COMMAND ----------

flight_details.filter("FlightNum == 361").count()

# COMMAND ----------

display(flight_details.filter("Cancelled == 1").groupby("FlightNum").count().sort("count", ascending=False))

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count
df_columns = ["ArrDelay", "DepDelay"]
df2 = newDf.select([count(when(col(c).contains('NA') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df_columns])

# COMMAND ----------

df2.show()

# COMMAND ----------

flight_df = flight_details.withColumn('arrDelay', F.regexp_replace('arrDelay', 'NA', '0')).withColumn('depDelay', F.regexp_replace('DepDelay', 'NA', '0'))

# COMMAND ----------

from pyspark.sql.types import IntegerType
flight_df = flight_df.withColumn("arrDelay",flight_df.arrDelay.cast(IntegerType())
                                ).withColumn("depDelay",flight_df.depDelay.cast(IntegerType()))

# COMMAND ----------

import pyspark.sql.functions as F
flight_df.groupby(
    "FlightNum").agg(
    F.mean('ArrDelay').alias("AvgArrDelay"), 
    F.mean('DepDelay').alias("AvgDepDelay"),
    F.max('ArrDelay').alias("MaxArrDelay"),
    F.min('ArrDelay').alias("MinArrDelay"),
    F.max('DepDelay').alias("MaxDepDelay"),
    F.min('DepDelay').alias("MinDepDelay")).show()

# COMMAND ----------

airports_data.count()

# COMMAND ----------

carriers_data.count()

# COMMAND ----------

plane_details.count()

# COMMAND ----------


