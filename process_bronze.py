# Databricks notebook source
# Setting up some basic widgets
dbutils.widgets.text("source_container", "landing-zone", "")
dbutils.widgets.text("account_name", "capstone01", "")
dbutils.widgets.text("base_location", "dbfs:/mnt/files", "")
dbutils.widgets.text("target_container", "silver-table", "")
dbutils.widgets.text("year", "1988", "")

# COMMAND ----------

source_container = dbutils.widgets.get("source_container")
account_name = dbutils.widgets.get("account_name")
target_container = dbutils.widgets.get("target_container")
base_loc = dbutils.widgets.get("base_location")
year = dbutils.widgets.get("year")

# COMMAND ----------

dbutils.notebook.run('./mount_datalake', 60, {"container_name": source_container, "account_name": account_name})
#dbutils.notebook.run( $container_name=$source_container $account_name= $account_name

# COMMAND ----------

dbutils.notebook.run('./mount_datalake', 60, {"container_name": target_container, "account_name": account_name})
#dbutils.notebook.run( $container_name=$source_container $account_name= $account_name

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/mnt/files/landing-zone/'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/mnt/files/bronze-table/'

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

flight_source_loc = f"{base_loc}/{source_container}/flight_details/"
airport_source_loc = f"{base_loc}/{source_container}/airports_data/airports.parquet"
carrier_source_loc = f"{base_loc}/{source_container}/carriers/carriers.parquet"
plane_source_loc = f"{base_loc}/{source_container}/plane_data/plane-data.parquet"
flight_target_loc = f"{base_loc}/{target_container}/fact_flight/"
carrier_target_loc = f"{base_loc}/{target_container}/dim_carriers/"
plane_target_loc = f"{base_loc}/{target_container}/dim_plane/"
airports_target_loc = f"{base_loc}/{target_container}/dim_airports/"

# COMMAND ----------

source_format = 'PARQUET'

# COMMAND ----------

flight_details = spark.read.option("inferschema", 'true').parquet(flight_source_loc)
airports_data = spark.read.option("inferschema", 'true').parquet(airport_source_loc)
carriers_data = spark.read.option("inferschema", 'true').parquet(carrier_source_loc)
plane_details = spark.read.option("inferschema", 'true').parquet(plane_source_loc)

# COMMAND ----------

def write_to_delta(table_name, source_df):
    try:
        source_df.write.format("delta").save(table_name)
    except:
        source_df.write.mode("overwrite").format("delta").save(table_name)
        

# COMMAND ----------

#Writing flight data to delta table
write_to_delta(flight_target_loc, flight_details)

# COMMAND ----------

# Writing plane data to delta table
write_to_delta(plane_target_loc, plane_source_loc, source_format, plane_details)
    

# COMMAND ----------

# Writing carriers data to delta table
write_to_delta(carrier_target_loc, carrier_source_loc, source_format, carriers_data)    

# COMMAND ----------

# Writing airports data to delta table
write_to_delta(airports_target_loc, airport_source_loc, source_format, airports_data)

# COMMAND ----------

flight_df = spark.read.format("delta").load("dbfs:/mnt/files/bronze-table/fact_flight/")
