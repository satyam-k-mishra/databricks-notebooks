# Databricks notebook source
# Setting up some basic widgets
dbutils.widgets.text("source_container", "bronze-table", "")
dbutils.widgets.text("year", "1988", "")

# COMMAND ----------

year = dbutils.widgets.get("year")
source_container = dbutils.widgets.get("source_container")

# COMMAND ----------

print(year)
print(source_container)

# COMMAND ----------


