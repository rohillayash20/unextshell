# Databricks notebook source
#dbutils.fs.mount(
  #source = "wasbs://raw@yashrohstorage.blob.core.windows.net",
  #mount_point = "/mnt/yashrohstorage/raw",
  #extra_configs = {"fs.azure.account.key.yashrohstorage.blob.core.windows.net":"3x1aqRKIqkAfvTX1FxvR2xZuEka91wYD+wXEpiXMk2I/aj+rJNmOg2rNmFjRY0BGtvvKMhpxQ27t+AStDOTYjw=="})

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/yashrohstorage/raw/json")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/yashrohstorage/raw/output/yash/json").saveAsTable("json.bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.bronze

# COMMAND ----------


