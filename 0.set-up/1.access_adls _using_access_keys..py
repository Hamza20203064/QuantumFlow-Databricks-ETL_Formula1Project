# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl.dfs.core.windows.net",
    "")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlhamza.dfs.core.windows.net", "WBLYA0kpH0QQNUFeY25MYyjjk2HlCdhTTchTUU1OYr2mBT3hDJrK2I7ZpOEFfi9ogKxmeyifGQFg+AStcR8ypQ==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlhamza.dfs.core.windows.net")) 

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlhamza.dfs.core.windows.net/circuits.csv"))