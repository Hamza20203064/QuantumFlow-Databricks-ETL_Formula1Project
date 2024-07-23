# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

# spark.conf.set("fs.azure.account.key.formula1dlhamza.dfs.core.windows.net", "WBLYA0kpH0QQNUFeY25MYyjjk2HlCdhTTchTUU1OYr2mBT3hDJrK2I7ZpOEFfi9ogKxmeyifGQFg+AStcR8ypQ==")

# Instide of using Spark Confeguration I directly added to cluster(formula1dlhamza) in Advance option > spark confe. > paste  both string.

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlhamza.dfs.core.windows.net")) 

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlhamza.dfs.core.windows.net/circuits.csv"))