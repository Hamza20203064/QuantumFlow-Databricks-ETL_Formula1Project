# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlhamza-account-key')
# Assign this value to a variable, so that we can use that variable in this statement here, instead of the hard-coded value. So I'm going to assign that to a variable called formula1dl account key.


# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlhamza.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlhamza.dfs.core.windows.net")) 

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlhamza.dfs.core.windows.net/circuits.csv"))