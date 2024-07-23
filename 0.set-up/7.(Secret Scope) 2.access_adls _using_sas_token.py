# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-SAS-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlhamza.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlhamza.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlhamza.dfs.core.windows.net", formula1dl_demo_sas_token)



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlhamza.dfs.core.windows.net")) 

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlhamza.dfs.core.windows.net/circuits.csv"))