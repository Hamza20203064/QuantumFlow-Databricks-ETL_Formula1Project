# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# spark.conf.set(
#     "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
#     dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlhamza.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlhamza.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlhamza.dfs.core.windows.net", "sp=rl&st=2024-07-09T10:25:47Z&se=2024-07-09T18:25:47Z&spr=https&sv=2022-11-02&sr=c&sig=4Ef5pYNF4naIVkIGA30fuqzmF5UawLGS8k9d9EA8LFY%3D")



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlhamza.dfs.core.windows.net")) 

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlhamza.dfs.core.windows.net/circuits.csv"))