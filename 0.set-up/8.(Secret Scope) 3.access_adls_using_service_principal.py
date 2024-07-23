# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC ##### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

# In this notebook we need to we're going to have the client id, tenant id and the client secret, all of them stored within the Key Vault. So that we've got none of these information hard-coded within our notebook.
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')


# COMMAND ----------

# client_id = "7417800b-bcb6-4642-a725-980cd26af096"
# tenant_id = "6a473d42-52b9-49dd-a211-5a987ff609f8"
# # Both are copied from Azure Databricks >microsoft Entra ID>  manage > App Registration 
# client_secret = "tPU8Q~cy1mwkSkqe0cEFVUeTqF6YfKLHiJoYIbC1"
# # Copied from Azure Databricks >microsoft Entra ID> formula1-app (a Service Principal) > manage > certificates and Secrets > copy the value not secreat ID

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlhamza.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlhamza.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlhamza.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlhamza.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlhamza.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlhamza.dfs.core.windows.net")) 

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlhamza.dfs.core.windows.net/circuits.csv"))