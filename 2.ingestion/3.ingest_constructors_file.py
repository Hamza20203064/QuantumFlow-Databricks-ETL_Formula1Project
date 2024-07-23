# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
# schema = "colonum_name DataType"
# These data types come from Hive. But as I said, actually I couldn't find much documentation. But when you use the hive data types, it works fine because it uses hive metastore for everything.

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1dlhamza/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()
# this print the dataType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

# # 1)
# constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# # 2)
# constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

# # 3)
# constructor_dropped_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# 4)
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlhamza/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlhamza/processed/constructors/
# MAGIC