# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(),nullable = False),
                                     StructField("circuitRef", StringType(),nullable = True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])
# the StructType represents your row and your StructFields represent fields or individual columns and yuou have to spacify DataType of each of the cloumn as well.
# we got a schema here for our DataFrame.

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/formula1dlhamza/raw/circuits.csv")
# .option("inferSchema", True) \ is replase bt ===> .schema(circuits_schema) \

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dlhamza/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlhamza/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlhamza/processed/circuits"))

# COMMAND ----------

