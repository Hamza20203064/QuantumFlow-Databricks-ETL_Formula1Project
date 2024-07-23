# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts()) # to list all mounts

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlhamza/raw

# COMMAND ----------

# circuits_df = (spark.read.csv('dbfs:/mnt/formula1dlhamza/raw/circuits.csv'))
circuits_df = spark.read.option("header", True).csv('dbfs:/mnt/formula1dlhamza/raw/circuits.csv')
# Read the CSV file as a DataFrame

# COMMAND ----------

# circuits_df = (spark.read.csv('dbfs:/mnt/formula1dlhamza/raw/circuits.csv'))
circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv('dbfs:/mnt/formula1dlhamza/raw/circuits.csv')
# Read the CSV file as a DataFrame
# DataFrame Reader API also give another parameter to pass into csv method called inferSchema --> defolt value is "False"

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema() # it spacefy all dataTata type
#  list all data types as strings (when inferSchema is false) which is not vright

# COMMAND ----------

# Another methodin our dataFrame which gives information about data within the our tdtaFrame, so you can identefy which type of data is present.
circuits_df.describe().show()

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

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns
# MAGIC Thare are four wayes to do that

# COMMAND ----------

# a)
circuits_selected_df0 = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")


# COMMAND ----------

display(circuits_selected_df0)
# url is not displead hare because qe have not select the url above.

# COMMAND ----------

# b)
circuits_selected_df1 = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng)


# COMMAND ----------

display(circuits_selected_df1)

# COMMAND ----------

# c)
circuits_selected_df2 = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"])


# COMMAND ----------

display(circuits_selected_df2)

# COMMAND ----------

# d)
from pyspark.sql.functions import col
# import the function call 'col'

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

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
# .withColums("column_name", value)
# current_timestamp() is a function so, we need to include it.

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dlhamza/processed/circuits")
# hare is parquet is the file formate and on using "overwrite" modle, if exist then overwrite it.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlhamza/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlhamza/processed/circuits"))