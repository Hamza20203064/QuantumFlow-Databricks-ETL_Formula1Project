# Databricks notebook source
# MAGIC %run "../3.includes/a_configuration" 

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# display(races_df)

# COMMAND ----------

#1)
#  let's assume that we only want to get data related to one of the tax years.
# lets take year 2019
races_filtered_df1 = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

display(races_filtered_df1)

# COMMAND ----------

# 2)
# Filter condition in a kind of Pythonic way.
# lets take year 2018
races_filtered_df2 = races_df.filter((races_df["race_year"] == 2018) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df2)

# COMMAND ----------

# 3)
races_filtered_df3 = races_df.filter((races_df.race_year == 2017) & (races_df.round <= 5))

# COMMAND ----------

display(races_filtered_df3)