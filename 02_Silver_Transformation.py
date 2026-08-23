# Databricks notebook source
# Load the raw table saved during the Bronze stage
df_bronze = spark.table("bronze.worldbank_gdp_raw")

# COMMAND ----------

import pyspark.sql.functions as F

#flatten nested structures and cast columns to proper data types
df_silver = df_bronze.select(

    #unpack country structure
    F.col("country.id").alias("country_id"),
    F.col("country.value").alias("country_name"),
    F.col("countryiso3code"),

    #unpack the indicator structure
    F.col("indicator.id").alias("indicator_id"),
    F.col("indicator.value").alias("indicator_name"),

    #Clean up standard fields (cast date string to integer year, value to double)
    F.col("date").cast("int").alias("year"),
    F.col("value").alias("gdp_used")
)

#save flattened dataset as a delta table in a new silver schema
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.worldbank_gdp_clean")

#preview the fully flattened Silver table
display(spark.table("silver.worldbank_gdp_clean").limit(5))