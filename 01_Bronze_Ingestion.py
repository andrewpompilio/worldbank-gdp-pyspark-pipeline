# Databricks notebook source
from pyspark.sql import functions as F #import Spark built in SQL function library and assign standard nomenclature F

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze") # run native SQL commands in Python, also creates db folder named bronze in Databricks folder for table. If not exists states whether or not a schema is already there or not

# COMMAND ----------

import json
import urllib.request
from pyspark.sql.functions import col, from_json, schema_of_json

# 1. Define the public REST API endpoint for World Bank GDP data (2015-2023)
url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&date=2015:2023&per_page=1000"

# 2. Execute an HTTP request to download the raw web page
web_page = urllib.request.urlopen(url)

# 3. Decode response to text, parse into Python list/dicts, and grab index [1] (the payload records)
data_list = json.loads(web_page.read().decode("utf-8"))[1]

# 4. Convert every Python dictionary into a plain text JSON string to ensure uniform string typing
text_rows = [json.dumps(row) for row in data_list]

# 5. Convert the Python string list into a 1-column PySpark DataFrame (column defaults to 'value' of string type)
df_text = spark.createDataFrame(text_rows, "string")

# 6. Extract the first JSON string sample to automatically build a column/type rulebook (schema blueprint)
rulebook = schema_of_json(text_rows[0])

# 7. Apply the rulebook to convert our raw text column into a single structured column named 'data'
df_nested = df_text.select(from_json(col("value"), rulebook).alias("data"))

# 8. Unpack ('data.*') all fields from inside the nested 'data' column into top-level DataFrame columns
df_raw = df_nested.select("data.*")

# COMMAND ----------

display(df_raw.limit(5)) #display head of dataframe

# COMMAND ----------

df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze.worldbank_gdp_raw") #Save the raw DataFrame to disk as a Delta table in the 'bronze' schema

display(spark.table("bronze.worldbank_gdp_raw").limit(5)) #Query the newly created Delta table directly from storage to verify