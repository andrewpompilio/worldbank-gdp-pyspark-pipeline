# Databricks notebook source
#import libraries
import pyspark.sql.functions as F
from delta.tables import DeltaTable

#1. Read clean data from the Silver layer
df_silver = spark.table("silver.worldbank_gdp_clean")

#2. Create the Gold database schema
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# Pre-execution null and data quality checks
assert df_silver.filter(F.col("country_id").isNull()).count() == 0, "NULL country_id found"
assert df_silver.filter(F.col("year").isNull()).count() == 0, "NULL year found"
assert df_silver.filter(F.col("gdp_used") < 0).count() == 0, "Negative GDP found"

# COMMAND ----------

# Build dim_date (standard time dimension)
# Extract unique years to serve as primary keys for time
dim_date = (
    df_silver.select("year").distinct()
    # Calculate the decade integer for higher level reporting (e.g., 2024 to 2020)
    .withColumn("decade", (F.col("year") / 10).cast("int") * 10)
    # Create a boolean flag to filter modern data (2020+) in BI application
    .withColumn("is_recent", F.when(F.col("year") >= 2020, True).otherwise(False))
)

# Save the date dimension as a Delta table in the gold layer
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

# COMMAND ----------

# Build dim_country (country dimension)
# Extract unique country attributes to build the dimension table
dim_country = (
    df_silver.select(
        "country_id",
        "country_name",
        "countryiso3code"
    )
    .distinct()
    #Generate an integer column for surrogate key for joining
    .withColumn("country_key", F.monotonically_increasing_id())
)

#Save the country dimension as a Delta table in the gold layer
dim_country.write.format("delta").mode("overwrite").saveAsTable("gold.dim_country")

# COMMAND ----------

#Upgrade existing gold.dim_country with SCD control columns
#Read the previous country dimension and attach SCD tracking metadata

df_scd_dim = (
    spark.table("gold.dim_country")
    #put today's date as the activation date
    .withColumn("effective_date", F.current_date())
    #set end_date to NULL because all current records are active
    .withColumn("end_date", F.lit(None).cast("date"))
    #mark all current records as active
    .withColumn("is_current",F.lit(True))
)

#overwrite schema on disk to save new columns while preserving existing surrogate keys
df_scd_dim.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_country")

#Apply SCD 1 and 2 Delta Merge logic
#extract unique country attributes from Silver to use as incoming staging data
stg_country = df_silver.select("country_id", "country_name", "countryiso3code").distinct()

#bind delta lake API to the gold.dim_country table
delta_dim = DeltaTable.forName(spark, "gold.dim_country")

# 1. SCD TYPE 1: Update minor administrative fixes in-place (e.g., ISO code corrections)
delta_dim.alias("target").merge(stg_country.alias("source"),"target.country_id=source.country_id AND target.is_current=true").whenMatchedUpdate(
    # Condition: ISO code changed but country name remained identical
    condition="target.countryiso3code <> source.countryiso3code AND target.country_name",
    # Overwrite ISO code in-place without creating a new row or storing history
    set={"countryiso3code": "source.countryiso3code"}
).execute()

# 2. SCD TYPE 2: Expire active records when major attributes (country_name) change
delta_dim.alias("target").merge(
    stg_country.alias("source"),
    "target.country_id=source.country_id AND target.is_current=true").whenMatchedUpdate(
    #Condition: country name changed (ie. official country rename)
    condition="target.country_name <> source.country_name",
    #Expire old record by setting end_date to today and marking is_current = False
    set={"end_date": F.current_date(), "is_current": F.lit(False)}
    ).execute()    
# 3. Append new active versions for expired records or brand-new countries
# Retrieve all country_ids that currently have an active row (is_current == True)
active_keys = spark.table("gold.dim_country").filter("is_current=true").select("country_id")

#filter staging data to isolate entities without an active record (using left_anti join)
new_records = (
    stg_country.join(active_keys, on="country_id", how="left_anti")
    # Assign a fresh surrogate key to the new active version
    .withColumn("country_key", F.monotonically_increasing_id())
    .withColumn("effective_date", F.current_date())
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("is_current", F.lit(True))
)

# Append new active dimension records to the Gold table
new_records.write.format("delta").mode("append").saveAsTable("gold.dim_country")

print("✅ GOLD.DIM_COUNTRY CONVERTED TO SCD 1 & 2 SUCCESSFULLY!")

# COMMAND ----------

#Build fact_gdp (central fact table)
#Join clean Silver data with dim_country to attach foreign keys
fact_gdp = (
    df_silver.join(
        dim_country,
        on=["country_id","country_name","countryiso3code"],
        how="inner"
    )
#Select foreign keys and numeric metrics for tableau aggregations
.select(
    F.col("country_key"), #Foreign key to dim_country
    F.col("year"), #Foreign key to dim_date
    F.col("indicator_id"),
    F.col("indicator_name"),
    F.col("gdp_used") #Core numeric metric
)
)

#Save the fact table as a Delta table in the gold layer
fact_gdp.write.format("delta").mode("overwrite").saveAsTable("gold.fact_gdp")

# COMMAND ----------

#check for orphan keys in fact table
orphan_facts = fact_gdp.filter(F.col("country_key").isNull()).count()
assert orphan_facts == 0, f"Found {orphan_facts} orphan facts missing country_key"

# Check for duplicates at the grain level
dup_facts = fact_gdp.groupBy("country_key", "year", "indicator_id").count().filter("count > 1").count()
assert dup_facts == 0, f"Found {dup_facts} duplicate rows in fact table"

# COMMAND ----------

# Preview all 3 Star Schema tables
print("--- GOLD.DIM_DATE ---")
display(spark.table("gold.dim_date").limit(5))

print("--- GOLD.DIM_COUNTRY ---")
display(spark.table("gold.dim_country").limit(5))

print("--- GOLD.FACT_GDP ---")
display(spark.table("gold.fact_gdp").limit(5))