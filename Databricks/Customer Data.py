# Databricks notebook source
# DBTITLE 1,Medallion Architecture Overview
# MAGIC %md
# MAGIC # Customer Data 
# MAGIC
# MAGIC This notebook implements a **Bronze → Silver → Golden** medallion architecture for customer data.
# MAGIC

# COMMAND ----------

# ============================================================
# Create dummy source CSV (if it doesn't already exist)
# ============================================================
import os

CATALOG = "workspace"
SCHEMA = "practice_db"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/staging"
SOURCE_FILE = f"{VOLUME_PATH}/customer_source.csv"

# Ensure the volume directory exists
os.makedirs(VOLUME_PATH, exist_ok=True)

dummy_data = """cust_id,full_name,email,city,signup_date,order_total
1,Customer A,customer.a@email.com,City A,2024-01-15,150.50
2,Customer B,customer.b@email.com,City B,2024-02-20,320.00
3,Customer C,customer.c@email.com,City C,2024-03-10,75.25
4,Customer D,customer.d@email.com,City D,2024-04-05,510.75
5,Customer E,customer.e@email.com,City E,2024-05-22,199.99
6,Customer F,customer.f@email.com,City F,2024-06-18,430.00
7,Customer G,customer.g@email.com,City G,2024-07-30,88.30
8,Customer H,customer.h@email.com,City H,2024-08-12,620.50
"""

# Write the dummy data to the CSV file
with open(SOURCE_FILE, "w") as f:
    f.write(dummy_data)

# Display whether the file has been created or not
if os.path.exists(SOURCE_FILE):
    print(f"✅ File created successfully: {SOURCE_FILE}")
    print(f"   Size: {os.path.getsize(SOURCE_FILE)} bytes")
else:
    print(f"❌ File not found: {SOURCE_FILE}")

# COMMAND ----------

# DBTITLE 1,Configuration
# ============================================================
# Configuration
# ============================================================
CATALOG = "workspace"
SCHEMA = "practice_db"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/staging"
SOURCE_FILE = f"{VOLUME_PATH}/customer_source.csv"

# Table names for each medallion layer
BRZ_TABLE = f"{CATALOG}.{SCHEMA}.brz_customers"
SLV_TABLE = f"{CATALOG}.{SCHEMA}.slv_customers"
GLD_TABLE = f"{CATALOG}.{SCHEMA}.gld_customers"

# COMMAND ----------

# DBTITLE 1,Check Source File Exists
# ============================================================
# Step 1: Validate source file exists before loading
# ============================================================
import os

if not os.path.exists(SOURCE_FILE):
    raise FileNotFoundError(
        f"SOURCE FILE MISSING: '{SOURCE_FILE}' was not found. "
        f"Please ensure the source file is uploaded to the staging volume "
        f"before running the pipeline."
    )

if os.path.getsize(SOURCE_FILE) == 0:
    raise ValueError(
        f"SOURCE FILE EMPTY: '{SOURCE_FILE}' exists but is 0 bytes. "
        f"The source file must contain data."
    )

print(f"✅ Source file validation passed: {SOURCE_FILE} ({os.path.getsize(SOURCE_FILE)} bytes)")

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Load Source CSV
# ============================================================
# Step 2: BRONZE Layer - Load source CSV as-is into Delta table
# ============================================================
# BRONZE layer keeps data in its original form with minimal transformation
BRZ_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SOURCE_FILE)
)

# Write to bronze Delta table (overwrite on re-run for idempotency)
(BRZ_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(BRZ_TABLE))

print(f"✅ Bronze layer loaded: {BRZ_TABLE} ({BRZ_df.count()} rows)")
display(spark.table(BRZ_TABLE))

# COMMAND ----------

# DBTITLE 1,Silver Layer - Clean & Standardize
# ============================================================
# Step 3: Silver Layer - Clean and standardize
# ============================================================
# basic data quality filtering (remove nulls, trim strings)
from pyspark.sql.functions import col, trim, to_date, regexp_replace

slv_df = (
    spark.table(BRZ_TABLE)
    # Rename columns to snake_case / standardized naming
    .withColumnRenamed("cust_id", "customer_id")
    .withColumnRenamed("full_name", "customer_name")
    .withColumnRenamed("signup_date", "signup_date_str")
    # Clean string columns (trim whitespace, lowercase email)
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("city", trim(col("city")))
    # Convert date string to proper date type
    .withColumn("signup_date", to_date(col("signup_date_str")))
    .withColumn("order_total", col("order_total").cast("decimal(10,2)"))
    # Drop the intermediate string column
    .drop("signup_date_str")
    # Data quality: filter out rows with null customer_id
    .filter(col("customer_id").isNotNull())
)

(slv_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(SLV_TABLE))

print(f"✅ Silver layer loaded: {SLV_TABLE} ({slv_df.count()} rows)")
display(spark.table(SLV_TABLE))

# COMMAND ----------

# DBTITLE 1,Gold Layer - Business Aggregation
# ============================================================
# Step 4: GOLDEN Layer - Business-level aggregation
# ============================================================
# Golden layer: aggregated metrics for business consumption
# Example: total orders and revenue by city
from pyspark.sql.functions import count, sum as _sum, round as _round

gold_df = (
    spark.table(SLV_TABLE)
    .groupBy("city")
    .agg(
        count("customer_id").alias("customer_count"),
        _sum("order_total").alias("total_revenue"),
        _round(_sum("order_total") / count("customer_id"), 2).alias("avg_order_value"),
    )
    .orderBy(col("total_revenue").desc())
)

(gold_df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(GLD_TABLE))

print(f"✅ Golden layer loaded: {GLD_TABLE} ({gold_df.count()} rows)")
display(spark.table(GLD_TABLE))

# COMMAND ----------

# DBTITLE 1,Cleanup - Remove Source File
# ============================================================
# Cleanup: Remove the source file so the next run recreates it
# ============================================================
import os

if os.path.exists(SOURCE_FILE):
    os.remove(SOURCE_FILE)
    print(f"🗑 Removed source file: {SOURCE_FILE}")
else:
    print(f"Source file already removed: {SOURCE_FILE}")