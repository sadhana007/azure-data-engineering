# ============================================
# Azure Databricks — Data Quality Checks
# Author: Sadhana S Kumar
# Tools: PySpark, Azure Databricks
# Description: Data quality validation checks
#              to ensure accuracy before loading
#              to data mart
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# ── Initialize Spark Session ──────────────────
spark = SparkSession.builder \
    .appName("DataQualityChecks") \
    .getOrCreate()

# ── Parameters ────────────────────────────────
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("load_date", "")

table_name = dbutils.widgets.get("table_name")
load_date  = dbutils.widgets.get("load_date")

print(f"🔍 Running data quality checks on: {table_name}")
print(f"   Load date: {load_date}")

# ── Read Data ─────────────────────────────────
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://your-server.database.windows.net") \
    .option("dbtable", table_name) \
    .load()

total_rows = df.count()
print(f"\n📊 Total rows: {total_rows}")

# ── Check 1: Null Values ──────────────────────
print("\n✅ Check 1: Null Values")

null_counts = df.select([
    F.count(
        F.when(F.col(c).isNull(), c)
    ).alias(c)
    for c in df.columns
])

null_counts.show()

# ── Check 2: Duplicate Records ────────────────
print("\n✅ Check 2: Duplicate Records")

duplicate_count = total_rows - df.dropDuplicates().count()
print(f"   Duplicate rows found: {duplicate_count}")

if duplicate_count > 0:
    print("   ⚠️ WARNING: Duplicates detected!")
else:
    print("   ✅ No duplicates found")

# ── Check 3: Negative Revenue ─────────────────
print("\n✅ Check 3: Negative Revenue Values")

if "revenue" in df.columns:
    negative_revenue = df.filter(F.col("revenue") < 0).count()
    print(f"   Negative revenue rows: {negative_revenue}")

    if negative_revenue > 0:
        print("   ⚠️ WARNING: Negative revenue detected!")
    else:
        print("   ✅ All revenue values are positive")

# ── Check 4: Date Range Validation ────────────
print("\n✅ Check 4: Date Range Validation")

if "transaction_date" in df.columns:
    date_stats = df.agg(
        F.min("transaction_date").alias("min_date"),
        F.max("transaction_date").alias("max_date")
    ).collect()[0]

    print(f"   Min date: {date_stats['min_date']}")
    print(f"   Max date: {date_stats['max_date']}")

    future_dates = df.filter(
        F.col("transaction_date") > F.current_date()
    ).count()

    if future_dates > 0:
        print(f"   ⚠️ WARNING: {future_dates} future dates found!")
    else:
        print("   ✅ All dates are valid")

# ── Check 5: Data Freshness ───────────────────
print("\n✅ Check 5: Data Freshness")

if "load_date" in df.columns:
    latest_load = df.agg(
        F.max("load_date").alias("latest_load")
    ).collect()[0]["latest_load"]

    print(f"   Latest load date: {latest_load}")

# ── Summary Report ────────────────────────────
print("\n" + "="*50)
print("📋 DATA QUALITY SUMMARY REPORT")
print("="*50)
print(f"   Table         : {table_name}")
print(f"   Load Date     : {load_date}")
print(f"   Total Rows    : {total_rows}")
print(f"   Duplicates    : {duplicate_count}")
if "revenue" in df.columns:
    print(f"   Negative Rev  : {negative_revenue}")
if "transaction_date" in df.columns:
    print(f"   Future Dates  : {future_dates}")
print(f"   Checked At    : {datetime.now()}")
print("="*50)

# ── Fail Pipeline if Critical Issues ──────────
if duplicate_count > 0 or (
    "revenue" in df.columns and negative_revenue > 0
):
    raise Exception(
        "❌ Data quality checks failed! Pipeline stopped."
    )
else:
    print("\n✅ All data quality checks passed!")
