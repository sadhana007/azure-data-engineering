# ============================================
# Databricks Workflow — Qlik to PBI Migration
# Author: Sadhana S Kumar
# Architecture: Medallion (Raw → Silver → Gold)
# Description: Read parquet files from ADLS,
#              write to Delta tables (Silver),
#              apply transformations (Gold)
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# ── Initialize Spark Session ──────────────────
spark = SparkSession.builder \
    .appName("QlikToPBI_Migration") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Parameters ────────────────────────────────
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("load_date", "")
dbutils.widgets.text("load_type", "incremental")

table_name = dbutils.widgets.get("table_name")
load_date  = dbutils.widgets.get("load_date")
load_type  = dbutils.widgets.get("load_type")

# ── Storage Paths ─────────────────────────────
ADLS_PATH    = "abfss://raw@yourstorage.dfs.core.windows.net"
SILVER_PATH  = "abfss://silver@yourstorage.dfs.core.windows.net"
GOLD_PATH    = "abfss://gold@yourstorage.dfs.core.windows.net"

print(f"🚀 Starting workflow for: {table_name}")
print(f"   Load type : {load_type}")
print(f"   Load date : {load_date}")


# ════════════════════════════════════════════
# SILVER LAYER — Raw Parquet → Delta Tables
# ════════════════════════════════════════════

def load_to_silver(table_name, load_date, load_type):
    print(f"\n📥 Loading {table_name} to Silver layer...")

    # Read parquet from Raw layer
    parquet_path = f"{ADLS_PATH}/{load_type}/{table_name}/load_date={load_date}/"

    raw_df = spark.read \
        .format("parquet") \
        .load(parquet_path)

    print(f"   Raw records read: {raw_df.count()}")

    # Add audit columns
    silver_df = raw_df \
        .withColumn("load_date",
                    F.lit(load_date).cast(DateType())) \
        .withColumn("load_timestamp",
                    F.current_timestamp()) \
        .withColumn("source_system",
                    F.lit("Oracle")) \
        .withColumn("load_type",
                    F.lit(load_type))

    # Write to Delta table (Silver)
    silver_table_path = f"{SILVER_PATH}/{table_name}"

    if load_type == "full_load":
        # Full load — overwrite entire table
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_table_path)
        print(f"   ✅ Full load complete — {silver_df.count()} rows")

    else:
        # Incremental — merge new records
        if DeltaTable.isDeltaTable(spark, silver_table_path):
            delta_table = DeltaTable.forPath(
                spark, silver_table_path
            )
            delta_table.alias("target") \
                .merge(
                    silver_df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            print(f"   ✅ Incremental merge complete")
        else:
            # First load — create table
            silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(silver_table_path)
            print(f"   ✅ Initial load complete")

    # Register in Hive metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver.{table_name}
        USING DELTA
        LOCATION '{silver_table_path}'
    """)


# ════════════════════════════════════════════
# GOLD LAYER — Silver → Transformed Tables
# ════════════════════════════════════════════

def load_to_gold(table_name):
    print(f"\n⚡ Creating Gold layer for: {table_name}...")

    # Read from Silver
    silver_df = spark.read \
        .format("delta") \
        .load(f"{SILVER_PATH}/{table_name}")

    # Apply transformations per PBI team requirements
    gold_df = silver_df \
        .filter(F.col("is_active") == True) \
        .withColumn(
            "full_name",
            F.concat_ws(" ",
                F.col("first_name"),
                F.col("last_name"))
        ) \
        .withColumn(
            "year",
            F.year(F.col("transaction_date"))
        ) \
        .withColumn(
            "month",
            F.month(F.col("transaction_date"))
        ) \
        .withColumn(
            "quarter",
            F.quarter(F.col("transaction_date"))
        ) \
        .withColumn(
            "profit",
            F.col("revenue") - F.col("cost")
        ) \
        .withColumn(
            "profit_margin_pct",
            F.round(
                (F.col("revenue") - F.col("cost"))
                * 100 / F.col("revenue"), 2
            )
        ) \
        .drop("load_date", "load_timestamp",
              "source_system", "load_type")

    # Write to Gold Delta table
    gold_table_path = f"{GOLD_PATH}/{table_name}_gold"

    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_table_path)

    # Register in Hive metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.{table_name}_gold
        USING DELTA
        LOCATION '{gold_table_path}'
    """)

    print(f"   ✅ Gold table created: {gold_df.count()} rows")


# ════════════════════════════════════════════
# CHECKPOINT — Track processed files
# ════════════════════════════════════════════

def update_checkpoint(table_name, load_date, status):
    checkpoint_path = f"{ADLS_PATH}/checkpoints/"

    checkpoint_df = spark.createDataFrame([{
        "table_name"      : table_name,
        "load_date"       : load_date,
        "status"          : status,
        "processed_at"    : str(datetime.now()),
        "load_type"       : load_type
    }])

    checkpoint_df.write \
        .format("delta") \
        .mode("append") \
        .save(checkpoint_path)

    print(f"   ✅ Checkpoint updated: {status}")


# ════════════════════════════════════════════
# MAIN — Run the workflow
# ════════════════════════════════════════════

try:
    # Step 1 — Load to Silver
    load_to_silver(table_name, load_date, load_type)

    # Step 2 — Load to Gold
    load_to_gold(table_name)

    # Step 3 — Update checkpoint
    update_checkpoint(table_name, load_date, "SUCCESS")

    print(f"\n🎉 Workflow complete for {table_name}!")
    print(f"   Completed at: {datetime.now()}")

except Exception as e:
    update_checkpoint(table_name, load_date, "FAILED")
    print(f"\n❌ Workflow failed: {str(e)}")
    raise e
