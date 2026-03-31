# ============================================
# Azure Databricks — Data Transformation
# Author: Sadhana S Kumar
# Tools: PySpark, Azure Databricks
# Description: Large scale data transformation
#              for business performance data mart
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# ── Initialize Spark Session ──────────────────
spark = SparkSession.builder \
    .appName("BusinessDataTransformation") \
    .getOrCreate()

# ── Parameters (passed from ADF pipeline) ─────
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("client_id", "")

start_date = dbutils.widgets.get("start_date")
end_date   = dbutils.widgets.get("end_date")
client_id  = dbutils.widgets.get("client_id")

print(f"Running transformation for:")
print(f"  Start Date : {start_date}")
print(f"  End Date   : {end_date}")
print(f"  Client ID  : {client_id}")


# ── Step 1: Read Raw Data ─────────────────────
print("\n📥 Reading raw data...")

transactions_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://your-server.database.windows.net") \
    .option("dbtable", "fact_transactions") \
    .load() \
    .filter(
        (F.col("transaction_date") >= start_date) &
        (F.col("transaction_date") <= end_date)
    )

clients_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://your-server.database.windows.net") \
    .option("dbtable", "dim_clients") \
    .load()

products_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://your-server.database.windows.net") \
    .option("dbtable", "dim_products") \
    .load()

print(f"  Transactions loaded : {transactions_df.count()} rows")
print(f"  Clients loaded      : {clients_df.count()} rows")
print(f"  Products loaded     : {products_df.count()} rows")


# ── Step 2: Clean & Validate Data ─────────────
print("\n🧹 Cleaning data...")

transactions_clean = transactions_df \
    .dropDuplicates(["order_id"]) \
    .filter(F.col("revenue").isNotNull()) \
    .filter(F.col("transaction_date").isNotNull()) \
    .filter(F.col("client_id").isNotNull())

null_count = transactions_df.count() - transactions_clean.count()
print(f"  Removed {null_count} duplicate/null rows")


# ── Step 3: Join & Enrich Data ────────────────
print("\n🔗 Joining tables...")

enriched_df = transactions_clean \
    .join(clients_df,  "client_id",  "left") \
    .join(products_df, "product_id", "left") \
    .select(
        "order_id",
        "client_id",
        "client_name",
        "client_segment",
        "client_region",
        "product_id",
        "product_name",
        "product_category",
        "transaction_date",
        F.year("transaction_date").alias("year"),
        F.month("transaction_date").alias("month"),
        F.quarter("transaction_date").alias("quarter"),
        "revenue",
        "cost",
        (F.col("revenue") - F.col("cost")).alias("profit"),
        F.round(
            (F.col("revenue") - F.col("cost")) * 100
            / F.col("revenue"), 2
        ).alias("profit_margin_pct"),
        "processing_time",
        "status",
        F.when(F.col("status") == "completed", 1)
         .otherwise(0).alias("is_completed")
    )

print(f"  Enriched rows: {enriched_df.count()}")


# ── Step 4: Calculate KPIs ────────────────────
print("\n📊 Calculating KPIs...")

window_client_month = Window \
    .partitionBy("client_name") \
    .orderBy("year", "month")

kpi_df = enriched_df \
    .groupBy("client_name", "year", "month", "quarter") \
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("profit").alias("total_profit"),
        F.avg("profit_margin_pct").alias("avg_margin_pct"),
        F.count("order_id").alias("total_orders"),
        F.avg("processing_time").alias("avg_processing_time"),
        F.sum("is_completed").alias("completed_orders")
    ) \
    .withColumn(
        "prev_month_revenue",
        F.lag("total_revenue").over(window_client_month)
    ) \
    .withColumn(
        "revenue_growth_pct",
        F.round(
            (F.col("total_revenue") - F.col("prev_month_revenue"))
            * 100 / F.col("prev_month_revenue"), 2
        )
    )


# ── Step 5: Write to Data Warehouse ───────────
print("\n📤 Writing to data warehouse...")

enriched_df.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://your-server.database.windows.net") \
    .option("dbtable", "mart_business_performance") \
    .mode("overwrite") \
    .save()

kpi_df.write \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://your-server.database.windows.net") \
    .option("dbtable", "mart_kpi_summary") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Transformation complete at {datetime.now()}")
print(f"  mart_business_performance : {enriched_df.count()} rows")
print(f"  mart_kpi_summary          : {kpi_df.count()} rows")
