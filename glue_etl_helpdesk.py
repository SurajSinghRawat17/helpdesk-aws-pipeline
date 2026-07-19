"""
AWS Glue PySpark ETL Script
Dataset: synthetic_it_support_tickets.csv (100,000 rows, 20 columns)
Pipeline: S3 (raw/) → Glue ETL → S3 (processed/) as Parquet
Author: Suraj | Helpdesk Portfolio Project
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ─────────────────────────────────────────
# 1. INIT GLUE JOB
# ─────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ─────────────────────────────────────────
# 2. CONFIGURATION — UPDATE THESE VALUES
# ─────────────────────────────────────────
INPUT_PATH  = "s3://your-bucket-name/raw/synthetic_it_support_tickets.csv"
OUTPUT_PATH = "s3://your-bucket-name/processed/"

# ─────────────────────────────────────────
# 3. READ RAW CSV FROM S3
# ─────────────────────────────────────────
df = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv(INPUT_PATH)


# ─────────────────────────────────────────
# 4. CLEAN: FIX NULLS
# ─────────────────────────────────────────

# Region has ~20,000 nulls — fill with "Unknown"
df = df.withColumn(
    "region",
    F.when(F.col("region").isNull(), "Unknown").otherwise(F.col("region"))
)

# resolution_summary null = ticket not yet resolved — fill with "Pending"
df = df.withColumn(
    "resolution_summary",
    F.when(F.col("resolution_summary").isNull(), "Pending").otherwise(F.col("resolution_summary"))
)

# resolution_time_hours null = ticket still open — fill with 0.0
df = df.withColumn(
    "resolution_time_hours",
    F.when(F.col("resolution_time_hours").isNull(), 0.0).otherwise(F.col("resolution_time_hours"))
)

# ─────────────────────────────────────────
# 5. CLEAN: STANDARDIZE TEXT COLUMNS
# ─────────────────────────────────────────

# Capitalize region, segment, status, priority for consistency in Power BI
text_cols = ["region", "customer_segment", "status", "priority",
             "sla_plan", "customer_sentiment", "channel",
             "product_area", "issue_type", "platform"]

for col in text_cols:
    df = df.withColumn(col, F.initcap(F.col(col)))

# ─────────────────────────────────────────
# 6. TRANSFORM: PARSE TIMESTAMPS
# ─────────────────────────────────────────

# created_at is ISO string — convert to proper timestamp
df = df.withColumn(
    "created_at",
    F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss")
)


# ─────────────────────────────────────────
# 7. TRANSFORM: DERIVED COLUMNS
# ─────────────────────────────────────────

# Resolution flag — was the ticket actually resolved?
df = df.withColumn(
    "is_resolved",
    F.when(F.col("status") == "Resolved", 1).otherwise(0)
)

# SLA breach flag — based on sla_plan thresholds (business rule)
# Standard: 48hrs, Gold: 24hrs, Platinum: 8hrs
df = df.withColumn(
    "sla_breached",
    F.when(
        (F.col("sla_plan") == "Standard") & (F.col("resolution_time_hours") > 48), 1
    ).when(
        (F.col("sla_plan") == "Gold") & (F.col("resolution_time_hours") > 24), 1
    ).when(
        (F.col("sla_plan") == "Platinum") & (F.col("resolution_time_hours") > 8), 1
    ).otherwise(0)
)

# Sentiment score — convert text sentiment to numeric for aggregations
sentiment_map = {
    "Very_Negative": -2,
    "Negative": -1,
    "Neutral": 0,
    "Positive": 1,
    "Very_Positive": 2
}
df = df.withColumn(
    "sentiment_score",
    F.when(F.col("customer_sentiment") == "Very_Negative", -2)
     .when(F.col("customer_sentiment") == "Negative", -1)
     .when(F.col("customer_sentiment") == "Neutral", 0)
     .when(F.col("customer_sentiment") == "Positive", 1)
     .when(F.col("customer_sentiment") == "Very_Positive", 2)
     .otherwise(0)
)

# CSAT bucket — group scores for easier slicing in Power BI
df = df.withColumn(
    "csat_bucket",
    F.when(F.col("csat_score") <= 2, "Detractor")   # 1-2
     .when(F.col("csat_score") == 3, "Neutral")      # 3
     .otherwise("Promoter")                           # 4-5
)

# Resolution speed category
df = df.withColumn(
    "resolution_speed",
    F.when(F.col("resolution_time_hours") == 0, "Unresolved")
     .when(F.col("resolution_time_hours") <= 4,  "Fast")      # within 4 hrs
     .when(F.col("resolution_time_hours") <= 24, "Normal")    # 4–24 hrs
     .when(F.col("resolution_time_hours") <= 72, "Slow")      # 24–72 hrs
     .otherwise("Very Slow")                                   # 72+ hrs
)

# Long text columns bloat Parquet & slow Power BI — drop for processed layer
# Keep them in raw/ if needed later for NLP
drop_cols = ["initial_message", "agent_first_reply"]
df = df.drop(*drop_cols)


# ─────────────────────────────────────────
# 8. WRITE TO S3 AS PARQUET
# ─────────────────────────────────────────

# Partition by region — makes Athena queries cheaper & faster
df.write \
  .mode("overwrite") \
  .parquet(OUTPUT_PATH)

print(f"[INFO] Data written to {OUTPUT_PATH} partitioned by region")

# ─────────────────────────────────────────
# 9. COMMIT JOB
# ─────────────────────────────────────────
job.commit()
print("[INFO] Glue job completed successfully.")
