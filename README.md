# Helpdesk Data Pipeline — AWS

An end-to-end automated data engineering pipeline built on AWS, processing IT helpdesk support tickets from raw ingestion to analytics-ready output.

---

## Architecture

```
Raw CSV (S3)
    ↓
Lambda                  -> triggered automatically on S3 upload
    ├── Glue Crawler 1  -> catalogs raw CSV schema in Glue Data Catalog
    ├── Glue ETL Job    -> cleans, transforms, writes Parquet to S3
    └── Glue Crawler 2  -> catalogs processed Parquet schema (after ETL finishes)
    ↓
Athena                  -> SQL-queryable analytics layer
```

---

## Tech Stack
```
| Layer -> Technology |

| Storage -> AWS S3 |
| Cataloging -> AWS Glue Data Catalog |
| ETL -> AWS Glue (PySpark) |
| Automation -> AWS Lambda (Python 3.12) |
| Eventing -> Amazon CloudWatch Events |
| Query Layer -> Amazon Athena |
| Language -> Python, PySpark, SQL |
```
---

## Dataset

Synthetic IT helpdesk support tickets dataset with 100,000 rows and 20 columns including:

- Ticket metadata: `ticket_id`, `created_at`, `priority`, `status`, `channel`
- Customer info: `customer_id`, `customer_segment`, `sla_plan`, `region`
- Issue details: `product_area`, `issue_type`, `platform`
- Resolution: `resolution_time_hours`, `reopened`, `resolution_summary`
- Satisfaction: `csat_score`, `customer_sentiment`

---

## Pipeline Components

### 1. Glue ETL Job (`glue_etl/glue_etl_helpdesk.py`)

PySpark script that performs the following transformations:

**Cleaning**
- Fills null `region` values with `"Unknown"`
- Fills null `resolution_summary` with `"Pending"` for unresolved tickets
- Fills null `resolution_time_hours` with `0.0`
- Standardizes all text columns to title case

**Feature Engineering**
- Adds `is_resolved` flag (1/0)
- Adds `sla_breached` flag based on SLA plan thresholds (Standard: 48hrs, Gold: 24hrs, Platinum: 8hrs)
- Adds `sentiment_score` (-2 to +2 numeric mapping)
- Adds `csat_bucket` (Detractor / Neutral / Promoter)
- Adds `resolution_speed` (Fast / Normal / Slow / Very Slow / Unresolved)

**Output**
- Converts to Parquet format

---

### 2. Lambda Functions (`lambda/`)

**lambda_pipeline_start.py**
- Triggered by S3 ObjectCreated event on raw/ prefix
- Filters events to only process files uploaded to raw/, prevents infinite loop triggers
- Starts Glue raw crawler to catalog incoming CSV schema
- Starts Glue ETL job immediately after
- Polls ETL job status every 30 seconds until completion
- Only starts processed Glue crawler after ETL job succeeds
- Each step is gated, if any step fails, downstream steps do not run
- All events logged to CloudWatch for monitoring

---

### 3. IAM Policy (`iam/lambda_iam_policy.json`)

Least-privilege IAM policy for Lambda execution role covering:
- `glue:StartCrawler`, `glue:GetCrawler`
- `glue:StartJobRun`, `glue:GetJobRun`
- `s3:GetObject`, `s3:ListBucket`, `s3:PutObject`
- `logs:CreateLogGroup`, `logs:PutLogEvents`

---

## S3 Bucket Structure

```
helpdesk-project-bucket/
    ├── raw/                  ← upload CSV files here
    ├── processed/            ← Glue ETL writes Parquet here
    │         └── part-XXXX.parquet
    └── athena-results/       ← Athena query output
```

---

## Glue Crawler Settings
```
| Setting | Raw Crawler | Processed Crawler |

| Name           | helpdesk-raw-crawler    | helpdesk-processed-crawler |
| S3 Path        | `s3://bucket/raw/`      | `s3://bucket/processed/`   |
| Database       | helpdesk_db             | helpdesk_db                |
| Table Prefix   | raw_                    | processed_                 |
| Schedule       | On demand               | On demand                  |
| Recrawl        | All                     | All                        |
```
---


## Setup Instructions

### Prerequisites
- AWS account with free tier
- Python 3.12
- IAM user with S3, Glue, Lambda, Athena, CloudWatch permissions

### Deployment Steps

1. Create S3 bucket with raw/, processed/, athena-results/ folders
2. Create Glue crawlers using settings in table above
3. Create Glue ETL job — attach glue_etl/glue_etl_helpdesk.py
4. Create Lambda function — attach lambda/lambda_helpdesk_pipeline.py
5. Add S3 trigger on Lambda — prefix: raw/, event type: PUT
6. Attach IAM policy from iam/lambda_iam_policy.json to Lambda execution role
7. Set Lambda timeout to 15 minutes (Configuration → General)
8. Set Lambda environment variables:

```
RAW_CRAWLER_NAME       = helpdesk-raw-crawler
PROCESSED_CRAWLER_NAME = helpdesk-processed-crawler
GLUE_JOB_NAME          = helpdesk-etl-job
EXPECTED_PREFIX        = raw/
```

9. Upload CSV to `raw/` — pipeline triggers automatically

---

## Key Design Decisions

**Event-driven ingestion** — S3 event notification triggers Lambda automatically on file upload. No manual intervention or scheduled jobs needed.

**Strict step gating** — each pipeline step only runs if the previous one succeeded. Raw crawler failure stops ETL. ETL failure stops processed crawler. Prevents partial or corrupt data reaching Athena.

**Parquet** — processed data stored as Parquet. Reduces Athena scan cost and improves query performance on time-based filters.

**Least-privilege IAM** — Lambda role has only the specific Glue and S3 permissions required. No wildcard admin access.

**Null handling strategy** — nulls filled with meaningful defaults rather than dropped, preserving all rows for analysis.

**Polling Method** — Used in this Project.

---

## Cost

This pipeline runs at near-zero cost on AWS free tier:
```
| Service | Cost |
|---|---|
| S3 (few MBs) | Free |
| Glue ETL (small job) | Free under 10 DPU-hours/month |
| Lambda (2 functions, ~1 sec each) | Free under 1M requests/month |
| CloudWatch Events | Free |
| Athena (MBs scanned) | ~₹0 |
```
---

## Author

Suraj — Data Engineer / BI Developer  
