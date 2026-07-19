"""
AWS Lambda Function
Trigger: S3 Event (ObjectCreated) on raw/ prefix
Action:  
  1. Start Raw Crawler       → catalogs raw CSV schema
  2. Start Glue ETL Job      → transforms CSV → Parquet in processed/
  3. Poll ETL job until done → wait for completion
  4. Start Processed Crawler → catalogs Parquet schema for Athena
Author:  Suraj | Helpdesk Portfolio Project
"""

import boto3
import os
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────
# CONFIGURATION — set these as Lambda
# Environment Variables in AWS Console
# ─────────────────────────────────────────
RAW_CRAWLER_NAME       = os.environ.get("RAW_CRAWLER_NAME",       "helpdesk-raw-crawler")
PROCESSED_CRAWLER_NAME = os.environ.get("PROCESSED_CRAWLER_NAME", "helpdesk-processed-crawler")
GLUE_JOB_NAME          = os.environ.get("GLUE_JOB_NAME",          "helpdesk-etl-job")
EXPECTED_PREFIX        = os.environ.get("EXPECTED_PREFIX",        "raw/")
POLL_INTERVAL          = int(os.environ.get("POLL_INTERVAL",      "30"))
MAX_WAIT_TIME          = int(os.environ.get("MAX_WAIT_TIME",       "1200"))

glue = boto3.client("glue")


def lambda_handler(event, context):
    """
    Full pipeline trigger with strict sequencing:
    S3 upload → Raw Crawler → ETL Job → (wait) → Processed Crawler → Athena ready

    Each step only runs if the previous one succeeded.
    """

    # ── 1. Parse S3 event ──────────────────────────────────────────────────
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = record["s3"]["object"]["key"]

        logger.info(f"[EVENT] New file detected: s3://{bucket}/{key}")

        # ── 2. Only process files in raw/ prefix ──────────────────────────
        if not key.startswith(EXPECTED_PREFIX):
            logger.info(f"[SKIP] '{key}' is not in '{EXPECTED_PREFIX}' — ignoring.")
            continue

        # ── 3. Start Raw Crawler ───────────────────────────────────────────
        raw_crawler_started = start_crawler(RAW_CRAWLER_NAME)

        if not raw_crawler_started:
            logger.error(
                "[STOPPED] Raw crawler did not start. "
                "ETL job and processed crawler will NOT run."
            )
            raise Exception(
                f"Pipeline aborted: Raw crawler '{RAW_CRAWLER_NAME}' could not be started. "
                f"It may already be RUNNING or STOPPING. Retry after it finishes."
            )

        logger.info("[STEP 1 ✅] Raw crawler started successfully.")

        # ── 4. Start Glue ETL Job ──────────────────────────────────────────
        job_run_id = start_etl_job(bucket, key)
        logger.info("[STEP 2 ✅] ETL job started successfully.")

        # ── 5. Wait for ETL Job to finish ──────────────────────────────────
        job_succeeded = wait_for_job(job_run_id)

        if not job_succeeded:
            logger.error(
                "[STOPPED] ETL job failed. "
                "Processed crawler will NOT run."
            )
            raise Exception(
                f"Pipeline aborted: Glue ETL job '{GLUE_JOB_NAME}' did not succeed. "
                f"Check Glue job run logs for details. Run ID: {job_run_id}"
            )

        logger.info("[STEP 3 ✅] ETL job completed successfully.")

        # ── 6. Start Processed Crawler ─────────────────────────────────────
        processed_crawler_started = start_crawler(PROCESSED_CRAWLER_NAME)

        if not processed_crawler_started:
            logger.error(
                "[WARNING] Processed crawler did not start. "
                "Parquet data is in S3 but Athena schema may not be updated."
            )
            raise Exception(
                f"Pipeline partially complete: ETL succeeded but processed crawler "
                f"'{PROCESSED_CRAWLER_NAME}' could not start. Trigger it manually in Glue console."
            )

        logger.info("[STEP 4 ✅] Processed crawler started. Athena will be ready shortly.")
        logger.info("[DONE ✅] Full pipeline completed successfully.")

    return {
        "statusCode": 200,
        "body": "Pipeline completed successfully."
    }


# ─────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────

def start_crawler(crawler_name: str) -> bool:
    """
    Start a Glue Crawler only if it is in READY state.
    Returns True if crawler was started successfully.
    Returns False if crawler was already running or could not start.
    """
    try:
        state = get_crawler_state(crawler_name)

        if state == "READY":
            glue.start_crawler(Name=crawler_name)
            logger.info(f"[SUCCESS] Crawler '{crawler_name}' started.")
            return True  # ← crawler started successfully

        else:
            # Crawler already RUNNING or STOPPING — cannot start
            logger.error(
                f"[FAILED] Crawler '{crawler_name}' is currently '{state}'. "
                f"Cannot start. Wait for it to finish and retry."
            )
            return False  # ← crawler did NOT start

    except Exception as e:
        logger.error(f"[ERROR] Exception while starting crawler '{crawler_name}': {str(e)}")
        return False  # ← unexpected AWS error


def start_etl_job(bucket: str, key: str) -> str:
    """Start the Glue ETL job and return its run ID."""
    try:
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--source_bucket": bucket,
                "--source_key": key
            }
        )
        job_run_id = response["JobRunId"]
        logger.info(f"[SUCCESS] ETL job '{GLUE_JOB_NAME}' started. Run ID: {job_run_id}")
        return job_run_id
    except Exception as e:
        logger.error(f"[ERROR] Failed to start ETL job: {str(e)}")
        raise


def wait_for_job(job_run_id: str) -> bool:
    """
    Poll Glue ETL job status every POLL_INTERVAL seconds.
    Returns True if SUCCEEDED, False if FAILED/ERROR/TIMEOUT/STOPPED.
    """
    elapsed = 0

    while elapsed < MAX_WAIT_TIME:
        response  = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        status    = response["JobRun"]["JobRunState"]

        logger.info(f"[POLL] ETL job status: {status} (elapsed: {elapsed}s)")

        if status == "SUCCEEDED":
            logger.info(f"[SUCCESS] ETL job completed in {elapsed}s.")
            return True

        elif status in ("FAILED", "ERROR", "TIMEOUT", "STOPPED"):
            error_msg = response["JobRun"].get("ErrorMessage", "No error message.")
            logger.error(f"[FAILED] ETL job ended with '{status}': {error_msg}")
            return False

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    logger.error(f"[TIMEOUT] ETL job did not finish within {MAX_WAIT_TIME}s.")
    return False


def get_crawler_state(crawler_name: str) -> str:
    """Returns the current state of a Glue Crawler."""
    response = glue.get_crawler(Name=crawler_name)
    return response["Crawler"]["State"]  # READY | RUNNING | STOPPING
