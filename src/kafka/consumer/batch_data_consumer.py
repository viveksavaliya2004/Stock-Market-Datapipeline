import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from pathlib import Path
import tempfile

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_BATCH       = os.getenv('KAFKA_TOPIC_BATCH')
KAFKA_GROUP_ID          = os.getenv('KAFKA_GROUP_BATCH_ID')

# MinIO configuration
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET     = os.getenv('MINIO_BUCKET')

# ------------------  BUG FIX 1 — wrong MinIO endpoint inside Docker ----------
# MINIO_ENDPOINT in .env is "localhost:9000" — fine for running on the host,
# but this consumer is executed inside the Airflow container via BashOperator.
# Inside Docker, "localhost" resolves to the Airflow container itself, NOT MinIO.
#
# Fix: derive the endpoint from MINIO_CONNECTION ("http://minio:9000") which
# already uses the correct Docker service name. The Minio Python client needs
# the host:port form without a scheme prefix, so we strip "http://" / "https://".
# ------------------------------------------------------------------------------
_minio_connection = os.getenv('MINIO_CONNECTION', 'http://minio:9000')
MINIO_ENDPOINT = _minio_connection.replace("https://", "").replace("http://", "")

# -------------------- BUG FIX 2 — termination config ---------------------------
# The original consumer looped forever (while True / msg is None → continue).
# A BashOperator in Airflow expects the subprocess to EXIT, otherwise the task
# runs until Airflow kills it (execution_timeout) and marks it as failed.
#
# Fix: exit after IDLE_TIMEOUT_SECONDS of receiving no new messages.
# 30 s is generous — AlphaVantage batch for 10 stocks produces ~10 messages
# total and the producer flushes synchronously, so the topic is fully populated
# long before the consumer starts.
# --------------------------------------------------------------------------------
IDLE_TIMEOUT_SECONDS = 30   # exit after this many seconds of no new messages


def create_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket_exists(minio_client: Minio, bucket_name: str):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket already exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"Error checking/creating bucket '{bucket_name}': {e}")
        raise


def write_to_minio(minio_client: Minio, data: dict, symbol: str, batch_date: str):
    """Write a single record as a CSV row to MinIO under the correct partition path."""

    year, month, day = batch_date.split("-")   # batch_date is always YYYY-MM-DD

    object_name = (
        f"raw/historical/year={year}/month={month}/day={day}/"
        f"{symbol}_{datetime.now().strftime('%H%M%S%f')}.csv"
    )

    df = pd.DataFrame([data])

    tmp_dir  = Path(tempfile.gettempdir())
    csv_file = tmp_dir / f"{symbol}_{datetime.now().strftime('%H%M%S%f')}.csv"

    try:
        df.to_csv(csv_file, index=False)
        minio_client.fput_object(MINIO_BUCKET, object_name, str(csv_file))
        logger.info(f"Written: s3://{MINIO_BUCKET}/{object_name}")
    finally:
        # ------------------- BUG FIX 3 — safe cleanup -----------------------
        # Original code called csv_file.unlink() before consumer.commit().
        # If unlink() raised an OSError, commit() was never reached and the
        # message was re-consumed on the next run, duplicating the MinIO file.
        #
        # Fix: always attempt cleanup in a finally block that is separate from
        # the commit, so an unlink failure does not prevent the Kafka commit.
        # --------------------------------------------------------------------
        if csv_file.exists():
            try:
                csv_file.unlink()
            except OSError as e:
                logger.warning(f"Could not delete temp file {csv_file}: {e}")


def main():
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id':          KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])
    logger.info(f"Subscribed to topic '{KAFKA_TOPIC_BATCH}' | idle-timeout={IDLE_TIMEOUT_SECONDS}s")

    messages_processed = 0
    last_message_time  = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            # ------------------  BUG FIX 4 — idle exit ----------------------
            # If no message arrives within IDLE_TIMEOUT_SECONDS, the topic is
            # exhausted for this batch — exit so the DAG task can complete.
            # ----------------------------------------------------------------
            if msg is None:
                idle_seconds = time.time() - last_message_time
                if idle_seconds >= IDLE_TIMEOUT_SECONDS:
                    logger.info(
                        f"No new messages for {idle_seconds:.0f}s — "
                        f"assuming batch complete. Processed {messages_processed} message(s). Exiting."
                    )
                    break
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data       = json.loads(msg.value().decode("utf-8"))
                symbol     = data['symbol']
                batch_date = data['batch_date']

                write_to_minio(minio_client, data, symbol, batch_date)

                # Commit AFTER the MinIO write succeeds
                consumer.commit()
                messages_processed += 1
                last_message_time = time.time()

            except KeyError as e:
                logger.error(f"Missing expected field in message: {e} — skipping")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total messages processed: {messages_processed}")


if __name__ == "__main__":
    main()
