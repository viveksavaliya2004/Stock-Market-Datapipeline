import json
import logging
import os
import time
import io
from io import StringIO
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

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
KAFKA_TOPIC_REALTIME    = os.getenv('KAFKA_TOPIC_REALTIME')
KAFKA_GROUP_ID          = os.getenv('KAFKA_GROUP_REALTIME_ID')

# MinIO configuration
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET     = os.getenv('MINIO_BUCKET')

# Derive Docker-safe MinIO endpoint (strip http:// / https:// scheme)
_minio_connection = os.getenv('MINIO_CONNECTION', 'http://minio:9000')
MINIO_ENDPOINT = _minio_connection.replace("https://", "").replace("http://", "")

# Flush to MinIO when either of these thresholds is crossed
DEFAULT_BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 60


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


def flush_to_minio(minio_client: Minio, messages: list) -> bool:
    """
    Write the accumulated message list to MinIO as a single CSV.
    Returns True on success, False on failure.
    """
    if not messages:
        return True

    df = pd.DataFrame(messages)

    now       = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    object_name = (
        f"raw/realtime/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"stock_data_{timestamp}.csv"
    )

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes   = csv_buffer.getvalue().encode("utf-8")
    data_stream = io.BytesIO(csv_bytes)

    try:
        minio_client.put_object(
            bucket_name  = MINIO_BUCKET,
            object_name  = object_name,
            data         = data_stream,
            length       = len(csv_bytes),
            content_type = "text/csv",
        )
        logger.info(
            f"Flushed {len(messages)} message(s) → s3://{MINIO_BUCKET}/{object_name}"
        )
        return True

    except S3Error as e:
        logger.error(f"S3Error writing batch to MinIO: {e}")
        return False


def main():
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers':  KAFKA_BOOTSTRAP_SERVERS,
        'group.id':           KAFKA_GROUP_ID,
        'auto.offset.reset':  'earliest',
        'enable.auto.commit': False,   # we commit manually after successful S3 write
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_REALTIME])
    logger.info(
        f"Subscribed to '{KAFKA_TOPIC_REALTIME}' | "
        f"batch_size={DEFAULT_BATCH_SIZE} | flush_interval={FLUSH_INTERVAL_SECONDS}s"
    )

    messages   = []
    flush_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # No message — check if flush interval has elapsed with pending data
                current_time = time.time()
                if messages and (current_time - flush_time >= FLUSH_INTERVAL_SECONDS):
                    logger.info(
                        f"Flush interval reached with {len(messages)} pending message(s)"
                    )
                    success = flush_to_minio(minio_client, messages)
                    if success:
                        # --------- BUG FIX 1 — commit only on successful write -----------
                        consumer.commit()
                        messages   = []
                        flush_time = time.time()
                    else:
                        logger.warning("S3 write failed — retaining messages for next flush")
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append(value)

                if len(messages) % 10 == 0:
                    logger.info(f"Buffered {len(messages)} message(s) in current batch")

                current_time = time.time()
                should_flush = (
                    len(messages) >= DEFAULT_BATCH_SIZE or
                    (current_time - flush_time >= FLUSH_INTERVAL_SECONDS and messages)
                )

                if should_flush:
                    success = flush_to_minio(minio_client, messages)

                    # ------ BUG FIX 2 — only reset state on successful write ------
                    # Original code always reset messages=[] and flush_time
                    # regardless of whether the S3Error block was hit. This
                    # caused silent data loss when MinIO was temporarily down.
                    #
                    # Fix: only clear the buffer and commit if the write succeeded.
                    # On failure, keep messages in the buffer and retry next cycle.
                    # ---------------------------------------------------------------
                    if success:
                        # ----- BUG FIX 3 — consumer.commit() was never called -------
                        # enable.auto.commit=False but commit() was missing.
                        # On restart the consumer would replay from the last
                        # committed offset (the beginning), causing duplicate
                        # files in MinIO.
                        #
                        # Fix: commit AFTER the S3 write succeeds, so offsets
                        # are only advanced once data is safely stored.
                        # -------------------------------------------------------------
                        consumer.commit()
                        messages   = []
                        flush_time = time.time()
                    else:
                        logger.warning(
                            f"S3 write failed — keeping {len(messages)} message(s) "
                            f"in buffer, will retry next flush cycle"
                        )

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted — flushing remaining messages before exit…")

        # ----------- BUG FIX 4 — flush remaining messages on shutdown -------------
        # Original code closed the consumer immediately on KeyboardInterrupt,
        # dropping all messages still buffered in the messages[] list.
        #
        # Fix: attempt a final flush before closing.
        # ---------------------------------------------------------------------------
        if messages:
            logger.info(f"Flushing {len(messages)} remaining message(s)…")
            success = flush_to_minio(minio_client, messages)
            if success:
                consumer.commit()
                logger.info("Final flush complete and offsets committed")
            else:
                logger.error(
                    "Final flush failed — these messages will be replayed on next start"
                )
        else:
            logger.info("No pending messages to flush")

    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    main()
