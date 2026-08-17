import os
import sys
import traceback
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')
MINIO_CONNECTION = os.getenv('MINIO_CONNECTION')


def create_spark_session():
    logger.info("Initializing Spark Session with S3 Configuration")

    spark = (SparkSession.builder
             .appName("StockMarketStreamingProcessor")
             .config("spark.jars.packages",
                     "org.apache.hadoop:hadoop-aws:3.3.1,"
                     "com.amazonaws:aws-java-sdk-bundle:1.11.901")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .config("spark.executor.memory", "1g")
             .config("spark.executor.cores", "1")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())

    spark_conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark_conf.set("fs.s3a.endpoint", MINIO_CONNECTION)
    spark_conf.set("fs.s3a.path.style.access", "true")
    spark_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark_conf.set("fs.s3a.credentials.provider",
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully")
    return spark


def define_schema():
    """
    Schema aligned to stream_data_producer output:
      symbol, price, change, percent_change, volume, latest_trading_day,
      previous_close, timestamp
    """
    logger.info("Defining schema for stock data…")
    return StructType([
        StructField("symbol",             StringType(),  False),
        StructField("price",              DoubleType(),  True),
        StructField("change",             DoubleType(),  True),
        StructField("percent_change",     DoubleType(),  True),
        StructField("volume",             IntegerType(), True),
        StructField("latest_trading_day", StringType(),  True),
        StructField("previous_close",     DoubleType(),  True),
        StructField("timestamp",          StringType(),  True),
    ])


def log_raw_data(df, batch_id):
    """Log raw data from each micro-batch."""
    count = df.count()
    if count > 0:
        logger.info(f"Raw batch {batch_id}: {count} rows")
        for row in df.collect():
            logger.info(f"  RAW → {row.asDict()}")
    else:
        logger.info(f"Raw batch {batch_id}: 0 rows (skipping)")


def process_and_write_batch(df, batch_id):
    """Write each processed micro-batch to MinIO as Parquet."""
    count = df.count()
    if count > 0:
        logger.info(f"Processed batch {batch_id}: {count} rows")
        for row in df.collect():
            logger.info(f"  PROCESSED → {row.asDict()}")

        output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
        logger.info(f"Writing batch {batch_id} to {output_path}")
        (df.write
           .mode("append")
           .partitionBy("symbol")
           .parquet(output_path))
    else:
        logger.info(f"Processed batch {batch_id}: 0 rows (skipping)")


def read_stream_from_s3(spark):
    """Set up a streaming read of CSV files from the MinIO realtime bucket."""
    logger.info("Setting up streaming read from S3…")
    schema = define_schema()
    s3_path = f"s3a://{MINIO_BUCKET}/raw/realtime/"
    logger.info(f"Streaming source path: {s3_path}")

    try:
        streaming_df = (spark.readStream
                        .schema(schema)
                        .option("header", "true")
                        .csv(s3_path))

        # Cast / clean columns
        streaming_df = (streaming_df
                        .withColumn("timestamp",      F.to_timestamp("timestamp"))
                        .withColumn("price",          F.col("price").cast(DoubleType()))
                        .withColumn("change",         F.col("change").cast(DoubleType()))
                        .withColumn("percent_change", F.col("percent_change").cast(DoubleType()))
                        .withColumn("volume",         F.col("volume").cast(IntegerType())))

        logger.info("Streaming DataFrame schema:")
        logger.info("\n" + streaming_df._jdf.schema().treeString())

        # Kick off a side stream just for logging raw data
        (streaming_df.writeStream
                     .foreachBatch(log_raw_data)
                     .outputMode("append")
                     .start())

        return streaming_df

    except Exception as e:
        logger.error(f"Error setting up streaming read from {s3_path}: {e}")
        logger.error(traceback.format_exc())
        return None


def process_streaming_data(streaming_df):
    """
    Calculate sliding-window metrics from the streaming DataFrame:
      - 15-minute and 1-hour moving averages, volatility, and volume sums.
    """
    logger.info("Processing streaming stock data…")

    if streaming_df is None:
        logger.warning("No streaming DataFrame to process")
        return None

    try:
        # Watermark to handle late-arriving data
        streaming_df = streaming_df.withWatermark("timestamp", "5 minutes")

        window_15min = F.window("timestamp", "15 minutes", "5 minutes")
        window_1h    = F.window("timestamp", "1 hour",     "10 minutes")

        df_15min = (streaming_df
                    .groupBy(F.col("symbol"), window_15min.alias("window"))
                    .agg(
                        F.avg("price").alias("ma_15m"),
                        F.stddev("price").alias("volatility_15m"),
                        F.sum("volume").alias("volume_sum_15m"),
                    )
                    .withColumn("window_start", F.col("window.start"))
                    .withColumn("window_end",   F.col("window.end"))
                    .drop("window"))

        df_1h = (streaming_df
                 .groupBy(F.col("symbol"), window_1h.alias("window"))
                 .agg(
                     F.avg("price").alias("ma_1h"),
                     F.stddev("price").alias("volatility_1h"),
                     F.sum("volume").alias("volume_sum_1h"),
                 )
                 .withColumn("window_start", F.col("window.start"))
                 .withColumn("window_end",   F.col("window.end"))
                 .drop("window"))

        processed_df = (df_15min
                        .join(
                            df_1h,
                            (df_15min.symbol == df_1h.symbol) &
                            (df_15min.window_start == df_1h.window_start),
                            "inner",
                        )
                        .select(
                            df_15min.symbol,
                            df_15min.window_start.alias("window_start"),
                            df_15min.window_end.alias("window_15m_end"),
                            df_1h.window_end.alias("window_1h_end"),
                            df_15min.ma_15m,
                            df_1h.ma_1h,
                            df_15min.volatility_15m,
                            df_1h.volatility_1h,
                            df_15min.volume_sum_15m,
                            df_1h.volume_sum_1h,
                        ))

        logger.info("Processed streaming DataFrame schema:")
        logger.info("\n" + processed_df._jdf.schema().treeString())
        return processed_df

    except Exception as e:
        logger.error(f"Error processing streaming data: {e}")
        logger.error(traceback.format_exc())
        return None


def write_stream_to_s3(processed_df):
    """Write the processed streaming DataFrame to MinIO using foreachBatch."""
    logger.info("Writing processed streaming data to S3…")

    if processed_df is None:
        logger.error("No processed DataFrame to write")
        return None

    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
    checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/streaming_processor"
    logger.info(f"Output path: {output_path}")

    try:
        query = (processed_df.writeStream
                 .foreachBatch(process_and_write_batch)
                 .trigger(processingTime='1 minute')
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .start())

        logger.info(f"Streaming query started → {output_path}")
        return query

    except Exception as e:
        logger.error(f"Error starting streaming write: {e}")
        logger.error(traceback.format_exc())
        return None


def main():
    logger.info("=" * 50)
    logger.info("STARTING STOCK MARKET STREAMING PROCESSOR")
    logger.info("=" * 50)

    spark = create_spark_session()

    try:
        # Step 1: Set up streaming read
        streaming_df = read_stream_from_s3(spark)

        if streaming_df is None:
            logger.error("Failed to set up streaming read — exiting")
            return

        # -------------------------- BUG FIX ---------------------------------
        # Was: process_streaming_data(df)   ← 'df' is undefined here
        # Fix: process_streaming_data(streaming_df)
        # --------------------------------------------------------------------
        processed_df = process_streaming_data(streaming_df)

        if processed_df is None:
            logger.error("Failed to process streaming data — exiting")
            return

        # Step 3: Write to S3
        query = write_stream_to_s3(processed_df)

        if query is not None:
            logger.info("Streaming processor is running… (awaiting termination)")
            query.awaitTermination()
        else:
            logger.error("Failed to start streaming query")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("Stopping Spark session…")
        spark.stop()
        logger.info("=" * 50)
        logger.info("STREAM PROCESSING COMPLETE")
        logger.info("=" * 50)


if __name__ == "__main__":
    main()
