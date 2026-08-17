import os
import sys
import traceback
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

import logging

# Env vars are injected by Docker Compose into the Spark container
# no dotenv needed here. os.getenv() reads them directly.

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET     = os.getenv('MINIO_BUCKET')
MINIO_CONNECTION = os.getenv('MINIO_CONNECTION')


def create_spark_session():
    print("Initializing Spark Session with S3 Configuration")

    spark = (SparkSession.builder
             .appName("StockMarketBatchProcessor")
             # ---------------------  BUG FIX 1 -------------------------------
             # Was:  "spark.jar.packages"   ← typo (missing 's')
             # Fix:  "spark.jars.packages"
             # Without this fix the Hadoop-AWS JARs are never loaded and every
             # S3 read/write fails immediately.
             # ----------------------------------------------------------------
             .config("spark.jars.packages",
                     "org.apache.hadoop:hadoop-aws:3.3.1,"
                     "com.amazonaws:aws-java-sdk-bundle:1.11.901")
             .getOrCreate())

    spark_conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark_conf.set("fs.s3a.access.key",          MINIO_ACCESS_KEY)
    spark_conf.set("fs.s3a.secret.key",          MINIO_SECRET_KEY)
    spark_conf.set("fs.s3a.endpoint",            MINIO_CONNECTION)
    spark_conf.set("fs.s3a.path.style.access",   "true")
    spark_conf.set("fs.s3a.impl",                "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark_conf.set("fs.s3a.credentials.provider",
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized successfully")
    return spark


def read_data_from_s3(spark, execution_date: str):
    """
    Read raw CSVs written by batch_data_consumer for the given execution_date.
    Path pattern: raw/historical/year=YYYY/month=MM/day=DD/
    """
    print(f"Reading data from S3 for execution_date={execution_date}")

    # ---------------------  BUG FIX 2 --------------------------------------------
    # Original code did:
    #   if date is None:
    #       process_date = datetime.now() - timedelta(days=2)   # datetime object
    #   else:
    #       process_date = datetime.strftime(date, "%Y-%m-%d")  # returns a STRING
    #   year = process_date.year   ← AttributeError when date was provided
    #
    # Fix: parse execution_date string once into a datetime object; .year/.month/.day
    # are always available on the datetime object.
    # -------------------------------------------------------------------------------
    try:
        process_date = datetime.strptime(execution_date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid execution_date format '{execution_date}'. Expected YYYY-MM-DD.")
        return None

    year  = process_date.year
    month = process_date.month
    day   = process_date.day

    s3_path = (
        f"s3a://{MINIO_BUCKET}/raw/historical/"
        f"year={year}/month={month:02d}/day={day:02d}/"
    )
    print(f"Reading raw data from: {s3_path}")

    try:
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(s3_path))
        print("Sample raw data:")
        df.show(5, truncate=False)
        df.printSchema()
        return df
    except Exception as e:
        print(f"Error reading data from S3: {str(e)}")
        return None


def process_stock_data(df):
    print("\n----- Processing Historical Stock Data")

    if df is None or df.count() == 0:
        print("No data to process")
        return None

    try:
        record_count = df.count()
        print(f"Record count: {record_count}")

        # Window over each (symbol, date) pair
        window_day = Window.partitionBy("symbol", "date")

        df = df.withColumn("daily_open",   F.first("open").over(window_day))
        df = df.withColumn("daily_high",   F.max("high").over(window_day))
        df = df.withColumn("daily_low",    F.min("low").over(window_day))
        df = df.withColumn("daily_volume", F.sum("volume").over(window_day))
        df = df.withColumn("daily_close",  F.last("close").over(window_day))
        df = df.withColumn(
            "daily_change",
            (F.col("daily_close") - F.col("daily_open")) / F.col("daily_open") * 100
        )

        print("Sample of processed data:")
        df.select(
            "symbol", "date",
            "daily_open", "daily_high", "daily_low",
            "daily_volume", "daily_close", "daily_change"
        ).show(5)

        return df

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None


def write_to_s3(df, execution_date: str):
    """
    Write processed Parquet files to MinIO under:
      processed/historical/date={execution_date}/symbol={symbol}/

    This path MUST match the prefix that load_to_snowflake.py reads:
      processed/historical/date={execution_date}
    """
    print("\n------ Writing processed data to S3")

    if df is None:
        print("No data to write")
        return

    # -------------------- BUG FIX 3 — path mismatch -------------------------
    # Was:  f"s3a://{MINIO_BUCKET}/processed/date={processed_date}"
    #        → Snowflake loader reads from "processed/historical/date=…"
    #        → Path never matches → loader always finds 0 files
    #
    # Fix:  align with what load_to_snowflake.py reads:
    #        "processed/historical/date={execution_date}"
    # -------------------------------------------------------------------------

    # ---------------------- BUG FIX 4 — UnboundLocalError ---------------------
    # Was:  if date is None: processed_date = datetime.now().strftime(…)
    #       output_path = f"…{processed_date}"   ← NameError when date was provided
    #
    # Fix:  execution_date is always passed in explicitly; no conditional needed.
    # ---------------------------------------------------------------------------
    output_path = f"s3a://{MINIO_BUCKET}/processed/historical/date={execution_date}"
    print(f"Writing processed Parquet to: {output_path}")

    try:
        (df.select(
                "symbol", "date",
                "daily_open", "daily_high", "daily_low",
                "daily_volume", "daily_close", "daily_change"
            )
            .dropDuplicates(["symbol", "date"])
            .write
            .partitionBy("symbol")
            .mode("overwrite")
            .parquet(output_path))
        print(f"Data successfully written to: {output_path}")
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")
        traceback.print_exc()


def main():
    print("\n===============================================")
    print("STARTING STOCK MARKET BATCH PROCESSOR")
    print("===============================================\n")

    # --------------  BUG FIX 5 — execution date never read from DAG ----------
    # Was:  date = None   (hardcoded — the {{ ds }} arg from the DAG was silently
    #       ignored, so Spark always processed data from 2 days ago regardless of
    #       which DAG run triggered it, making the pipeline non-idempotent)
    #
    # Fix:  read sys.argv[1] which the Airflow BashOperator passes as {{ ds }}.
    # --------------------------------------------------------------------------
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
        try:
            datetime.strptime(execution_date, "%Y-%m-%d")  # validate
        except ValueError:
            print(f"Invalid date '{execution_date}'. Expected YYYY-MM-DD.")
            sys.exit(1)
    else:
        execution_date = datetime.now().strftime("%Y-%m-%d")
        print(f"No execution date provided — defaulting to today: {execution_date}")

    print(f"Processing data for execution_date={execution_date}")

    spark = create_spark_session()

    try:
        df = read_data_from_s3(spark, execution_date)

        if df is not None:
            processed_df = process_stock_data(df)

            if processed_df is not None:
                write_to_s3(processed_df, execution_date)
                print("Batch processing complete — data written to S3")
            else:
                print("Processing step returned no data")
        else:
            print("Failed to read raw data from S3")

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        print("\nStopping Spark Session")
        spark.stop()
        print("\n===============================================")
        print("BATCH PROCESSING COMPLETE")
        print("===============================================\n")


if __name__ == "__main__":
    main()
