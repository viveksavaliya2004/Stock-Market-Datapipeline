"""
load_to_snowflake.py
--------------------
Reads processed Parquet files for a given execution_date from MinIO and
performs an incremental (MERGE / upsert) load into Snowflake.

Called by the Airflow DAG as:
    python load_to_snowflake.py {{ ds }}

Data flow expected:
    MinIO:  processed/historical/date={execution_date}/symbol={SYM}/part-*.parquet
    Target: STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS  (symbol + date as PK)
"""

import io
import logging
import os
import sys
import traceback
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── MinIO / S3 config — read from env, never hardcode credentials ─────────────
# FIX 5: credentials were hardcoded as plain strings in the original.
# They are now read from environment variables so secrets stay out of source.
S3_ENDPOINT   = os.getenv("MINIO_CONNECTION", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
S3_BUCKET     = os.getenv("MINIO_BUCKET", "stock-market-data")

# ── Snowflake config — read from env ──────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "STOCKMARKETBATCH")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",   "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_TABLE     = os.getenv("SNOWFLAKE_TABLE",    "DAILY_STOCK_METRICS")

# Columns that must be present in the processed Parquet before loading
REQUIRED_COLUMNS = [
    "symbol", "date",
    "daily_open", "daily_high", "daily_low",
    "daily_volume", "daily_close", "daily_change",
    "last_updated",
]


# ── S3 / MinIO ────────────────────────────────────────────────────────────────

def init_s3_client():
    try:
        client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
        logger.info(f"S3 client initialised -> {S3_ENDPOINT}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialise S3 client: {e}")
        raise


def read_processed_data(s3_client, execution_date: str) -> pd.DataFrame | None:
    """
    List all Parquet files under processed/historical/date={execution_date}/
    and concatenate them into a single DataFrame.

    Spark writes with .partitionBy("symbol"), which means:
      - The physical file lives under  .../symbol=AAPL/part-00000.parquet
      - The 'symbol' column is NOT stored inside the file — it is encoded
        in the directory name only.
    We extract 'symbol' from the S3 key and inject it as a column.
    """
    s3_prefix = f"processed/historical/date={execution_date}/"
    logger.info(f"Listing objects under s3://{S3_BUCKET}/{s3_prefix}")

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
    except Exception as e:
        logger.error(f"Failed to list S3 objects: {e}")
        return None

    if "Contents" not in response:
        logger.warning(f"No objects found under '{s3_prefix}' — nothing to load")
        return None

    dfs    = []
    skipped = 0

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue

        # Extract symbol from the Hive-style partition path segment
        symbol = None
        for segment in key.split("/"):
            if segment.startswith("symbol="):
                symbol = segment.split("=", 1)[-1]
                break

        if symbol is None:
            # FIX 2: original silently appended a df without a symbol column,
            # only failing later at the required_columns check with no indication
            # of which file caused the problem. Now we skip with a clear error.
            logger.error(
                f"Could not parse symbol from key '{key}' — skipping. "
                f"Verify Spark wrote with .partitionBy('symbol')."
            )
            skipped += 1
            continue

        try:
            obj_response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            raw_bytes    = obj_response["Body"].read()
            df           = pd.read_parquet(io.BytesIO(raw_bytes))

            # Spark partition column is not inside the file — inject it here
            df["symbol"] = symbol
            dfs.append(df)
            logger.debug(f"Read {len(df)} row(s) from {key}")

        except Exception as e:
            logger.error(f"Failed to read '{key}': {e}")
            skipped += 1

    if not dfs:
        logger.warning("No parquet files could be read successfully")
        return None

    if skipped:
        logger.warning(f"{skipped} file(s) skipped — check logs above")

    df_all = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(df_all)} total row(s) from {len(dfs)} file(s)")

    # ── Type coercion ──────────────────────────────────────────────────────
    df_all["date"]         = pd.to_datetime(df_all["date"]).dt.date
    df_all["last_updated"] = datetime.now()

    # De-duplicate: keep the last occurrence per (symbol, date)
    df_all = df_all.drop_duplicates(subset=["symbol", "date"], keep="last")

    # ── Column validation ──────────────────────────────────────────────────
    missing = [c for c in REQUIRED_COLUMNS if c not in df_all.columns]
    if missing:
        logger.error(f"Missing required columns: {missing} — aborting load")
        return None

    return df_all[REQUIRED_COLUMNS].reset_index(drop=True)


# ── Snowflake ─────────────────────────────────────────────────────────────────

def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        logger.info("Snowflake connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def create_snowflake_table(conn):
    """Idempotent DDL — safe to call on every DAG execution."""
    ddl = f"""
        CREATE TABLE IF NOT EXISTS
            {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
                symbol       STRING    NOT NULL,
                date         DATE      NOT NULL,
                daily_open   FLOAT,
                daily_high   FLOAT,
                daily_low    FLOAT,
                daily_volume FLOAT,
                daily_close  FLOAT,
                daily_change FLOAT,
                last_updated TIMESTAMP,
                -- NOTE: Snowflake PRIMARY KEY is metadata only — not enforced.
                -- Uniqueness is guaranteed by the MERGE upsert logic below.
                PRIMARY KEY (symbol, date)
            )
    """
    cursor = conn.cursor()
    try:
        cursor.execute(ddl)
        # FIX 9: DDL in Snowflake is auto-committed — conn.commit() is a no-op
        # here and was removed to avoid implying it does something meaningful.
        logger.info(f"Table '{SNOWFLAKE_TABLE}' is ready")
    except Exception as e:
        logger.error(f"Failed to create/verify Snowflake table: {e}")
        raise
    finally:
        cursor.close()


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert numpy scalar types to Python-native equivalents that the Snowflake
    connector serialises cleanly. Works on a copy so caller's df is unchanged.
    NaN floats become None so Snowflake receives SQL NULL (not the string 'nan').
    """
    out = df.copy()

    for col in out.select_dtypes(include=[np.floating]).columns:
        # Replace NaN with None (-> SQL NULL); cast to object so None survives
        out[col] = out[col].where(out[col].notna(), other=None).astype(object)

    for col in out.select_dtypes(include=[np.integer]).columns:
        out[col] = out[col].astype(object)

    return out


def incremental_load_to_snowflake(conn, df: pd.DataFrame):
    """
    Upsert strategy — three steps:
      1. CREATE TEMPORARY TABLE (same schema as target)
      2. Bulk-load df into staging via write_pandas  (PUT + COPY, not row-by-row)
      3. MERGE from staging into target

    FIX 4: the original used cursor.executemany() row-by-row — extremely slow
    for any real volume. write_pandas uses Snowflake's PUT/COPY path internally
    and is the officially recommended approach for bulk DataFrame loads.

    FIX 3: added explicit rollback on any failure — Snowflake DML is
    transactional and leaves an open transaction if we don't roll back.
    """
    if df is None or df.empty:
        logger.info("No data to load — skipping")
        return

    logger.info(f"Starting incremental load: {len(df)} row(s) -> {SNOWFLAKE_TABLE}")
    stage_table = "TEMP_DAILY_STOCK_STAGE"
    cursor = conn.cursor()

    try:
        # Step 1 — staging table
        cursor.execute(
            f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} "
            f"LIKE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
        )
        logger.info(f"Staging table '{stage_table}' created")

        # Step 2 — bulk load via write_pandas
        # Snowflake stores column names as UPPERCASE by default; match that.
        upload_df = _coerce_types(df)
        upload_df.columns = [c.upper() for c in upload_df.columns]

        success, n_chunks, n_rows, output = write_pandas(
            conn=conn,
            df=upload_df,
            table_name=stage_table,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            auto_create_table=False,
            overwrite=False,
        )

        if not success:
            raise RuntimeError(
                f"write_pandas failed loading into '{stage_table}': {output}"
            )
        logger.info(f"write_pandas: {n_rows} row(s) loaded in {n_chunks} chunk(s)")

        # Step 3 — MERGE
        merge_sql = f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS tgt
            USING {stage_table} AS src
                ON  tgt.SYMBOL = src.SYMBOL
                AND tgt.DATE   = src.DATE
            WHEN MATCHED THEN UPDATE SET
                tgt.DAILY_OPEN   = src.DAILY_OPEN,
                tgt.DAILY_HIGH   = src.DAILY_HIGH,
                tgt.DAILY_LOW    = src.DAILY_LOW,
                tgt.DAILY_VOLUME = src.DAILY_VOLUME,
                tgt.DAILY_CLOSE  = src.DAILY_CLOSE,
                tgt.DAILY_CHANGE = src.DAILY_CHANGE,
                tgt.LAST_UPDATED = src.LAST_UPDATED
            WHEN NOT MATCHED THEN INSERT (
                SYMBOL, DATE,
                DAILY_OPEN, DAILY_HIGH, DAILY_LOW,
                DAILY_VOLUME, DAILY_CLOSE, DAILY_CHANGE,
                LAST_UPDATED
            ) VALUES (
                src.SYMBOL, src.DATE,
                src.DAILY_OPEN, src.DAILY_HIGH, src.DAILY_LOW,
                src.DAILY_VOLUME, src.DAILY_CLOSE, src.DAILY_CHANGE,
                src.LAST_UPDATED
            )
        """
        cursor.execute(merge_sql)
        conn.commit()
        logger.info("MERGE committed — incremental load complete")

    except Exception as e:
        logger.error(f"Snowflake load failed: {e}")
        logger.error(traceback.format_exc())
        try:
            conn.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rb_err:
            logger.error(f"Rollback also failed: {rb_err}")
        raise

    finally:
        cursor.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def _validate_env():
    """Fail fast if any required secret is missing rather than getting a
    cryptic auth error mid-run."""
    required = {
        "SNOWFLAKE_ACCOUNT":  SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER":     SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "MINIO_ACCESS_KEY":   S3_ACCESS_KEY,
        "MINIO_SECRET_KEY":   S3_SECRET_KEY,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        sys.exit(1)


def main():
    logger.info("=" * 55)
    logger.info("  STARTING SNOWFLAKE INCREMENTAL LOAD")
    logger.info("=" * 55)

    _validate_env()

    # Accept execution date from Airflow {{ ds }} or default to today
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
        try:
            datetime.strptime(execution_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date '{execution_date}'. Expected YYYY-MM-DD.")
            sys.exit(1)
    else:
        execution_date = datetime.now().strftime("%Y-%m-%d")
        logger.warning(f"No execution_date supplied — defaulting to today: {execution_date}")

    logger.info(f"execution_date = {execution_date}")

    s3_client = init_s3_client()
    conn      = init_snowflake_connection()

    try:
        create_snowflake_table(conn)
        df = read_processed_data(s3_client, execution_date)

        if df is not None:
            incremental_load_to_snowflake(conn, df)
        else:
            logger.info("No processed data found — Snowflake table unchanged")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

    finally:
        conn.close()
        logger.info("Snowflake connection closed")

    logger.info("=" * 55)
    logger.info("  LOAD COMPLETE")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
