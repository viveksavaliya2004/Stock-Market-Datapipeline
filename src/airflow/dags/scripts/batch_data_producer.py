import json
import logging
import os
import sys
import time
from datetime import datetime

import pandas as pd
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from typing import Optional

# Load Env Variables
load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_BATCH = os.getenv('KAFKA_TOPIC_BATCH')

# AlphaVantage Config
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# AlphaVantage Free Tier limits:
#   - 25 API calls/day
#   - 5 API calls/minute
# With 10 stocks: 10 calls total, spaced 12s apart to stay under 5/min
API_CALL_DELAY_SECONDS = 12  # safe buffer for 5 calls/minute limit

# Define stocks to collect for historical data
STOCKS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V"
]


class HistoricalDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC_BATCH):
        self.logger = logger
        self.topic = topic

        try:
            self.producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                "client.id": "historical-data-collector-0",
            })
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def fetch_historical_data(self, symbol: str, execution_date: str) -> Optional[pd.DataFrame]:
        """
        Fetch daily historical stock data from AlphaVantage TIME_SERIES_DAILY.
        Uses 'compact' outputsize (last 100 trading days) to minimize API usage.
        Filters to the specific execution_date passed by the Airflow DAG.
        """
        try:
            self.logger.info(f"Fetching historical data for {symbol} (execution_date={execution_date})")

            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": "compact",   # last 100 trading days — saves API quota
                "datatype": "json",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }

            response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # AlphaVantage returns an 'Information' key when rate-limited or quota exceeded
            if "Information" in data:
                self.logger.error(f"AlphaVantage API limit hit: {data['Information']}")
                return None

            if "Error Message" in data:
                self.logger.error(f"AlphaVantage error for {symbol}: {data['Error Message']}")
                return None

            time_series = data.get("Time Series (Daily)", {})
            if not time_series:
                self.logger.warning(f"No time series data returned for {symbol}")
                return None

            records = []
            for date_str, values in time_series.items():
                records.append({
                    "date": date_str,
                    "symbol": symbol,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                })

            df = pd.DataFrame(records)
            df = df.sort_values("date").reset_index(drop=True)

            # Filter to just the execution date so batch is idempotent per DAG run
            df_filtered = df[df["date"] == execution_date]
            if df_filtered.empty:
                self.logger.warning(
                    f"No data for {symbol} on {execution_date}. "
                    f"Available range: {df['date'].min()} → {df['date'].max()}"
                )
                # Fall back to most recent available date (e.g. weekends/holidays)
                df_filtered = df.tail(1)
                self.logger.info(f"Using most recent available date: {df_filtered['date'].values[0]}")

            self.logger.info(f"Fetched {len(df_filtered)} row(s) for {symbol}")
            return df_filtered

        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error fetching data for {symbol}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return None

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg.key()}: {err}")
        else:
            self.logger.info(
                f"Message delivered to topic '{msg.topic()}' "
                f"partition [{msg.partition()}] offset {msg.offset()}"
            )

    def produce_to_kafka(self, df: pd.DataFrame, symbol: str, execution_date: str):
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        df = df.copy()
        df['batch_id'] = batch_id
        df['batch_date'] = execution_date  # use the DAG execution date as the batch date

        records = df.to_dict(orient="records")
        successful_records = 0
        failed_records = 0

        for record in records:
            try:
                data = json.dumps(record, default=str)
                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=data,
                    callback=self.delivery_report,
                )
                self.producer.poll(0)
                successful_records += 1
            except Exception as e:
                self.logger.error(f"Failed to produce message for {symbol}: {e}")
                failed_records += 1

        self.producer.flush()
        self.logger.info(
            f"Produced for {symbol}: {successful_records} OK, {failed_records} FAILED"
        )

    def collect_historical_data(self, execution_date: str):
        self.logger.info(
            f"Starting historical data collection for {len(STOCKS)} symbols | "
            f"execution_date={execution_date}"
        )
        successful_symbols = 0
        failed_symbols = 0

        for i, symbol in enumerate(STOCKS):
            try:
                df = self.fetch_historical_data(symbol, execution_date)

                if df is not None and not df.empty:
                    self.produce_to_kafka(df, symbol, execution_date)
                    successful_symbols += 1
                else:
                    self.logger.warning(f"No data to produce for {symbol}")
                    failed_symbols += 1

            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                failed_symbols += 1

            # Respect AlphaVantage rate limit: 5 calls/min → wait 12s between calls
            # Skip delay after the last stock
            if i < len(STOCKS) - 1:
                self.logger.info(
                    f"Rate-limit pause: waiting {API_CALL_DELAY_SECONDS}s before next API call…"
                )
                time.sleep(API_CALL_DELAY_SECONDS)

        self.logger.info(
            f"Collection complete — Successful: {successful_symbols}, Failed: {failed_symbols}"
        )


def main():
    # The Airflow DAG passes {{ ds }} (YYYY-MM-DD) as the first argument
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
        try:
            datetime.strptime(execution_date, "%Y-%m-%d")  # validate format
        except ValueError:
            logger.error(f"Invalid date format '{execution_date}'. Expected YYYY-MM-DD.")
            sys.exit(1)
    else:
        execution_date = datetime.now().strftime("%Y-%m-%d")
        logger.warning(f"No execution date provided — defaulting to today: {execution_date}")

    logger.info(f"Starting Historical Stock Data Collector | execution_date={execution_date}")

    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY is not set in environment variables!")
        sys.exit(1)

    try:
        collector = HistoricalDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_BATCH,
        )
        collector.collect_historical_data(execution_date=execution_date)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
