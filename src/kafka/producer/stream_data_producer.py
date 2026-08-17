import json
import logging
import os
import time
from datetime import datetime
from typing import Optional, Dict

import requests
from confluent_kafka import Producer
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
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_REALTIME')

# AlphaVantage Config
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# AlphaVantage Free Tier:
#   25 API calls/day, 5 API calls/minute
#
# Strategy with 8 stocks:
#   - We rotate through stocks one at a time.
#   - 12 seconds between each API call respects the 5/min limit.
#   - One full cycle of 8 stocks takes ~96 seconds.
#   - With 25 calls/day and 8 stocks we get 3 full cycles/day max.
#   - We enforce a CYCLE_INTERVAL_SECONDS gap between full cycles to
#     stay safely within the 25 calls/day budget across the service uptime.
#
#   Adjust CYCLES_PER_DAY if you are on a paid AlphaVantage plan.

STOCKS = [
    'AAPL',   # Apple
    'MSFT',   # Microsoft
    'GOOGL',  # Alphabet
    'AMZN',   # Amazon
    'META',   # Meta
    'TSLA',   # Tesla
    'NVDA',   # NVIDIA
    'INTC',   # Intel
]

API_CALL_DELAY_SECONDS = 13      # safe buffer between individual calls (5/min limit)
CYCLES_PER_DAY = 3               # max full cycles on free tier (25 calls / 8 stocks ≈ 3)
SECONDS_IN_DAY = 86_400
CYCLE_INTERVAL_SECONDS = SECONDS_IN_DAY // CYCLES_PER_DAY   # ~28 800 s ≈ 8 hours


class StreamDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC):
        self.logger = logger
        self.topic = topic

        # Track previous prices to calculate change/percent_change
        self._prev_prices: Dict[str, Optional[float]] = {s: None for s in STOCKS}

        try:
            self.producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'alphavantage-stream-producer',
            })
            self.logger.info(f"Kafka producer ready → {bootstrap_servers}, topic={topic}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    # ------------------------------------------------------------------
    # AlphaVantage helpers
    # ------------------------------------------------------------------

    def fetch_quote(self, symbol: str) -> Optional[Dict]:
        """
        Call AlphaVantage GLOBAL_QUOTE to get the latest price for a symbol.
        Returns a normalised dict or None on failure.
        """
        try:
            params = {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": ALPHA_VANTAGE_API_KEY,
            }
            response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "Information" in data:
                self.logger.error(
                    f"AlphaVantage rate limit / quota hit: {data['Information']}"
                )
                return None

            if "Error Message" in data:
                self.logger.error(f"AlphaVantage error for {symbol}: {data['Error Message']}")
                return None

            quote = data.get("Global Quote", {})
            if not quote or not quote.get("05. price"):
                self.logger.warning(f"Empty quote returned for {symbol}")
                return None

            price = float(quote["05. price"])
            prev = self._prev_prices[symbol]

            # AlphaVantage gives change vs previous market close; we also track
            # intra-session change between our own polling cycles.
            av_change = float(quote.get("09. change", 0))
            av_change_pct = float(quote.get("10. change percent", "0%").replace("%", ""))

            # If we have a prior price from this session, override with live delta
            if prev is not None:
                session_change = round(price - prev, 4)
                session_change_pct = round((session_change / prev) * 100, 4) if prev else 0.0
            else:
                session_change = av_change
                session_change_pct = av_change_pct

            self._prev_prices[symbol] = price

            return {
                "symbol": symbol,
                "price": price,
                "change": session_change,
                "percent_change": session_change_pct,
                "volume": int(quote.get("06. volume", 0)),
                "latest_trading_day": quote.get("07. latest trading day", ""),
                "previous_close": float(quote.get("08. previous close", 0)),
                "timestamp": datetime.now().isoformat(),
            }

        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error fetching quote for {symbol}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching quote for {symbol}: {e}")
            return None

    # ------------------------------------------------------------------
    # Kafka helpers
    # ------------------------------------------------------------------

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed [{msg.key()}]: {err}")
        else:
            self.logger.debug(
                f"Delivered → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )

    def publish(self, stock_data: Dict):
        try:
            self.producer.produce(
                self.topic,
                key=stock_data["symbol"],
                value=json.dumps(stock_data),
                callback=self.delivery_report,
            )
            self.producer.poll(0)
        except Exception as e:
            self.logger.error(f"Failed to publish {stock_data['symbol']}: {e}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def produce_stock_data(self):
        """
        Continuously fetches real-time quotes from AlphaVantage and publishes
        them to Kafka, respecting the free-tier API limits.

        Cycle behaviour:
          • Iterate through all STOCKS, fetching one quote at a time.
          • Sleep API_CALL_DELAY_SECONDS between each call (≤5 calls/min).
          • After a full cycle, sleep until CYCLE_INTERVAL_SECONDS have elapsed
            since the cycle started (≈8 h on free tier).
        """
        self.logger.info("Starting AlphaVantage stream producer")
        self.logger.info(
            f"Config → {len(STOCKS)} stocks | "
            f"{API_CALL_DELAY_SECONDS}s between calls | "
            f"~{CYCLE_INTERVAL_SECONDS // 3600}h between cycles"
        )

        cycle_number = 0
        try:
            while True:
                cycle_number += 1
                cycle_start = time.time()
                self.logger.info(f"=== Starting cycle {cycle_number} ===")

                ok_count = 0
                fail_count = 0

                for i, symbol in enumerate(STOCKS):
                    stock_data = self.fetch_quote(symbol)
                    if stock_data:
                        self.publish(stock_data)
                        ok_count += 1
                        self.logger.info(
                            f"[{symbol}] price={stock_data['price']} "
                            f"change={stock_data['change']:+.4f} "
                            f"({stock_data['percent_change']:+.4f}%)"
                        )
                    else:
                        fail_count += 1

                    # Delay between API calls (except after the last stock in a cycle)
                    if i < len(STOCKS) - 1:
                        self.logger.debug(
                            f"Waiting {API_CALL_DELAY_SECONDS}s before next API call…"
                        )
                        time.sleep(API_CALL_DELAY_SECONDS)

                # Flush any buffered Kafka messages
                self.producer.flush()
                self.logger.info(
                    f"Cycle {cycle_number} complete — OK: {ok_count}, Failed: {fail_count}"
                )

                # Wait for the remainder of the cycle interval before starting the next cycle
                elapsed = time.time() - cycle_start
                wait = max(0, CYCLE_INTERVAL_SECONDS - elapsed)
                self.logger.info(
                    f"Sleeping {wait / 3600:.2f}h until next cycle "
                    f"(free-tier budget management)…"
                )
                time.sleep(wait)

        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user (KeyboardInterrupt)")
        except Exception as e:
            self.logger.error(f"Unexpected error in produce loop: {e}")
        finally:
            self.logger.info("Flushing producer and shutting down…")
            self.producer.flush()


def main():
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY is not set in environment variables!")
        raise EnvironmentError("Missing ALPHA_VANTAGE_API_KEY")

    if not KAFKA_TOPIC:
        logger.error("KAFKA_TOPIC_REALTIME is not set in environment variables!")
        raise EnvironmentError("Missing KAFKA_TOPIC_REALTIME")

    producer = StreamDataCollector(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
    )
    producer.produce_stock_data()


if __name__ == "__main__":
    main()
