import os
import time
import logging
from typing import List, Dict

import requests
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000 # Max limit per request for this endpoint
# Optional hard sleep to prevent minute-based rate limits. Set to 0 to disable.
SLEEP_BETWEEN_PAGES_SECONDS = 5.0 

# Snowflake connection info (uses user's SNOWFLAKE_ prefix)
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "TICKERS_TABLE")

# Mapping of Polygon API keys to the user's specific Snowflake column names
API_KEY_TO_SQL_COLUMN = {
    "ticker": "ticker",
    "name": "company_name",
    "market": "market",
    "locale": "locale",
    "primary_exchange": "primary_exchange",
    "type": "Ticker_Type",
    "active": "active",
    "currency_name": "currency_name",
    "cik": "cik",
    "composite_figi": "composite_figi",
    "share_class_figi": "share_class_figi",
    "last_updated_utc": "last_updated_utc",
}

# Source structure for defining expected fields and ordering
EXAMPLE_TICKER_KEYS = list(API_KEY_TO_SQL_COLUMN.keys())


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def safe_request(url: str, params: dict = None, retries: int = 5, backoff: float = 2.0) -> dict:
    """HTTP GET with retry and rate-limit handling (429)."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=20)

            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code == 429:
                retry_after_str = resp.headers.get("Retry-After")
                retry_after = backoff * attempt
                
                try:
                    retry_after = int(retry_after_str)
                except (TypeError, ValueError):
                    pass

                logging.warning(f"Rate limited (429). Sleeping {retry_after:.1f}s before retry {attempt}/{retries}...")
                time.sleep(retry_after)
                continue

            # Handle non-200, non-429 client/server errors
            else:
                logging.warning(f"Request failed (status {resp.status_code}, {resp.text[:50]}), attempt {attempt}/{retries}. Sleeping {backoff * attempt:.1f}s.")
                time.sleep(backoff * attempt)
                continue

        except requests.exceptions.RequestException as e:
            # Handle connection errors
            logging.warning(f"Connection error on attempt {attempt}/{retries}: {e}. Sleeping {backoff * attempt:.1f}s.")
            time.sleep(backoff * attempt)
            continue
            
    # Raise failure after all attempts
    raise SystemExit(f"Request failed after {retries} retries: {url}")


def collect_tickers() -> List[Dict]:
    """Fetch all active stock tickers from Polygon with pagination."""
    if not API_KEY:
        logging.error("POLYGON_API_KEY environment variable not set.")
        return []

    base_url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "order": "asc",
        "limit": LIMIT,
        "sort": "ticker",
        "apiKey": API_KEY,
    }

    tickers = []
    
    # Initial Request
    logging.info(f"Starting initial request to {base_url}...")
    try:
        data = safe_request(base_url, params)
    except SystemExit as e:
        logging.error(f"Initial request failed: {e}")
        return []

    if "results" in data:
        tickers.extend(data["results"])

    total_pages = 1
    # Pagination loop
    while data.get("next_url"):
        next_url = data["next_url"]
        
        # Ensure apiKey is present
        url_with_key = f"{next_url}&apiKey={API_KEY}" if "apiKey=" not in next_url else next_url

        if SLEEP_BETWEEN_PAGES_SECONDS > 0:
            time.sleep(SLEEP_BETWEEN_PAGES_SECONDS)  # Hard throttle
            
        logging.info(f"Fetching page {total_pages + 1}...")
        
        try:
            data = safe_request(url_with_key)
        except SystemExit:
            logging.warning("Pagination halted due to persistent API failure.")
            break

        if "results" not in data:
            break

        tickers.extend(data["results"])
        total_pages += 1

        if total_pages % 10 == 0:
            logging.info(f"Collected {len(tickers)} tickers so far...")

    logging.info(f"✅ Collected {len(tickers)} tickers across {total_pages} pages.")
    return tickers


def write_to_snowflake(tickers: List[Dict]):
    """Insert tickers into a Snowflake table using the specific schema."""
    if not tickers:
        logging.warning("No tickers to insert.")
        return

    # Check for missing required Snowflake environment variables
    missing_vars = [name for name, val in SNOWFLAKE_CONFIG.items() if not val]
    if not SNOWFLAKE_TABLE:
         missing_vars.append("SNOWFLAKE_TABLE (or default is missing)")
    
    if missing_vars:
        logging.error(f"Required Snowflake environment variables are missing: {', '.join(missing_vars)}. Skipping insertion.")
        return

    # 1. Prepare Data for Insertion (List of Tuples with type casting)
    data_to_insert = []
    for ticker_dict in tickers:
        row = []
        for field in EXAMPLE_TICKER_KEYS:
            value = ticker_dict.get(field, None)
            
            # Special handling for CIK: Convert string to integer for Snowflake INT column
            if field == 'cik':
                try:
                    value = int(value) if value else None
                except ValueError:
                    logging.warning(f"Could not convert CIK value '{value}' to integer for ticker {ticker_dict.get('ticker')}. Inserting None.")
                    value = None

            row.append(value)
        
        data_to_insert.append(tuple(row))
        
    # 2. Connect to Snowflake
    conn = None
    try:
        logging.info(f"Connecting to Snowflake database: {SNOWFLAKE_CONFIG['database']}...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        
        full_table_name = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{SNOWFLAKE_TABLE}"
        
        # Build SQL column names from the API key list
        sql_column_names = [API_KEY_TO_SQL_COLUMN[key] for key in EXAMPLE_TICKER_KEYS]
        
        # Truncate to ensure a fresh load (as established in previous steps)
        logging.info(f"Truncating table {full_table_name} for a fresh load...")
        cur.execute(f"TRUNCATE TABLE {full_table_name}")
        
        # Prepare insert statement
        placeholders = ", ".join(["%s"] * len(sql_column_names))
        column_names_sql = ', '.join(sql_column_names)
        
        insert_sql = f"""
        INSERT INTO {full_table_name} ({column_names_sql})
        VALUES ({placeholders})
        """

        # Perform Bulk Insertion using executemany
        batch_size = 500
        rows_processed = 0
        
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cur.executemany(insert_sql, batch)
            rows_processed += len(batch)
            logging.info(f"Inserted {rows_processed} rows...")

        conn.commit()
        
        logging.info(f"✅ Successfully inserted {rows_processed} rows into Snowflake table: {full_table_name}.")

    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"Snowflake Programming Error: {e.msg}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Snowflake operation: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")


def run_stock_job():
    """Main job runner."""
    start = time.time()
    tickers = collect_tickers()
    write_to_snowflake(tickers)
    logging.info(f"🏁 Job complete in {time.time() - start:.1f}s")


if __name__ == "__main__":
    run_stock_job()


