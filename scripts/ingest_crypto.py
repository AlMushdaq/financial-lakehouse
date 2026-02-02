import os
import json
import logging
import requests
import snowflake.connector
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# --- 1. LOAD CONFIGURATION ---
load_dotenv()  # Load variables from .env file

# Snowflake Config
KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
USER = os.getenv("SNOWFLAKE_USER")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TABLE = os.getenv("SNOWFLAKE_TABLE")

# API Config
API_KEY = os.getenv("COINGECKO_API_KEY")
API_URL = os.getenv("COINGECKO_API_URL")
API_LIMIT = os.getenv("COINGECKO_API_LIMIT", "10")

# --- 2. LOGGING SETUP ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_private_key():
    """Reads the private key file for Snowflake authentication."""
    if not KEY_PATH:
        raise ValueError("Missing SNOWFLAKE_PRIVATE_KEY_PATH in .env")

    with open(KEY_PATH, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(), password=None, backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pkb


def get_snowflake_conn():
    """Establishes connection to Snowflake."""
    ctx = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        private_key=get_private_key(),
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )
    return ctx


# --- 3. DATA INGESTION ---
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_crypto_data():
    """Fetches data from CoinGecko using the API Key."""

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": API_LIMIT,
        "page": 1,
        "sparkline": "false",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "accept": "application/json",
        "x-cg-demo-api-key": API_KEY,
    }

    logger.info(f"Fetching top {API_LIMIT} coins from CoinGecko...")
    response = requests.get(API_URL, params=params, headers=headers, timeout=10)
    response.raise_for_status()

    data = response.json()

    if not data:
        raise ValueError("API returned 0 records! Pipeline halted.")

    logger.info(f"Successfully fetched {len(data)} records.")
    return data


def load_to_snowflake(data):
    """Loads JSON data into Snowflake RAW table."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()

    try:
        # 1. Create Table (if not exists)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                record_id VARCHAR DEFAULT UUID_STRING(),
                ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                json_data VARIANT
            )
        """)

        # 2. Insert Data
        logger.info(f"Inserting {len(data)} records into Snowflake...")
        insert_query = f"""
        INSERT INTO {SCHEMA}.{TABLE} (json_data) 
        SELECT PARSE_JSON($1) FROM VALUES (%s)
        """

        records_to_insert = [(json.dumps(record),) for record in data]

        cursor.executemany(insert_query, records_to_insert)
        conn.commit()
        logger.info("Load complete!")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    try:
        market_data = fetch_crypto_data()
        load_to_snowflake(market_data)
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        exit(1)
