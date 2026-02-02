import os
import json
import logging
import requests
import snowflake.connector
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# --- 1. CONFIGURATION ---
load_dotenv()

# Common Config
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD") # New!
KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FINANCE_LAKE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
TABLE = os.getenv("SNOWFLAKE_TABLE", "MARKET_DATA")

# API Config
API_KEY = os.getenv("COINGECKO_API_KEY")
API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3/coins/markets")
API_LIMIT = os.getenv("COINGECKO_API_LIMIT", "10")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_conn():
    """
    Connects to Snowflake using Key Pair (Local) OR Password (GitHub).
    """
    # 1. Try Key Pair Authentication first (Best for Local)
    if KEY_PATH and os.path.exists(KEY_PATH):
        logger.info("Authentication: Using Private Key (Local Mode)")
        with open(KEY_PATH, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=None,
                backend=default_backend()
            )
        
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return snowflake.connector.connect(
            user=USER,
            account=ACCOUNT,
            private_key=pkb,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
    
    # 2. Fallback to Password Authentication (Best for GitHub Actions)
    elif PASSWORD:
        logger.info("Authentication: Using Password (Cloud Mode)")
        return snowflake.connector.connect(
            user=USER,
            account=ACCOUNT,
            password=PASSWORD,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
    
    else:
        raise ValueError("No valid authentication found! Check .env or GitHub Secrets.")

# --- 3. DATA INGESTION ---
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_crypto_data():
    """Fetches data from CoinGecko."""
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": API_LIMIT,
        "page": 1,
        "sparkline": "false"
    }
    
    headers = {"User-Agent": "Mozilla/5.0"}
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY

    logger.info(f"Fetching top {API_LIMIT} coins from CoinGecko...")
    response = requests.get(API_URL, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    
    data = response.json()
    if not data:
        raise ValueError("API returned 0 records!")
        
    return data

def load_to_snowflake(data):
    """Loads JSON data into Snowflake."""
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
                record_id VARCHAR DEFAULT UUID_STRING(),
                ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                json_data VARIANT
            )
        """)
        
        logger.info(f"Inserting {len(data)} records...")
        
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
