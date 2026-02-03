import requests
import os
from time import sleep
from dotenv import load_dotenv
from datetime import datetime
import snowflake.connector
load_dotenv()

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
LIMIT = 1000
SLEEP = 15
DS = '2026-02-03'

def run_stock_job():
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\tStarting stock job")
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={MASSIVE_API_KEY}'

    response = requests.get(url)
    tickers = []

    data = response.json()
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tFirst URL: {data["next_url"]}')

    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    while'next_url' in data:
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tRequesting next page, {data["next_url"]}')
        response = requests.get(data['next_url'] + f'&apiKey={MASSIVE_API_KEY}')
        data = response.json()
        # print(data)
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tSleeping for {SLEEP} seconds...')
        sleep(SLEEP)  # Be polite and avoid hitting rate limits

    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tFetched {len(tickers)} tickers from Massive API')

    # Connect to Snowflake
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\tConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_tickers (
        ticker VARCHAR(10),
        name VARCHAR(255),
        market VARCHAR(50),
        locale VARCHAR(10),
        primary_exchange VARCHAR(10),
        type VARCHAR(10),
        active BOOLEAN,
        currency_name VARCHAR(50),
        cik VARCHAR(20),
        composite_figi VARCHAR(20),
        share_class_figi VARCHAR(20),
        last_updated_utc TIMESTAMP_NTZ,
        ds VARCHAR
    )
    """
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\tCreating stock_tickers table if not exists...")
    cursor.execute(create_table_sql)

    # Insert tickers into Snowflake using batch insert
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\tInserting tickers into stock_tickers table...")
    
    # Prepare data for batch insert
    data_rows = []
    for t in tickers:
        data_rows.append([
            t.get('ticker', ''),
            t.get('name', ''),
            t.get('market', ''),
            t.get('locale', ''),
            t.get('primary_exchange', ''),
            t.get('type', ''),
            t.get('active', False),
            t.get('currency_name', ''),
            t.get('cik', ''),
            t.get('composite_figi', ''),
            t.get('share_class_figi', ''),
            t.get('last_updated_utc', ''),
            t.get('ds', None)
        ])
    
    # Insert all rows at once
    insert_sql = """
    INSERT INTO stock_tickers 
    (ticker, name, market, locale, primary_exchange, type, active, currency_name, cik, composite_figi, share_class_figi, last_updated_utc, ds)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_sql, data_rows)
    print(f"Inserted {len(data_rows)} rows")
    
    conn.commit()
    cursor.close()
    conn.close()

    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\tWrote {len(tickers)} tickers to Snowflake stock_tickers table')

if __name__ == "__main__":
    run_stock_job()