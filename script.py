import requests
import os
import csv
from time import sleep
from dotenv import load_dotenv
load_dotenv()

MASSSIVE_API_KEY = os.getenv("MASSIVE_API_KEY")
LIMIT = 1000
SLEEP = 15

def run_stock_job():
    print("Running stock job...")
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={MASSSIVE_API_KEY}'

    response = requests.get(url)
    tickers = []

    data = response.json()
    print(data['next_url'])

    for ticker in data['results']:
        tickers.append(ticker)

    while'next_url' in data:
        print('Requesting next page, ', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={MASSSIVE_API_KEY}')
        data = response.json()
        print(data)
        for ticker in data['results']:
            tickers.append(ticker)
        sleep(SLEEP)  # Be polite and avoid hitting rate limits

    example_ticker = {'ticker': 'HE',
    'name': 'Hawaiian Electric Industries, Inc.',
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'XNYS',
    'type': 'CS',
    'active': True,
    'currency_name': 'usd',
    'cik': '0000354707',
    'composite_figi': 'BBG000BL0P40',
    'share_class_figi': 'BBG001S5RV43',
    'last_updated_utc': '2026-02-01T07:05:32.111618403Z'}

    # Write tickers to CSV using the same schema/order as example_ticker
    fieldnames = list(example_ticker.keys())

    output_path = os.path.join(os.getcwd(), 'tickers.csv')
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            # Ensure row contains all expected keys; missing keys get empty string
            row = {k: t.get(k, '') for k in fieldnames}
            writer.writerow(row)

    print(f'Wrote {len(tickers)} tickers to', output_path)

if __name__ == "__main__":
    run_stock_job()