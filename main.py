import requests
import csv
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

base_url = "https://api.polygon.io/v3/reference/tickers"
params = {
    "market": "stocks",
    "active": "true",
    "order": "asc",
    "limit": LIMIT,
    "sort": "ticker",
    "apiKey": API_KEY,
}

# Example item to derive CSV field order (your sample)
example_ticker = {
    "ticker": "A",
    "name": "Agilent Technologies Inc.",
    "market": "stocks",
    "locale": "us",
    "primary_exchange": "XNYS",
    "type": "CS",
    "active": True,
    "currency_name": "usd",
    "cik": "0001090872",
    "composite_figi": "BBG000C2V3D6",
    "share_class_figi": "BBG001SCTQY4",
    "last_updated_utc": "2025-09-26T06:06:19.269611859Z",
}
field_names = list(example_ticker.keys())

tickers = []  # will hold dicts

# initial request
resp = requests.get(base_url, params=params)
if resp.status_code != 200:
    raise SystemExit(f"Initial request failed: {resp.status_code} {resp.text}")

data = resp.json()

# collect from first page (if present)
if "results" in data:
    tickers.extend(data["results"])

# pagination loop using next_url from response
while data.get("next_url"):
    next_url = data["next_url"]
    print("Requesting Next Page", next_url)
    # next_url already contains cursor and original query params except apiKey,
    # so append apiKey if needed (safe fallback)
    if "apiKey=" not in next_url:
        next_url = f"{next_url}&apiKey={API_KEY}"

    resp = requests.get(next_url)
    if resp.status_code != 200:
        print("Warning: next page request failed:", resp.status_code, resp.text)
        break

    data = resp.json()

    # stop if no results on this page
    if "results" not in data:
        break

    tickers.extend(data["results"])

print(f"Collected {len(tickers)} ticker dicts.")

# write CSV (stream to file)
output_csv = "tickers.csv"
with open(output_csv, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=field_names)
    writer.writeheader()
    for item in tickers:
        # item may not contain all fields; use .get to avoid KeyError
        row = {k: item.get(k, "") for k in field_names}
        writer.writerow(row)

print(f"Wrote {len(tickers)} rows to {output_csv}")

