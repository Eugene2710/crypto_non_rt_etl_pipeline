from datetime import datetime
import requests
from pprint import pprint


def fetch_spot_klines(symbol, interval, limit=500, start_time=None, end_time=None):
    """
    Fetch candlestick price data for given symbol
    https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
    """
    url = "https://api.binance.com/api/v3/klines"

    start_timestamp = int(start_time.timestamp() * 1000) if start_time else None
    end_timestamp = int(end_time.timestamp() * 1000) if end_time else None

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "startTime": start_timestamp,
        "endTime": end_timestamp,
    }

    response = requests.get(url=url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None


data = fetch_spot_klines(symbol="ETHBTC", interval="1s", limit=500, start_time=datetime(2025, 2, 10), end_time=datetime(2025, 2, 11))
pprint(data)