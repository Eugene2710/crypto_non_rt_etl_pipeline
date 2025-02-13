import requests
from pprint import pprint


def get_binance_spot_tickers():
    """
    https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints
    """
    url: str = "https://api.binance.com/api/v3/exchangeInfo"
    response= requests.get(url)

    if response.status_code == 200:
        data = response.json()
        """
        possible symbol status: PRE_TRADING, TRADING, POST_TRADING, END_OF_DAY, HALT, AUCTION_MATCH, BREAK
        https://developers.binance.com/docs/binance-spot-api-docs/enums#account-and-symbol-permissions
        """
        tickers = [symbol['symbol'] for symbol in data['symbols'] if symbol['status'] == "TRADING"]
        return tickers
    else:
        print(f"Error: {response.status_code}")
        return []


tickers = get_binance_spot_tickers()
print(f"Available trading pairs:")
pprint(tickers)