import asyncio
from typing import Any
import aiohttp
from pprint import pprint

from src.models.binance_models.binance_exchange_info import ExchangeInfo
from src.models.database_transfer_objects.binance.binance_exchange_tickers import (
    BinanceTickerMetadataDTO,
)


class BinanceExchangeInfoExtractor:
    @staticmethod
    async def extract() -> ExchangeInfo:
        """
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints
        """
        url: str = "https://api.binance.com/api/v3/exchangeInfo"
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as response:
                if response.status == 200:
                    data: dict[str, Any] = await response.json()
                    """
                    possible symbol status: PRE_TRADING, TRADING, POST_TRADING, END_OF_DAY, HALT, AUCTION_MATCH, BREAK
                    https://developers.binance.com/docs/binance-spot-api-docs/enums#account-and-symbol-permissions
                    """
                    exchange_info: ExchangeInfo = ExchangeInfo.model_validate(data)
                    return exchange_info

                else:
                    raise Exception(f"Received non-status code 200: {response.status}")


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    # result: ExchangeInfo = event_loop.run_until_complete(get_binance_exchange_info())
    # pprint(result)
    try:
        # Use run_until_complete to execute the async function synchronously.
        exchange_info: ExchangeInfo = event_loop.run_until_complete(
            get_binance_exchange_info()
        )
        # Convert the ExchangeInfo data into DTOs.
        dtos = BinanceTickerMetadataDTO.from_exchange_info(exchange_info)

        print("Extracted Binance Ticker Metadata DTOs:")
        for dto in dtos:
            pprint(dto.model_dump())
    finally:
        event_loop.close()
