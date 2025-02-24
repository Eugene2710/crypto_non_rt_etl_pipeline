import asyncio
import aiohttp
from tenacity import retry, wait_fixed, stop_after_attempt
from typing import Any
from asyncio import AbstractEventLoop
import logging
from src.utils.logging_utils import setup_logging

from src.models.binance_models.binance_exchange_info import ExchangeInfo

logger: logging.Logger = logging.getLogger(__name__)
setup_logging(logger)


class BinanceExchangeInfoExtractor:
    """
    extract the exchange info from Binance into S3

    Responsible for
    - extracting the exchange info given a symbol,
    - inserting the latest symbol details into S3 table

    Ideally, the BinanceExchangeInfoExtractor does not have to query the symbols bc it is unlikely to have a change soon
    """

    @staticmethod
    @retry(
        wait=wait_fixed(0.01),  # ~10ms between attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries/attempts
        reraise=True,  # re-raise the last exception if all attempts fail
    )
    async def extract(symbol: str) -> ExchangeInfo:
        """
        given an input symbol, extract the exchange info regarding the symbol
        current extractor supports extraction of 1 symbol, multiple symbols can be extracted by tweaking the url string
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints
        """
        url: str = f"https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}"
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(url) as response:
                    if response.status == 200:
                        # happy path
                        data: dict[str, Any] = await response.json()
                        exchange_info: ExchangeInfo = ExchangeInfo.model_validate(data)
                        return exchange_info
                    else:
                        raise aiohttp.ClientError(
                            f"Received non-status code 200: {response.status}"
                        )
        except aiohttp.ClientError as e:
            # catch any http.ClientError which includes cases when response status == 200,
            # e.g JSONDecodeError wrapped as clientError
            logger.error(e)
            raise e


if __name__ == "__main__":
    event_loop: AbstractEventLoop = asyncio.new_event_loop()
    symbol: str = "ETHBTC"
    response: ExchangeInfo = event_loop.run_until_complete(
        BinanceExchangeInfoExtractor.extract(symbol=symbol)
    )
    # pprint(response)
    with open("exchange_info.json", "w", encoding="utf-8") as json_file:
        json_str = response.model_dump_json(indent=2)
        json_file.write(json_str)
