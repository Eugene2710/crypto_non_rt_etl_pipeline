import asyncio
import aiohttp
from typing import Any
from asyncio import AbstractEventLoop
from datetime import datetime
from tenacity import retry, wait_fixed, stop_after_attempt
from src.utils.logging_utils import setup_logging
import logging

from src.models.binance_models.binance_klines import Klines

logger: logging.Logger = logging.getLogger(__name__)
setup_logging(logger)


class BinanceKlinesExtractor:
    """
    extract the klines info from Binance into S3

    Responsible for
    - extracting the klines info given a symbol
    - inserting the latest klines details into a s3 table

    API docs: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
    """

    @staticmethod
    @retry(
        wait=wait_fixed(0.01),  # ~10ms before attempts
        stop=stop_after_attempt(5),  # equivalent to 5 retries
        reraise=True,  # re-raise the last exception if all attempts fail
    )
    async def extract(
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> Klines:
        url: str = "https://api.binance.com/api/v3/klines"

        start_timestamp = int(start_time.timestamp() * 1000) if start_time else None
        end_timestamp = int(end_time.timestamp() * 1000) if end_time else None

        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
        }

        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(url, params=params) as response:
                    if response.status == 200:
                        # happy path
                        data: list[list[int | str]] = await response.json()
                        k_lines: Klines = Klines.from_json(symbol=symbol, raw_data=data)
                        return k_lines
                    else:
                        raise aiohttp.ClientError(
                            f"Received non-status code 200: {response.status}"
                        )

        except aiohttp.ClientError as e:
            logger.error(e)
            raise e


if __name__ == "__main__":
    event_loop: AbstractEventLoop = asyncio.new_event_loop()
    symbol: str = "ETHBTC"
    interval: str = "1m"
    limit: int = 500
    start_time: datetime = datetime(2025, 2, 10)
    end_time: datetime = datetime(2025, 2, 11)
    response: Klines = event_loop.run_until_complete(
        BinanceKlinesExtractor.extract(
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
    )
    # pprint(response)
    with open("klines.json", "w", encoding="utf-8") as json_file:
        json_str = response.model_dump_json(indent=2)
        json_file.write(json_str)
