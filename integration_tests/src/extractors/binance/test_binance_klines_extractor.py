import pytest
import aiohttp
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

from src.extractors.binance_klines_extractor import BinanceKlinesExtractor
from src.models.binance_models.binance_klines import Klines

"""
To test:
- Binance Kline API
    - when test response != 200
    - retries
    Prepare: static method for BinanceKlinesExtractor, mock Response, mock Session
    Act: call extract method
    Assert: assert the listed things above
    Teardown: -
- Extracted results
    - if data types are correct
    - if returned value is of type list
    Prepare: static method for BinanceKlinesExtractor
    Act: call extract mthod
    Assert: assert the listed things above
    Teardown: -
"""


class TestBinanceKlinesAPI:
    @staticmethod
    @pytest.fixture
    def binance_klines_extractor() -> BinanceKlinesExtractor:
        return BinanceKlinesExtractor()

    @pytest.mark.asyncio
    async def test_binance_klines_api_responds(
        self, binance_klines_extractor: BinanceKlinesExtractor
    ) -> None:
        """
        Purpose:
        1. To test if extractor return error response for non-200 status
        2. To test if retries were up to 5 times
        """
        # GIVEN Binance raised an aiohttp.ClientResponse == 400 status
        # WHEN we call Binance Klines API
        mock_response = MagicMock(spec=aiohttp.ClientResponse)
        mock_response.status = 400
        mock_response.json = AsyncMock(
            return_value=f"Received non-status code 200: {mock_response.status}"
        )
        # this ensures that line await returns a ClientError, else it will fail the rest
        with pytest.raises(aiohttp.ClientError) as exc_info, patch(
            "src.extractors.binance_klines_extractor.aiohttp.ClientSession.get"
        ) as mocked_get:
            await binance_klines_extractor.extract(
                symbol="ETHBTC",
                interval="1s",
                limit=500,
                start_time=datetime(2025, 2, 10),
                end_time=datetime(2025, 2, 11),
            )

        # THEN we expect retry of 5 times, followed by a ClientError raised
        assert mocked_get.call_count == 5
        assert "Received non-status code 200" in str(exc_info.value)


class TestBinanceKlinesExtractedResults:
    @staticmethod
    @pytest.fixture
    def binance_klines_extractor() -> BinanceKlinesExtractor:
        return BinanceKlinesExtractor()

    @pytest.mark.asyncio
    async def test_binance_kline_api_returns_data(
        self, binance_klines_extractor: BinanceKlinesExtractor
    ) -> None:
        """
        Purpose
        1. To test if the extracted results are of the right data type
        2. To test if the extracted results return the specified symbol
        3. To test if Klines is of type list
        """
        # GIVEN the extractor extracts results
        # WHEN the Klines API is working
        symbol: str = "ETHBTC"
        interval: str = "1m"
        limit: int = 500
        start_time: datetime = datetime(2025, 2, 10)
        end_time: datetime = datetime(2025, 2, 11)
        klines: Klines = await binance_klines_extractor.extract(
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
        )
        # THEN the response should not be none, and has the same returned symbol as the input symbol
        assert klines is not None
        assert klines.klines[0].symbol == "ETHBTC"
