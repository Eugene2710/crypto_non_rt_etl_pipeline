import pytest
import aiohttp
from unittest.mock import patch, MagicMock, AsyncMock

from src.binance.asynchronous.get_binance_exchange_info import BinanceExchangeInfoExtractor
from src.extractors.binance_exchange_info_extractor import BinanceExchangeInfoExtractor
from src.models.binance_models.binance_exchange_info import ExchangeInfo
"""
To test:
- Binance Exchange Info API
    - when test response!=200 
    - retries 
    Prepare: static method for BinanceExchangeInfoExtractor, mock Response, mock Session
    Act: call extract method
    Assert: assert the listed things above
    Teardown: -
- Extracted results
    - if data type is correct
    - if symbols exist
    - if symbols is of type list
    - if symbols.symbol is the same value and data type as the input param, e.g "ETHBTC", "SOLUSD"
    Prepare: static method for BinanceExchangeInfoExtractor
    Act: call extract method
    Assert: assert the listed things above
    Teardown: -
"""

# GIVEN the connection to Binance Exchange API works
class TestBinanceExchangeInfoAPI:
    @staticmethod
    @pytest.fixture
    def binance_exchange_info_extractor() -> BinanceExchangeInfoExtractor:
        return BinanceExchangeInfoExtractor()


    @pytest.mark.asyncio
    async def test_binance_exchange_info_api_responds(self, binance_exchange_info_extractor: BinanceExchangeInfoExtractor) -> None:
        """
        Purpose:
        1. To test if extractor return error response for non-200 status
        2. To test if retries were up to 5 times
        """
        # GIVEN Binance raising an aiohttp.ClientResponse == 400
        # WHEN we call Binance API
        mock_response = MagicMock(spec=aiohttp.ClientResponse)
        mock_response.status = 400
        mock_response.json = AsyncMock(
            return_value=f"Received non-status code 200: {mock_response.status}"
        )
        # this ensures that line await returns a ClientError, else it will fail the test
        # edit the path to the extractor on the patch info accordingly
        with pytest.raises(aiohttp.ClientError) as exc_info, patch(
            "src.extractors.binance_exchange_info_extractor.aiohttp.ClientSession.get"
        ) as mocked_get:
            await binance_exchange_info_extractor.extract("SOLUSD")

        # THEN we expect retry of 5 times, followed by a ClientError raised
        assert mocked_get.call_count == 5
        assert "Received non-status code 200" in str(exc_info.value)


# THEN the extracted results should be of correct data types
class BinanceExtractedResults:
    @staticmethod
    @pytest.fixture
    def binance_exchange_info_extractor() -> BinanceExchangeInfoExtractor:
        return BinanceExchangeInfoExtractor()

    @pytest.mark.asyncio
    async def test_exchange_info_api_returns_data(self, binance_exchange_info_extractor: BinanceExchangeInfoExtractor) -> None:
        """
        Purpose:
        1. To test if exchange info returns something
        2. To test if exchange info include symbols
        3. To test if exchange_info.symbols is of type list
        4. To test if exchange_info.symbols.symbol returns the same value and data type as input
        """
        # GIVEN the extractor extract results
        # WHEN the Binance Exchange Info API is working
        exchange_info: ExchangeInfo = await binance_exchange_info_extractor.extract("SOLUSD")
        # THEN the returned response should not be None, should include "symbols" in the response,
        # symbols should be of type list, symbol should be the same as the input string/param and of type str
        assert exchange_info is not None
        assert hasattr(exchange_info, "symbols")
        assert isinstance(exchange_info.symbols, list)
        # this check that returned symbol is of the right type, str, and also returns the same string
        for symbol_obj in exchange_info.symbols:
            assert symbol_obj.symbol == "SOLUSD" # edit this according to the input symbol str




