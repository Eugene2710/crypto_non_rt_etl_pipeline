import pytest
from src.models.binance_models.binance_klines import Klines, Kline
from src.models.database_transfer_objects.binance.binance_klines import BinanceKlinePriceDTO
from datetime import datetime, timezone
from decimal import Decimal

"""
sample Kline object
class Kline(BaseModel):
    open_time: int  # e.g. 1499040000000
    open_price: str  # e.g. "0.01634790"
    high_price: str  # e.g. "0.80000000"
    low_price: str  # e.g. "0.01575800"
    close_price: str  # e.g. "0.01577100"
    volume: str  # e.g. "148976.11427815"
    close_time: int  # e.g. 1499644799999
    quote_asset_volume: str  # e.g. "2434.19055334"
    number_of_trades: int  # e.g. 308
    taker_buy_base_asset_volume: str  # e.g. "1756.87402397"
    taker_buy_quote_asset_volume: str  # e.g. "28.46694368"
    ignore: str  # e.g. "0" (unused field)
"""
@pytest.fixture()
def dummy_kline() -> Kline:
    return Kline(
            open_time=1499040000000,
            open_price = "0.01634790",
            high_price = "0.80000000",
            low_price = "0.01575800",
            close_price = "0.01577100",
            volume = "148976.11427815",
            close_time = 1499644799999,
            quote_asset_volume = "2434.19055334",
            number_of_trades = 308,
            taker_buy_base_asset_volume = "1756.87402397",
            taker_buy_quote_asset_volume = "28.46694368",
            ignore = "0"
    )

@pytest.fixture()
def sample_data(dummy_kline: Kline) -> Klines:
    return Klines(klines=[dummy_kline])


def test_from_service_klines(sample_data: Klines, dummy_kline: Kline) -> None:
    """
    test if a single-level kline object is converted into a BinanceKlinePrice DTO using the from_service_kline method
    Prepare: dummy klines object
    Act: read kline objects
    Assert: assert if kline objects are of the right data type
    Teardown: -
    """
    symbol: str = "ETHBTC"

    dto_list: list[BinanceKlinePriceDTO] = BinanceKlinePriceDTO.from_service_klines(symbol=symbol, service_klines=sample_data)
    expected_open_time: datetime = datetime.fromtimestamp(dummy_kline.open_time/1000, tz=timezone.utc)
    expected_close_time: datetime = datetime.fromtimestamp(dummy_kline.close_time/1000, tz=timezone.utc)

    for dto in dto_list:
        assert dto.symbol == symbol
        assert dto.kline_open_time == expected_open_time
        assert dto.kline_close_time == expected_close_time
        assert dto.open_price == Decimal(dummy_kline.open_price)
        assert dto.close_price == Decimal(dummy_kline.close_price)
        assert dto.high_price == Decimal(dummy_kline.high_price)
        assert dto.low_price == Decimal(dummy_kline.low_price)
        assert dto.volume == Decimal(dummy_kline.volume)
        assert dto.quote_asset_volume == Decimal(dummy_kline.quote_asset_volume)
        assert dto.number_of_trades == dummy_kline.number_of_trades
        assert dto.taker_buy_base_asset_vol == Decimal(dummy_kline.taker_buy_base_asset_volume)
        assert dto.taker_buy_quote_asset_vol == Decimal(dummy_kline.taker_buy_quote_asset_volume)
        assert isinstance(dto.created_at, datetime) # check if it is datetime





