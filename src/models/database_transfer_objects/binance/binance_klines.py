from datetime import datetime, timezone
from decimal import Decimal
from pydantic import BaseModel
from src.models.binance_models.binance_klines import Klines, Kline


class BinanceKlinePriceDTO(BaseModel):
    symbol: str
    kline_open_time: datetime
    kline_close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_asset_volume: Decimal
    number_of_trades: int
    taker_buy_base_asset_vol: Decimal
    taker_buy_quote_asset_vol: Decimal
    created_at: datetime

    @staticmethod
    def from_service_kline(symbol: str, service_kline: Kline) -> "BinanceKlinePriceDTO":
        """
        Converts a single service-level Kline object into a BinanceKlinePriceDTO
        """
        return BinanceKlinePriceDTO(
            symbol=symbol,
            kline_open_time=datetime.fromtimestamp(service_kline.open_time/1000, tz=timezone.utc),
            kline_close_time=datetime.fromtimestamp(service_kline.close_time/1000, tz=timezone.utc),
            open_price=Decimal(service_kline.open_price),
            high_price=Decimal(service_kline.high_price),
            low_price=Decimal(service_kline.low_price),
            close_price=Decimal(service_kline.close_price),
            volume=Decimal(service_kline.volume),
            quote_asset_volume=Decimal(service_kline.quote_asset_volume),
            number_of_trades=service_kline.number_of_trades,
            taker_buy_base_asset_vol=Decimal(service_kline.taker_buy_base_asset_volume),
            taker_buy_quote_asset_vol=Decimal(service_kline.taker_buy_quote_asset_volume),
            created_at=datetime.now(timezone.utc)
        )

    @staticmethod
    def from_service_klines(symbol: str, service_klines: Klines) -> list["BinanceKlinePriceDTO"]:
        """
        Converts a Klines object into a list of BinanceKlinesPriceDTO objects, one per kline
        """
        return [
            BinanceKlinePriceDTO.from_service_kline(symbol, kline) for kline in service_klines.klines
        ]


if __name__=="__main__":
    sample_data = [
        [
            1499040000000,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499644799999,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0"
        ]
    ]
    # parse sample data into the service level data class
    service_klines: Klines = Klines.from_json(sample_data)
    # convert service level data class into DTOs
    dto_list = BinanceKlinePriceDTO.from_service_klines("ETHBTC", service_klines)

    for dto in dto_list:
        print(dto.model_dump_json(indent=2)) # for pretty printing

