from datetime import datetime, timezone
from pydantic import BaseModel
from typing import Any
from src.models.binance_models.binance_exchange_info import ExchangeInfo


class BinanceTickerMetadataDTO(BaseModel):
    symbol: str
    server_time: datetime
    status: str
    base_asset: str
    quote_asset: str
    created_at: datetime

    @staticmethod
    def from_exchange_info(exchange_info: "ExchangeInfo") -> list["BinanceTickerMetadata"]:
        """
        Converts ExchangeInfo into a list of BinanceTickerMetadataDTO objects
        """
        # Use exchange-level serverTime for all symbols since symbol object does not have its own time
        # convert serverTime (in ms) to a datetime object
        server_time_dt: datetime = datetime.fromtimestamp(exchange_info.serverTime / 1000, tz=timezone.utc)
        dtos: list["BinanceTickerMetadataDTO"] = [
            BinanceTickerMetadataDTO(
                symbol=symbol_obj.symbol,
                server_time=server_time_dt,
                status=symbol_obj.status,
                base_asset=symbol_obj.baseAsset,
                quote_asset=symbol_obj.quoteAsset,
                created_at=datetime.now(timezone.utc),
            ) for symbol_obj in exchange_info.symbols
        ]
        return dtos


if __name__ == "__main__":
    sample_json = {
        "timezone": "UTC",
        "serverTime": 1625246363776,
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 6000
            },
            {
                "rateLimitType": "ORDERS",
                "interval": "SECOND",
                "intervalNum": 1,
                "limit": 10
            }
        ],
        "exchangeFilters": [
            {
                "filterType": "EXCHANGE_MAX_NUM_ORDERS",
                "maxNumOrders": 1000
            }
        ],
        "symbols": [
            {
                "symbol": "ETHBTC",
                "status": "TRADING",
                "baseAsset": "ETH",
                "baseAssetPrecision": 8,
                "quoteAsset": "BTC",
                "quotePrecision": 8,
                "quoteAssetPrecision": 8,
                "baseCommissionPrecision": 8,
                "quoteCommissionPrecision": 8,
                "orderTypes": ["LIMIT", "MARKET"],
                "icebergAllowed": True,
                "ocoAllowed": True,
                "otoAllowed": True,
                "quoteOrderQtyMarketAllowed": True,
                "allowTrailingStop": False,
                "cancelReplaceAllowed": False,
                "isSpotTradingAllowed": True,
                "isMarginTradingAllowed": True,
                "filters": [],  # If needed, add filter objects here.
                "permissions": [],
                "permissionSets": [["SPOT"]],
                "defaultSelfTradePreventionMode": "NONE",
                "allowedSelfTradePreventionModes": ["NONE"]
            }
        ],
        "sors": [
            {
                "baseAsset": "BTC",
                "symbols": ["BTCUSDT", "BTCUSDC"]
            }
        ]
    }
    exchange_info = ExchangeInfo.model_validate(sample_json)
    dtos = BinanceTickerMetadataDTO.from_exchange_info(exchange_info)
    for dto in dtos:
        print(dto.model_dump())

