from typing import Optional
from pydantic import BaseModel
from src.models.binance_models.binance_rate_limit import RateLimit
from src.models.binance_models.binance_filters import SymbolFilter, ExchangeFilter
from pprint import pprint


# Other Models
class Symbol(BaseModel):
    symbol: str
    status: str
    baseAsset: str
    baseAssetPrecision: int
    quoteAsset: str
    quotePrecision: Optional[int] = None  # slated for removal in future API versions
    quoteAssetPrecision: int
    baseCommissionPrecision: int
    quoteCommissionPrecision: int
    orderTypes: list[str]
    icebergAllowed: bool
    ocoAllowed: bool
    otoAllowed: bool
    quoteOrderQtyMarketAllowed: bool
    allowTrailingStop: bool
    cancelReplaceAllowed: bool
    isSpotTradingAllowed: bool
    isMarginTradingAllowed: bool
    filters: list[SymbolFilter]
    permissions: list[str]
    permissionSets: list[list[str]]
    defaultSelfTradePreventionMode: str
    allowedSelfTradePreventionModes: list[str]


class SOR(BaseModel):
    baseAsset: str
    symbols: list[str]


class ExchangeInfo(BaseModel):
    timezone: str
    serverTime: int
    rateLimits: Optional[list[RateLimit]] = None
    exchangeFilters: Optional[list[ExchangeFilter]] = None
    symbols: list[Symbol]
    sors: Optional[list[SOR]] = None


if __name__ == "__main__":
    # Sample JSON data based on Binance's /exchangeInfo endpoint.
    sample_json = {
        "timezone": "UTC",
        "serverTime": 1625246363776,
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 6000,
            },
            {
                "rateLimitType": "ORDERS",
                "interval": "SECOND",
                "intervalNum": 1,
                "limit": 10,
            },
        ],
        "exchangeFilters": [
            {"filterType": "EXCHANGE_MAX_NUM_ORDERS", "maxNumOrders": 1000}
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
                "allowedSelfTradePreventionModes": ["NONE"],
            }
        ],
        "sors": [{"baseAsset": "BTC", "symbols": ["BTCUSDT", "BTCUSDC"]}],
    }

    # Parse the JSON into an ExchangeInfo object
    exchange_info = ExchangeInfo.model_validate(sample_json)
    pprint(exchange_info)
