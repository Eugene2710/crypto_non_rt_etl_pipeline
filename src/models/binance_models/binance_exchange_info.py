from typing import Any, Optional, Literal, Union
from pydantic import BaseModel, ConfigDict


class BinanceExchangeInfo(BaseModel):
    """
    timezone, serverTime
    nested: rateLimits, exchangeFilters, symbols, sors
    """


# rate limit model
class RateLimit(BaseModel):
    rateLimitType: Literal["REQUEST_WEIGHT", "ORDERS", "RAW_REQUESTS"]
    interval: str
    intervalNum: int
    limit: int


# symbol filter models
# https://developers.binance.com/docs/binance-spot-api-docs/filters
class BaseFilter(BaseModel):
    filterType: str


class PriceFilter(BaseFilter):
    filterType = Literal["PRICE_FILTER"]
    minPrice: str
    maxPrice: str
    tickSize: str


class PercentPriceFilter(BaseFilter):
    filterType: Literal["PERCENT_PRICE"]
    minPrice: str
    maxPrice: str
    tickSize: str


class PercentPriceBySideFilter(BaseFilter):
    filterType: Literal["PERCENT_PRICE_BY_SIDE"],
    bidMultiplierUp: str
    bidMultiplierDown: str
    askMultiplierUp: str
    askMultiplierDown: str
    avgPriceMins: int


class LotSizeFilter(BaseFilter):
    filterType: Literal["LOT_SIZE"]
    minQty: str
    maxQty: str
    stepSize: str


class MinNotionalFilter(BaseFilter):
    filterType: Literal["MIN_NOTIONAL"]
    minNotional: str
    applyToMarket: bool
    avgPriceMins: int


class NotionalFilter(BaseFilter):
    filterType: Literal["NOTIONAL"]
    minNotional: str
    applyMinToMarket: bool
    maxNotional: str
    applyMaxToMarket: bool
    avgPriceMins: int


class IcebergPartsFilter(BaseFilter)
    filterType: Literal["ICEBERG_PARTS"]
    limit: int


class MarketLotSizeFilter(BaseFilter):
    filterType: Literal["MARKET_LOT_SIZE"]
    minQty: str
    maxQty: str
    stepSize: str

class MaxNumOrdersFilter(BaseFilter):
    filterType: Literal["MAX_NUM_ORDERS"]
    maxNumOrders: int

class MaxNumAlgoOrdersFilter(BaseFilter):
    filterType: Literal["MAX_NUM_ALGO_ORDERS"]
    maxNumAlgoOrders: int

class MaxNumIcebergOrdersFilter(BaseFilter):
    filterType: Literal["MAX_NUM_ICEBERG_ORDERS"]
    maxNumIcebergOrders: int

class MaxPositionFilter(BaseFilter):
    filterType: Literal["MAX_POSITION"]
    maxPosition: str

class TrailingDeltaFilter(BaseFilter):
    filterType: Literal["TRAILING_DELTA"]
    minTrailingAboveDelta: int
    maxTrailingAboveDelta: int
    minTrailingBelowDelta: int
    maxTrailingBelowDelta: int


# Union for symbol filters
SymbolFilter = Union[
    PriceFilter,
    PercentPriceFilter,
    PercentPriceBySideFilter,
    LotSizeFilter,
    MinNotionalFilter,
    NotionalFilter,
    IcebergPartsFilter,
    MarketLotSizeFilter,
    MaxNumOrdersFilter,
    MaxNumAlgoOrdersFilter,
    MaxNumIcebergOrdersFilter,
    MaxPositionFilter,
    TrailingDeltaFilter,
]


# Exchange Filter Models
class ExchangeMaxNumOrdersFilter(BaseFilter):
    filterType: Literal["EXCHANGE_MAX_NUM_ORDERS"]
    maxNumOrders: int


class ExchangeMaxNumAlgoOrdersFilter(BaseFilter):
    filterType: Literal["EXCHANGE_MAX_NUM_ALGO_ORDERS"]
    maxNumAlgoOrders: int


class ExchangeMaxNumIcebergOrdersFilter(BaseFilter):
    filterType: Literal["EXCHANGE_MAX_NUM_ICEBERG_ORDERS"]
    maxNumIcebergOrders: int

# Union for exchange filters
ExchangeFilter = Union[
    ExchangeMaxNumOrdersFilter,
    ExchangeMaxNumAlgoOrdersFilter,
    ExchangeMaxNumIcebergOrdersFilter,
]


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
    rateLimits: Optional[list[RateLimit]]
    exchangeFilters: list[Any] # currently unknown as mentioned in the docs - requires further testing
    symbols: list[Symbol]
    sors: Optional[list[SOR]] # optional field; only present when SOR is available

