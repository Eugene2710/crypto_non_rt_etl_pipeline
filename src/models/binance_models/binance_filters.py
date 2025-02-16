from typing import Literal, Union
from pydantic import BaseModel


class BaseFilter(BaseModel):
    """
    Symbol filter models
    - https://developers.binance.com/docs/binance-spot-api-docs/filters
    """
    filterType: str


class PriceFilter(BaseFilter):
    filterType: Literal["PRICE_FILTER"] = "PRICE_FILTER"
    minPrice: str
    maxPrice: str
    tickSize: str


class PercentPriceFilter(BaseFilter):
    filterType: Literal["PERCENT_PRICE"] = "PERCENT_PRICE"
    multiplierUp: str
    multiplierDown: str
    avgPriceMins: int


class PercentPriceBySideFilter(BaseFilter):
    filterType: Literal["PERCENT_PRICE_BY_SIDE"] = "PERCENT_PRICE_BY_SIDE"
    bidMultiplierUp: str
    bidMultiplierDown: str
    askMultiplierUp: str
    askMultiplierDown: str
    avgPriceMins: int


class LotSizeFilter(BaseFilter):
    filterType: Literal["LOT_SIZE"] = "LOT_SIZE"
    minQty: str
    maxQty: str
    stepSize: str


class MinNotionalFilter(BaseFilter):
    filterType: Literal["MIN_NOTIONAL"] = "MIN_NOTIONAL"
    minNotional: str
    applyToMarket: bool
    avgPriceMins: int


class NotionalFilter(BaseFilter):
    filterType: Literal["NOTIONAL"] = "NOTIONAL"
    minNotional: str
    applyMinToMarket: bool
    maxNotional: str
    applyMaxToMarket: bool
    avgPriceMins: int


class IcebergPartsFilter(BaseFilter):
    filterType: Literal["ICEBERG_PARTS"] = "ICEBERG_PARTS"
    limit: int


class MarketLotSizeFilter(BaseFilter):
    filterType: Literal["MARKET_LOT_SIZE"] = "MARKET_LOT_SIZE"
    minQty: str
    maxQty: str
    stepSize: str


class MaxNumOrdersFilter(BaseFilter):
    filterType: Literal["MAX_NUM_ORDERS"] = "MAX_NUM_ORDERS"
    maxNumOrders: int


class MaxNumAlgoOrdersFilter(BaseFilter):
    filterType: Literal["MAX_NUM_ALGO_ORDERS"] = "MAX_NUM_ALGO_ORDERS"
    maxNumAlgoOrders: int


class MaxNumIcebergOrdersFilter(BaseFilter):
    filterType: Literal["MAX_NUM_ICEBERG_ORDERS"] = "MAX_NUM_ICEBERG_ORDERS"
    maxNumIcebergOrders: int


class MaxPositionFilter(BaseFilter):
    filterType: Literal["MAX_POSITION"] = "MAX_POSITION"
    maxPosition: str


class TrailingDeltaFilter(BaseFilter):
    filterType: Literal["TRAILING_DELTA"] = "TRAILING_DELTA"
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


class ExchangeMaxNumOrdersFilter(BaseFilter):
    """
    Exchange Filter Models
    """
    filterType: Literal["EXCHANGE_MAX_NUM_ORDERS"] = "EXCHANGE_MAX_NUM_ORDERS"
    maxNumOrders: int


class ExchangeMaxNumAlgoOrdersFilter(BaseFilter):
    filterType: Literal["EXCHANGE_MAX_NUM_ALGO_ORDERS"] = "EXCHANGE_MAX_NUM_ALGO_ORDERS"
    maxNumAlgoOrders: int


class ExchangeMaxNumIcebergOrdersFilter(BaseFilter):
    filterType: Literal["EXCHANGE_MAX_NUM_ICEBERG_ORDERS"] = "EXCHANGE_MAX_NUM_ICEBERG_ORDERS"
    maxNumIcebergOrders: int


# Union for exchange filters
ExchangeFilter = Union[
    ExchangeMaxNumOrdersFilter,
    ExchangeMaxNumAlgoOrdersFilter,
    ExchangeMaxNumIcebergOrdersFilter,
]