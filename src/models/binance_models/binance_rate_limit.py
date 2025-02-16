from typing import Literal
from pydantic import BaseModel


class RateLimit(BaseModel):
    """
    Rate Limit Model
    - https://developers.binance.com/docs/binance-spot-api-docs/enums#rate-limiters-ratelimittype
    """
    rateLimitType: Literal["REQUEST_WEIGHT", "ORDERS", "RAW_REQUESTS"]
    interval: str
    intervalNum: int
    limit: int