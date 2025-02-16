from typing import Literal
from pydantic import BaseModel


# rate limit model
# https://developers.binance.com/docs/binance-spot-api-docs/enums#rate-limiters-ratelimittype
class RateLimit(BaseModel):
    rateLimitType: Literal["REQUEST_WEIGHT", "ORDERS", "RAW_REQUESTS"]
    interval: str
    intervalNum: int
    limit: int