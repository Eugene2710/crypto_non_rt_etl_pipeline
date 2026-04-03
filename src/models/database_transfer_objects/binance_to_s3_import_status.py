from pydantic import BaseModel
from datetime import datetime


class BinanceToS3ImportStatusDTO(BaseModel):
    data_source: str
    symbol: str
    kline_open_time: datetime
    created_at: datetime