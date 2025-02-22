import asyncio
import aiohttp
from typing import Any
from pprint import pprint

from src.models.binance_models.binance_klines import Klines
from src.models.database_transfer_objects.binance.binance_klines import BinanceKlinePriceDTO


async def get_binance_kline() -> Klines:
    