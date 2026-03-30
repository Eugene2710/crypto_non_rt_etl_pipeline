import os
from sqlalchemy import (
    TextClause,
    text,
    CursorResult,
    Row,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from datetime import datetime
from dotenv import load_dotenv
import logging
import asyncio
from tenacity import retry, wait_fixed, stop_after_attempt

from src.models.database_transfer_objects.binance.binance_klines import BinanceKlinePriceDTO
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class KlineBinanceDAO:
    """
    DAO responsible for CRUD operations into binance.binance_klines_prices table

    Responsible for
    - read single kline by
    - inserting
    """
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def read_kline(self, symbol: str, kline_open_time: datetime) -> BinanceKlinePriceDTO | None:
        query: str = (
            "SELECT symbol, kline_open_time, kline_close_time, open_price, high_price, low_price, close_price, "
            "volume, quote_asset_volume, number_of_trades, taker_buy_base_asset_vol, taker_buy_quote_asset_vol, created_at "
            "FROM binance_klines_prices WHERE symbol = :symbol AND kline_open_time = :kline_open_time LIMIT 1"
        )
        query_text_clause: TextClause = text(query)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(
                query_text_clause, {"symbol": symbol, "kline_open_time": kline_open_time}
            )

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            binance_kline_dto: BinanceKlinePriceDTO = BinanceKlinePriceDTO(
                symbol=single_row[0],
                kline_open_time=single_row[1],
                kline_close_time=single_row[2],
                open_price=single_row[3],
                high_price=single_row[4],
                low_price=single_row[5],
                close_price=single_row[6],
                volume=single_row[7],
                quote_asset_volume=single_row[8],
                number_of_trades=single_row[9],
                taker_buy_base_asset_vol=single_row[10],
                taker_buy_quote_asset_vol=single_row[11],
                created_at=single_row[12],
            )
            return binance_kline_dto

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def insert_kline(self, async_connection: AsyncConnection, input: list[BinanceKlinePriceDTO]) -> None:
        if not input:
            print("insert kline: not input, exiting")
            return

        for single_input in input:
            if single_input.symbol is None:
                raise ValueError(
                    f"kline at {single_input.kline_open_time} is missing symbol data"
                )
            if single_input.kline_open_time is None:
                raise ValueError(
                    f"kline at {single_input.symbol} is missing kline_open_time data"
                )

        insert_kline: str = (
            "INSERT INTO binance_klines_prices (symbol, kline_open_time, kline_close_time, open_price, high_price, "
            "low_price, close_price, volume, quote_asset_volume, number_of_trades, taker_buy_base_asset_vol, taker_buy_quote_asset_vol, created_at "
            ") values (:symbol, :kline_open_time, :kline_close_time, :open_price, "
            ":high_price, :low_price, :close_price, :volume, :quote_asset_volume, :number_of_trades, "
            ":taker_buy_base_asset_vol, :taker_buy_quote_asset_vol, :created_at) ON CONFLICT DO NOTHING "
            "RETURNING symbol, kline_open_time, kline_close_time, open_price, high_price, "
            "low_price, close_price, volume, quote_asset_volume, number_of_trades, taker_buy_base_asset_vol, taker_buy_quote_asset_vol, created_at "
        )
        insert_text_clause: TextClause = text(insert_kline)
        rows_to_insert = [
            {
                "symbol": single_input.symbol,
                "kline_open_time": single_input.kline_open_time,
                "kline_close_time": single_input.kline_close_time,
                "open_price": single_input.open_price,
                "high_price": single_input.high_price,
                "low_price": single_input.low_price,
                "close_price": single_input.close_price,
                "volume": single_input.volume,
                "quote_asset_volume": single_input.quote_asset_volume,
                "number_of_trades": single_input.number_of_trades,
                "taker_buy_base_asset_vol": single_input.taker_buy_base_asset_vol,
                "taker_buy_quote_asset_vol": single_input.taker_buy_quote_asset_vol,
                "created_at": single_input.created_at,
            } for single_input in input
        ]
        _: CursorResult = await async_connection.execute(
            insert_text_clause, rows_to_insert
        )


if __name__ == "__main__":
    load_dotenv()
    connection_str: str = os.getenv("BINANCE_PG_CONNECTION_STRING", "")
    kline_dao: KlineBinanceDAO = KlineBinanceDAO(connection_str)

    input_klines: list[BinanceKlinePriceDTO] = [
        BinanceKlinePriceDTO.model_validate({
            "symbol": "btcusdc",
            "kline_open_time": datetime(2025, 2, 19, 16, 1, 0),
            "kline_close_time": datetime(2025, 2, 19, 16, 2, 0),
            "open_price": 0.02811000,
            "high_price": 0.02812000,
            "low_price": 0.02811000,
            "close_price": 0.02812000,
            "volume": 3.36490000,
            "quote_asset_volume": 0.09459171,
            "number_of_trades": 8,
            "taker_buy_base_asset_vol": 3.36490000,
            "taker_buy_quote_asset_vol": 0.09459171,
            "created_at": datetime.utcnow(),
        }
        )
    ]

    async def run_insert_klines() -> None:
        engine: AsyncEngine = create_async_engine(connection_str)
        async with engine.begin() as conn:
            await kline_dao.insert_kline(async_connection=conn, input=input_klines)

    asyncio.run(run_insert_klines())
