import os
from asyncio import new_event_loop, AbstractEventLoop

from dotenv import load_dotenv
from tenacity import retry, wait_fixed, stop_after_attempt
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncConnection
from sqlalchemy import (
    Table,
    Select,
    select,
    func,
    CursorResult,
    Row,
)
from datetime import datetime
import logging
from sqlalchemy.dialects.postgresql import insert, Insert

from database_management.binance.binance_table import provider_to_s3_import_status_table
from src.models.database_transfer_objects.binance_to_s3_import_status import BinanceToS3ImportStatusDTO
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class ProviderToS3ImportStatusDAO:
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = provider_to_s3_import_status_table

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def insert_latest_import_status(self, import_status: BinanceToS3ImportStatusDTO) -> None:
        # values take in a dict, hence the need to convert the dto to a dict
        insert_text_clause: Insert = insert(self._table).values(
            import_status.model_dump()
        ).on_conflict_do_nothing()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(insert_text_clause)
        except SQLAlchemyError:
            logger.exception("Failed to insert latest import status")
            raise

    @retry(
        wait=wait_fixed(0.01),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def read_latest_kline_import_status(self, table: str) -> datetime | None:
        # the latest is defined by latest kline
        query_latest_import_status: Select = select(
            func.coalesce(func.max(self._table.c.kline_open_time), datetime(1970, 3, 30))
        ).where(self._table.c.data_source == table)
        try:
            async with self._engine.begin() as conn:
                cursor_result: CursorResult = await conn.execute(
                    query_latest_import_status
                )
            result: Row | None = cursor_result.fetchone()
        except SQLAlchemyError:
            logger.exception("Failed to fetch from s3 latest kline import status")
            raise
        return result[0] if result else None


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("BINANCE_PG_CONNECTION_STRING", "")
    dao: ProviderToS3ImportStatusDAO = ProviderToS3ImportStatusDAO(connection_string=connection_string)
    event_loop: AbstractEventLoop = new_event_loop()
    latest_kline_date: datetime | None = event_loop.run_until_complete(dao.read_latest_kline_import_status(
        table="binance_klines"
    ))
    print(f"latest kline date: {latest_kline_date}")