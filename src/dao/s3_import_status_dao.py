import os
from asyncio import new_event_loop, AbstractEventLoop

from dotenv import load_dotenv
from retry import retry
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncConnection
from sqlalchemy import (
    Table,
    insert,
    Insert,
    Select,
    select,
    func,
    CursorResult,
    Row,
)
from datetime import datetime
import logging

from database_management.tables import s3_import_status_table
from src.models.database_transfer_objects.s3_import_status import S3ImportStatusDTO
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


class S3ImportStatusDAO:
    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._table: Table = s3_import_status_table

    @retry(
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        jitter=(-0.01, 0.01),
        backoff=1.5,
    )
    async def insert_latest_import_status(
        self, import_status: S3ImportStatusDTO, conn: AsyncConnection
    ) -> None:
        insert_text_clause: Insert = insert(self._table).values(
            import_status.model_dump()
        )  # values take in a dict, hence the need to convert the dto to a dict
        try:
            await conn.execute(insert_text_clause)
        except SQLAlchemyError:
            logger.exception("Failed to insert latest import status")
            raise

    @retry(
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        jitter=(-0.01, 0.01),
        backoff=1.5,
    )
    async def read_latest_import_status(self, data_source) -> datetime | None:
        query_latest_import_status: Select = select(
            func.max(self._table.c.file_modified_date)
        ).where(self._table.c.data_source == data_source)
        try:
            async with self._engine.begin() as conn:
                cursor_result: CursorResult = await conn.execute(
                    query_latest_import_status
                )
            result: Row | None = cursor_result.fetchone()
        except SQLAlchemyError:
            logger.exception("failed to fetch from s3 latest import status")
            raise
        return result[0] if result else None


if __name__ == "__main__":
    load_dotenv()
    connection_string: str = os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", "")
    dao: S3ImportStatusDAO = S3ImportStatusDAO(connection_string)
    s3_import_status_dto: S3ImportStatusDTO = S3ImportStatusDTO(
        data_source="chainstack_eth_blocks",
        file_modified_date=datetime.utcnow(),
        created_at=datetime.utcnow(),
    )

    # async def run_insert_status() -> None:
    #     engine: AsyncEngine = create_async_engine(connection_string)
    #     async with engine.begin() as conn:
    #         await dao.insert_latest_import_status(s3_import_status_dto, conn)
    # asyncio.run(run_insert_status())

    event_loop: AbstractEventLoop = new_event_loop()
    latest_file_date: datetime | None = event_loop.run_until_complete(
        dao.read_latest_import_status(data_source="chainstack_eth_blocks")
    )
    print(latest_file_date)
