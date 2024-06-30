
import retry
from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError

from src.models.database_transfer_objects.eth_block_import_status import (
    EthBlockImportStatusDTO,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection


class EthBlockImportStatusDAO:
    """
    DAO responsible for CRUD operations into quick_node.eth_block_import_status table

    Responsible for
    - querying the latest block number
    - inserting the latest block number into

    Table: quick_node.eth_block_import_status table
    """

    def __init__(self, connection_string: str) -> None:
        self._engine: AsyncEngine = create_async_engine(connection_string)

    @retry.retry(
        exceptions=SQLAlchemyError,
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def read_latest_import_status(self) -> EthBlockImportStatusDTO | None:
        query_latest_import_status: str = (
            "SELECT id, block_number, created_at from eth_block_import_status ORDER BY block_number LIMIT 1"
        )
        query_text_clause: TextClause = text(query_latest_import_status)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(query_text_clause)

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            eth_block_import_status_dto: EthBlockImportStatusDTO = (
                EthBlockImportStatusDTO(
                    id=single_row[0],
                    block_number=single_row[1],
                    created_at=single_row[2],
                )
            )
            return eth_block_import_status_dto

    @retry.retry(
        exceptions=SQLAlchemyError,
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def insert_import_status(
        self, async_connection: AsyncConnection, input: EthBlockImportStatusDTO
    ) -> EthBlockImportStatusDTO | None:
        insert_import_status: str = (
            "INSERT INTO eth_block_import_status(id, block_number, created_at) "
            "VALUES (:id, :block_number, :created_at) "
            "RETURNING id, block_number, created_at"
        )
        insert_text_clause: TextClause = text(insert_import_status)

        async with async_connection:
            cursor_result: CursorResult = await async_connection.execute(
                insert_text_clause,
                {
                    "id": input.id,
                    "block_number": input.block_number,
                    "created_at": input.created_at,
                },
            )
        inserted_row: Row | None = cursor_result.fetchone()

        if inserted_row:
            return EthBlockImportStatusDTO(
                id=inserted_row.id,
                block_number=inserted_row.block_number,
                created_at=inserted_row.created_at,
            )
        else:
            # okay to raise error; after 5 retries, this exception stops the data pipeline
            # this is by design; it is better for the data pipeline to stop, than to silently fail
            raise SQLAlchemyError(
                f"Failed to insert import status, id: {input.id}, block_number: {input.block_number}, created_at: {input.created_at}. Retrying..."
            )
