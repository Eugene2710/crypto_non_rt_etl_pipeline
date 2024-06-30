from typing import Sequence

import retry
from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection

from src.models.database_transfer_objects.eth_transaction_access_list import (
    EthTransactionAccessListDTO,
)


class EthTransactionAccessListDAO:
    """
    DAO responsible for CRUD operations into quick_node.eth_transaction_access_list table

    Responsible for
    - read single eth_transaction_access_list by id
    - inserting multiple eth_transaction_access_list into table

    Table: quick_node.eth_transaction_access_list table
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
    async def read_transaction_access_list_by_id(
        self, id: str
    ) -> EthTransactionAccessListDTO | None:
        query_transaction_access_list_by_id: str = (
            "SELECT id, transaction_hash, address, storagekeys, created_at "
            "FROM eth_transaction_access_list WHERE id = :id limit 1"
        )
        query_text_clause: TextClause = text(query_transaction_access_list_by_id)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(
                query_text_clause, {"id": id}
            )

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            eth_transaction_access_list_dto: EthTransactionAccessListDTO = (
                EthTransactionAccessListDTO(
                    id=single_row[0],
                    transaction_hash=single_row[1],
                    address=single_row[2],
                    storagekeys=single_row[3],
                    created_at=single_row[4],
                )
            )
            return eth_transaction_access_list_dto

    @retry.retry(
        exceptions=SQLAlchemyError,
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def insert_transaction_access_list(
        self,
        async_connection: AsyncConnection,
        input: list[EthTransactionAccessListDTO],
    ) -> None:
        insert_block: str = (
            "INSERT into eth_transaction_access_list (id, transaction_hash, address, storagekeys, created_at) values ("
            ":id, :transaction_hash, :address, :storagekeys, :created_at) "
            "RETURNING id, transaction_hash, address, storagekeys, created_at"
        )
        insert_text_clause: TextClause = text(insert_block)

        cursor_result: CursorResult = await async_connection.execute(
            insert_text_clause,
            [
                {
                    "id": single_input.id,
                    "transaction_hash": single_input.transaction_hash,
                    "address": single_input.address,
                    "storagekeys": single_input.storagekeys,
                    "created_at": single_input.created_at,
                }
                for single_input in input
            ],
        )
        inserted_rows: Sequence[Row] = cursor_result.fetchall()
        if inserted_rows:
            return None
        else:
            # okay to raise error; after 5 retries, this exception stops the data pipeline
            # this is by design; it is better for the data pipeline to stop, than to silently fail
            raise SQLAlchemyError("Failed to insert blocks. Retrying...")
