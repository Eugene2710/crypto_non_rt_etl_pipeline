from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)
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

    @retry(
        retry=retry_if_exception_type(SQLAlchemyError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.1, exp_base=1.5, max=0.3375)
        + wait_random(-0.01, 0.01),
        reraise=True,
    )
    async def read_transaction_access_list_by_id(
        self, id: str
    ) -> EthTransactionAccessListDTO | None:
        query_transaction_access_list_by_id: str = (
            "SELECT id, transaction_hash, item_index, address, storagekeys, created_at "
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
                    item_index=single_row[2],
                    address=single_row[3],
                    storageKeys=single_row[4],
                    created_at=single_row[5],
                )
            )
            return eth_transaction_access_list_dto

    @retry(
        retry=retry_if_exception_type(SQLAlchemyError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.1, exp_base=1.5, max=0.3375)
        + wait_random(-0.01, 0.01),
        reraise=True,
    )
    async def insert_transaction_access_list(
        self,
        async_connection: AsyncConnection,
        input: list[EthTransactionAccessListDTO],
    ) -> None:
        if not input:
            print("insert_transaction_access_list: No input. Exiting")
            return
        insert_block: str = (
            "INSERT into eth_transaction_access_list (id, transaction_hash, item_index, address, storagekeys, created_at) values ("
            ":id, :transaction_hash, :item_index, :address, :storagekeys, :created_at) "
            # conflict target is the natural key, not the uuid4 primary key: a re-run
            # mints a fresh id, so an untargeted ON CONFLICT would never fire
            "ON CONFLICT (transaction_hash, item_index) DO NOTHING"
        )
        insert_text_clause: TextClause = text(insert_block)

        # the result is intentionally discarded. Passing a list of params takes the
        # executemany path, where the result has returns_rows=False and calling
        # fetchall() on it raises ResourceClosedError -- even when every row inserted
        # successfully. A genuine failure is raised by execute() itself, which is
        # where the retry decorator above can see it.
        _: CursorResult = await async_connection.execute(
            insert_text_clause,
            [
                {
                    "id": single_input.id,
                    "transaction_hash": single_input.transaction_hash,
                    "item_index": single_input.item_index,
                    "address": single_input.address,
                    "storagekeys": single_input.storageKeys,
                    "created_at": single_input.created_at,
                }
                for single_input in input
            ],
        )
