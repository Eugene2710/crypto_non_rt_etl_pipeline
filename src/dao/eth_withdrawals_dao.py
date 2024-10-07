import retry
from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from src.models.database_transfer_objects.eth_withdrawals import EthWithdrawalDTO


class EthWithdrawalDAO:
    """
    DAO responsible for CRUD operations into quick_node.eth_withdrawals table

    Responsible for
    - read single withdrawal by id
    - inserting multiple withdrawals into table

    Table: quick_node.eth_withdrawals table
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
    async def read_withdrawal_by_id(self, id: str) -> EthWithdrawalDTO | None:
        query_withdrawal_by_id: str = (
            "SELECT id, block_number, address, amount, index, validatorIndex, created_at "
            "FROM eth_withdrawals WHERE id = :id limit 1"
        )
        query_text_clause: TextClause = text(query_withdrawal_by_id)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(
                query_text_clause, {"id": id}
            )

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            eth_withdrawal_dto: EthWithdrawalDTO = EthWithdrawalDTO(
                id=single_row[0],
                block_number=single_row[1],
                address=single_row[2],
                amount=single_row[3],
                index=single_row[4],
                validatorIndex=single_row[5],
                created_at=single_row[6],
            )
            return eth_withdrawal_dto

    @retry.retry(
        exceptions=SQLAlchemyError,
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def insert_withdrawals(
        self, async_connection: AsyncConnection, input: list[EthWithdrawalDTO]
    ) -> None:
        if not input:
            print("insert_withdrawals: No input. Exiting")
            return
        insert_block: str = (
            "INSERT into eth_withdrawals (id, block_number, address, amount, index, validatorIndex, created_at) values ("
            ":id, :block_number, :address, :amount, :index, :validatorIndex, :created_at) "
            "RETURNING id, block_id, address, amount, index, validatorIndex, created_at"
        )
        insert_text_clause: TextClause = text(insert_block)

        _: CursorResult = await async_connection.execute(
            insert_text_clause,
            [
                {
                    "id": single_input.id,
                    "block_number": single_input.block_number,
                    "address": single_input.address,
                    "amount": single_input.amount,
                    "index": single_input.index,
                    "validatorindex": single_input.validatorIndex,
                    "created_at": single_input.created_at,
                }
                for single_input in input
            ],
        )
