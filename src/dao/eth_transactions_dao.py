from typing import Sequence

import retry
from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from src.models.database_transfer_objects.eth_transaction import EthTransactionDTO


class EthTransactionDAO:
    """
    DAO responsible for CRUD operations into quick_node.eth_transactions table

    Responsible for
    - read single transaction by hash
    - inserting multiple transactions into table

    Table: quick_node.eth_transactions table
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
    async def read_transaction_by_hash(
        self, transaction_hash: str
    ) -> EthTransactionDTO | None:
        query_transaction_by_hash: str = (
            "SELECT hash, block_id, blockhash, blocknumber, chainid, from_address, "
            "gas, gasprice, input, maxfeepergas, maxpriorityfeepergas, nonce, r, s, to_address, "
            "transactionindex, type, v, value, yparity, created_at "
            "FROM eth_transactions WHERE hash = :hash limit 1"
        )
        query_text_clause: TextClause = text(query_transaction_by_hash)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(
                query_text_clause, {"hash": transaction_hash}
            )

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            eth_transaction_dto: EthTransactionDTO = EthTransactionDTO(
                hash=single_row[0],
                block_id=single_row[1],
                blockhash=single_row[2],
                blocknumber=single_row[3],
                chainid=single_row[4],
                from_address=single_row[5],
                gas=single_row[6],
                gasprice=single_row[7],
                input=single_row[8],
                maxfeepergas=single_row[9],
                maxpriorityfeepergas=single_row[10],
                nonce=single_row[11],
                r=single_row[12],
                s=single_row[13],
                to_address=single_row[14],
                transactionindex=single_row[15],
                type=single_row[16],
                v=single_row[17],
                value=single_row[18],
                yparity=single_row[19],
                created_at=single_row[20],
            )
            return eth_transaction_dto

    @retry.retry(
        exceptions=SQLAlchemyError,
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def insert_transactions(
        self, async_connection: AsyncConnection, input: list[EthTransactionDTO]
    ) -> None:
        for single_input in input:
            if single_input.hash is None:
                raise ValueError("One of the transactions is missing the 'hash' attribute")

        insert_transactions: str = (
            "INSERT into eth_transactions (hash, block_id, blockhash, blocknumber, chainid, from_address, "
            "gas, gasprice, input, maxfeepergas, maxpriorityfeepergas, nonce, r, s, to_address, "
            "transactionindex, type, v, value, yparity, created_at) values ("
            ":hash, :block_id, :blockhash, :blocknumber, :chainid, :from_address, "
            ":gas, :gasprice, :input, :maxfeepergas, :maxpriorityfeepergas, :nonce, :r, :s, :to_address, "
            ":transactionindex, :type, :v, :value, :yparity, :created_at) "
            "RETURNING hash, block_id, blockhash, blocknumber, chainid, from_address, "
            "gas, gasprice, input, maxfeepergas, maxpriorityfeepergas, nonce, r, s, to_address, "
            "transactionindex, type, v, value, yparity, created_at"
        )
        insert_text_clause: TextClause = text(insert_transactions)

        cursor_result: CursorResult = await async_connection.execute(
            insert_text_clause,
            [
                {
                    "hash": single_input.hash,
                    "block_id": single_input.block_id,
                    "blockhash": single_input.blockhash,
                    "blocknumber": single_input.blocknumber,
                    "chainid": single_input.chainid,
                    "from_address": single_input.from_address,
                    "gas": single_input.gas,
                    "gasprice": single_input.gasprice,
                    "input": single_input.input,
                    "maxfeepergas": single_input.maxfeepergas,
                    "maxpriorityfeepergas": single_input.maxpriorityfeepergas,
                    "nonce": single_input.nonce,
                    "r": single_input.r,
                    "s": single_input.s,
                    "to_address": single_input.to_address,
                    "transactionindex": single_input.transactionindex,
                    "type": single_input.type,
                    "v": single_input.v,
                    "value": single_input.value,
                    "yparity": single_input.yparity,
                    "created_at": single_input.created_at,
                }
                for single_input in input
            ],
        )