import asyncio
import datetime
from typing import Sequence

import retry
from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection

from src.dao.eth_block_dao import EthBlockDAO
from src.models.database_transfer_objects.eth_blocks import EthBlockDTO
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
            "INSERT into eth_transactions (hash, block_id, blockhash, block_number, chainid, from_address, "
            "gas, gasprice, input, maxfeepergas, maxpriorityfeepergas, nonce, r, s, to_address, "
            "transactionindex, type, v, value, yparity, created_at) values ("
            ":hash, :block_id, :blockhash, :block_number, :chainid, :from_address, "
            ":gas, :gasprice, :input, :maxfeepergas, :maxpriorityfeepergas, :nonce, :r, :s, :to_address, "
            ":transactionindex, :type, :v, :value, :yparity, :created_at) "
            "RETURNING hash, block_id, blockhash, block_number, chainid, from_address, "
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
                    "blockhash": single_input.blockHash,
                    "block_number": single_input.blockNumber,
                    "chainid": single_input.chainId,
                    "from_address": single_input.from_address,
                    "gas": single_input.gas,
                    "gasprice": single_input.gasPrice,
                    "input": single_input.input,
                    "maxfeepergas": single_input.maxFeePerGas,
                    "maxpriorityfeepergas": single_input.maxPriorityFeePerGas,
                    "nonce": single_input.nonce,
                    "r": single_input.r,
                    "s": single_input.s,
                    "to_address": single_input.to_address,
                    "transactionindex": single_input.transactionIndex,
                    "type": single_input.type,
                    "v": single_input.v,
                    "value": single_input.value,
                    "yparity": single_input.yParity,
                    "created_at": single_input.created_at,
                }
                for single_input in input
            ],
        )


if __name__ == "__main__":
    connection_string: str = "postgresql+asyncpg://localhost:5432/quick_node"
    transaction_dao: EthTransactionDAO = EthTransactionDAO(connection_string)
    block_dao: EthBlockDAO = EthBlockDAO(connection_string)

    input_block_list: list[EthBlockDTO] = [
        EthBlockDTO.model_validate(
            {
                "block_number": "12345678",
                "id": 1,
                "jsonrpc": "2.0",
                "baseFeePerGas": "1000000000",
                "blobGasUsed": None,
                "difficulty": "10000000000000",
                "excessBlobGas": None,
                "extraData": "0x12345678",
                "gasLimit": "15000000",
                "gasUsed": "12000000",
                "hash": "0xabcdef123456789abcdef123456789abcdef123456789abcdef123456789abcdef",
                "logsBloom": "0x123456...",
                "miner": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "mixHash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "nonce": "0xabcdefabcdefabcdef",
                "number": "12345678",
                "parentBeaconBlockRoot": None,
                "parentHash": "0xabcdef123456789abcdef123456789abcdef123456789abcdef123456789abcdef",
                "receiptsRoot": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "sha3Uncles": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "size": "2000",
                "stateRoot": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "timestamp": "1627564800",
                "totalDifficulty": "20000000000000",
                "transactionsRoot": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "withdrawalsRoot": None,
                "created_at": datetime.datetime(2024, 7, 2, 12, 0, 0)
            }
        )
    ]

    input_transaction_list: list[EthTransactionDTO] = [
        EthTransactionDTO.model_validate(
            {
                "hash": "0x123456789abcdef123456789abcdef123456789abcdef123456789abcdef123456",
                "block_id": 12345678,
                "blockHash": "0xabcdef123456789abcdef123456789abcdef123456789abcdef123456789abcdef",
                "blockNumber": "12345678",
                "chainId": "1",
                "from_address": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "gas": "21000",
                "gasPrice": "20000000000",
                "input": "0x",
                "maxFeePerGas": None,
                "maxPriorityFeePerGas": "2000000000",
                "nonce": "0",
                "r": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "s": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "to_address": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdef",
                "transactionIndex": "0",
                "type": "0x2",
                "v": "0x1c",
                "value": "1000000000000000000",
                "yParity": None,
                "created_at": datetime.datetime(2024, 7, 2, 12, 0, 0)
            }
        )
    ]

    async def run_insert_transaction() -> None:
        engine: AsyncEngine = create_async_engine(connection_string)
        async with engine.begin() as conn:
            await block_dao.insert_blocks(conn, input_block_list)
            await transaction_dao.insert_transactions(conn, input_transaction_list)

    asyncio.run(run_insert_transaction())