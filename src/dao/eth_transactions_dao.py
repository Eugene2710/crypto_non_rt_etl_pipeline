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
            "SELECT hash, block_id, blockHash, blockNumber, chainId, from_address, "
            "gas, gasPrice, input, maxFeePerGas, maxPriorityFeePerGas, nonce, r, s, to_address, "
            "transactionIndex, type, v, value, yParity, created_at "
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
                id=single_row[0],
                jsonrpc=single_row[1],
                baseFeePerGas=single_row[2],
                blobGasUsed=single_row[3],
                difficulty=single_row[4],
                excessBlobGas=single_row[5],
                extraData=single_row[6],
                gasLimit=single_row[7],
                gasUsed=single_row[8],
                hash=single_row[9],
                logsBloom=single_row[10],
                miner=single_row[11],
                mixHash=single_row[12],
                nonce=single_row[13],
                number=single_row[14],
                parentBeaconBlockRoot=single_row[15],
                parentHash=single_row[16],
                receiptsRoot=single_row[17],
                sha3Uncles=single_row[18],
                size=single_row[19],
                stateRoot=single_row[20],
                timestamp=single_row[21],
                totalDifficulty=single_row[22],
                transactionRoot=single_row[23],
                withdrawalsRoot=single_row[24],
                created_at=single_row[25],
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
        insert_transactions: str = (
            "INSERT into eth_transactions (hash, block_id, blockHash, blockNumber, chainId, from_address, "
            "gas, gasPrice, input, maxFeePerGas, maxPriorityFeePerGas, nonce, r, s, to_address, "
            "transactionIndex, type, v, value, yParity, created_at) values ("
            ":hash, :block_id, :blockHash, :blockNumber, :chainId, :from_address, "
            ":gas, :gasPrice, :input, :maxFeePerGas, :maxPriorityFeePerGas, :nonce, :r, :s, :to_address, "
            ":transactionIndex, :type, :v, :value, :yParity, :created_at) "
            "RETURNING hash, block_id, blockHash, blockNumber, chainId, from_address, "
            "gas, gasPrice, input, maxFeePerGas, maxPriorityFeePerGas, nonce, r, s, to_address, "
            "transactionIndex, type, v, value, yParity, created_at"
        )
        insert_text_clause: TextClause = text(insert_transactions)

        async with async_connection:
            cursor_result: CursorResult = await async_connection.execute(
                insert_text_clause,
                [
                    {
                        "hash": single_input.hash,
                        "block_id": single_input.block_id,
                        "blockHash": single_input.blockHash,
                        "blockNumber": single_input.blockNumber,
                        "chainId": single_input.chainId,
                        "from_address": single_input.from_address,
                        "gas": single_input.gas,
                        "gasPrice": single_input.gasPrice,
                        "input": single_input.input,
                        "maxFeePerGas": single_input.maxFeePerGas,
                        "maxPriorityFeePerGas": single_input.maxPriorityFeePerGas,
                        "nonce": single_input.nonce,
                        "r": single_input.r,
                        "s": single_input.s,
                        "to_address": single_input.to_address,
                        "transactionIndex": single_input.transactionIndex,
                        "type": single_input.type,
                        "v": single_input.v,
                        "value": single_input.value,
                        "yParity": single_input.yParity,
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
