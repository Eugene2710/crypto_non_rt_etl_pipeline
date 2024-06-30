from typing import Sequence

import retry
from sqlalchemy import TextClause, text, CursorResult, Row
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection

from src.models.database_transfer_objects.eth_blocks import EthBlockDTO


class EthBlockDAO:
    """
    DAO responsible for CRUD operations into quick_node.eth_transactions table

    Responsible for
    - read single block by id
    - inserting multiple blocks into table

    Table: quick_node.eth_blocks table
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
    async def read_block_by_id(self, block_id: str) -> EthBlockDTO | None:
        query_block_by_id: str = (
            "SELECT id, jsonrpc, baseFeePerGas, blobGasUsed, difficulty, excessBlobGas, "
            "extraData, gasLimit, gasUsed, hash, logsBloom, miner, mixHash, nonce, number, "
            "parentBeaconBlockRoot, parentHash, receiptsRoot, sha3Uncles, size, stateRoot, "
            "timestamp, totalDifficulty, transactionsRoot, withdrawalsRoot, created_at "
            "FROM eth_blocks WHERE id = :id limit 1"
        )
        query_text_clause: TextClause = text(query_block_by_id)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(
                query_text_clause, {"id": block_id}
            )

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            eth_block_dto: EthBlockDTO = EthBlockDTO(
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
            return eth_block_dto

    @retry.retry(
        exceptions=SQLAlchemyError,
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def insert_blocks(
        self, async_connection: AsyncConnection, input: list[EthBlockDTO]
    ) -> None:
        insert_block: str = (
            "INSERT into eth_blocks (id, jsonrpc, baseFeePerGas, blobGasUsed, difficulty, excessBlobGas, "
            "extraData, gasLimit, gasUsed, hash, logsBloom, miner, mixHash, nonce, number, "
            "parentBeaconBlockRoot, parentHash, receiptsRoot, sha3Uncles, size, stateRoot, "
            "timestamp, totalDifficulty, transactionsRoot, withdrawalsRoot, created_at) values ("
            ":id, :jsonrpc, :baseFeePerGas, :blobGasUsed, :difficulty, :excessBlobGas, "
            ":extraData, :gasLimit, :gasUsed, :hash, :logsBloom, :miner, :mixHash, :nonce, :number, "
            ":parentBeaconBlockRoot, :parentHash, :receiptsRoot, :sha3Uncles, :size, :stateRoot, "
            ":timestamp, :totalDifficulty, :transactionsRoot, :withdrawalsRoot, :created_at) "
            "RETURNING id, jsonrpc, baseFeePerGas, blobGasUsed, difficulty, excessBlobGas, "
            "extraData, gasLimit, gasUsed, hash, logsBloom, miner, mixHash, nonce, number, "
            "parentBeaconBlockRoot, parentHash, receiptsRoot, sha3Uncles, size, stateRoot, "
            "timestamp, totalDifficulty, transactionsRoot, withdrawalsRoot, created_at"
        )
        insert_text_clause: TextClause = text(insert_block)

        async with async_connection:
            cursor_result: CursorResult = await async_connection.execute(
                insert_text_clause,
                [
                    {
                        "id": single_input.id,
                        "jsonrpc": single_input.jsonrpc,
                        "baseFeePerGas": single_input.baseFeePerGas,
                        "blobGasUsed": single_input.blobGasUsed,
                        "difficulty": single_input.difficulty,
                        "excessBlobGas": single_input.excessBlobGas,
                        "extraData": single_input.extraData,
                        "gasLimit": single_input.gasLimit,
                        "gasUsed": single_input.gasUsed,
                        "hash": single_input.hash,
                        "logsBloom": single_input.logsBloom,
                        "miner": single_input.miner,
                        "mixHash": single_input.mixHash,
                        "nonce": single_input.nonce,
                        "number": single_input.number,
                        "parentBeaconBlockRoot": single_input.parentBeaconBlockRoot,
                        "parentHash": single_input.parentHash,
                        "receiptsRoot": single_input.receiptsRoot,
                        "sha3Uncles": single_input.sha3Uncles,
                        "size": single_input.size,
                        "stateRoot": single_input.stateRoot,
                        "timestamp": single_input.timestamp,
                        "totalDifficulty": single_input.totalDifficulty,
                        "transactionsRoot": single_input.transactionsRoot,
                        "withdrawalsRoot": single_input.withdrawalsRoot,
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
