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
            "SELECT block_number, id, jsonrpc, basefeepergas, blobgasused, difficulty, excessblobgas, "
            "extradata, gaslimit, gasused, hash, logsbloom, miner, mixhash, nonce, number, "
            "parentbeaconblockroot, parenthash, receiptsroot, sha3uncles, size, stateroot, "
            "timestamp, totaldifficulty, transactionsroot, withdrawalsroot, created_at "
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
                transactionsRoot=single_row[23],
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
            "INSERT into eth_blocks (block_number, id, jsonrpc, basefeepergas, blobgasused, difficulty, excessblobgas, "
            "extradata, gaslimit, gasused, hash, logsbloom, miner, mixhash, nonce, number, "
            "parentbeaconblockroot, parenthash, receiptsroot, sha3uncles, size, stateroot, "
            "timestamp, totaldifficulty, transactionsroot, withdrawalsroot, created_at) values ("
            ":block_number, :id, :jsonrpc, :basefeepergas, :blobgasused, :difficulty, :excessblobgas, "
            ":extradata, :gaslimit, :gasused, :hash, :logsbloom, :miner, :mixhash, :nonce, :number, "
            ":parentbeaconblockroot, :parenthash, :receiptsroot, :sha3uncles, :size, :stateroot, "
            ":timestamp, :totaldifficulty, :transactionsroot, :withdrawalsroot, :created_at) "
            "RETURNING block_number, id, jsonrpc, basefeepergas, blobgasused, difficulty, excessblobgas, "
            "extradata, gaslimit, gasused, hash, logsbloom, miner, mixhash, nonce, number, "
            "parentbeaconblockroot, parenthash, receiptsroot, sha3uncles, size, stateroot, "
            "timestamp, totaldifficulty, transactionsroot, withdrawalsroot, created_at"
        )
        insert_text_clause: TextClause = text(insert_block)

        unique_ids = [current_input.id for current_input in input]
        print("UNIQUE IDS")
        print(unique_ids)

        cursor_result: CursorResult = await async_connection.execute(
            insert_text_clause,
            [
                {
                    "block_number": single_input.block_number,
                    "id": single_input.id,
                    "jsonrpc": single_input.jsonrpc,
                    "basefeepergas": single_input.baseFeePerGas,
                    "blobgasused": single_input.blobGasUsed,
                    "difficulty": single_input.difficulty,
                    "excessblobgas": single_input.excessBlobGas,
                    "extradata": single_input.extraData,
                    "gaslimit": single_input.gasLimit,
                    "gasused": single_input.gasUsed,
                    "hash": single_input.hash,
                    "logsbloom": single_input.logsBloom,
                    "miner": single_input.miner,
                    "mixhash": single_input.mixHash,
                    "nonce": single_input.nonce,
                    "number": single_input.number,
                    "parentbeaconblockroot": single_input.parentBeaconBlockRoot,
                    "parenthash": single_input.parentHash,
                    "receiptsroot": single_input.receiptsRoot,
                    "sha3uncles": single_input.sha3Uncles,
                    "size": single_input.size,
                    "stateroot": single_input.stateRoot,
                    "timestamp": single_input.timestamp,
                    "totaldifficulty": single_input.totalDifficulty,
                    "transactionsroot": single_input.transactionsRoot,
                    "withdrawalsroot": single_input.withdrawalsRoot,
                    "created_at": single_input.created_at,
                }
                for single_input in input
            ],
        )