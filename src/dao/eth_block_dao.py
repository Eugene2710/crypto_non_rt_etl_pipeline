import os

import retry
from sqlalchemy import (
    TextClause,
    text,
    CursorResult,
    Row,
    Table,
    schema,
    insert,
    select,
    Column,
)
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from src.models.database_transfer_objects.eth_blocks import EthBlockDTO
from database_management.tables import eth_block_table, metadata
from src.file_explorer.s3_file_explorer import S3Explorer

from datetime import datetime
from dotenv import load_dotenv
import logging
import asyncio
import io

from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logger)


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
        self._table: Table = eth_block_table

    @retry.retry(
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def read_block_by_block_number(self, block_number: str) -> EthBlockDTO | None:
        """
        TO-DO: Refactor to use sqlalchemy.Table instead so that typo mistakes can be avoided
        """
        query_block_by_id: str = (
            "SELECT block_number, id, jsonrpc, basefeepergas, blobgasused, difficulty, excessblobgas, "
            "extradata, gaslimit, gasused, hash, logsbloom, miner, mixhash, nonce, number, "
            "parentbeaconblockroot, parenthash, receiptsroot, sha3uncles, size, stateroot, "
            "timestamp, totaldifficulty, transactionsroot, withdrawalsroot, created_at "
            "FROM eth_blocks WHERE block_number = :block_number limit 1"
        )
        query_text_clause: TextClause = text(query_block_by_id)

        async with self._engine.begin() as async_conn:
            cursor_result: CursorResult = await async_conn.execute(
                query_text_clause, {"block_number": block_number}
            )

        single_row: Row | None = cursor_result.fetchone()
        if not single_row:
            return None
        else:
            eth_block_dto: EthBlockDTO = EthBlockDTO(
                block_number=block_number,
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
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        max_delay=0.3375,
        backoff=1.5,
        jitter=(-0.01, 0.01),
    )
    async def insert_blocks(
        self, async_connection: AsyncConnection, input: list[EthBlockDTO]
    ) -> None:
        if not input:
            print("insert_blocks: No input. Exiting")
            return
        insert_block: str = (
            "INSERT into eth_blocks (block_number, id, jsonrpc, basefeepergas, blobgasused, difficulty, excessblobgas, "
            "extradata, gaslimit, gasused, hash, logsbloom, miner, mixhash, nonce, number, "
            "parentbeaconblockroot, parenthash, receiptsroot, sha3uncles, size, stateroot, "
            "timestamp, totaldifficulty, transactionsroot, withdrawalsroot, created_at) values ("
            ":block_number, :id, :jsonrpc, :basefeepergas, :blobgasused, :difficulty, :excessblobgas, "
            ":extradata, :gaslimit, :gasused, :hash, :logsbloom, :miner, :mixhash, :nonce, :number, "
            ":parentbeaconblockroot, :parenthash, :receiptsroot, :sha3uncles, :size, :stateroot, "
            ":timestamp, :totaldifficulty, :transactionsroot, :withdrawalsroot, :created_at) ON CONFLICT DO NOTHING "
            "RETURNING block_number, id, jsonrpc, basefeepergas, blobgasused, difficulty, excessblobgas, "
            "extradata, gaslimit, gasused, hash, logsbloom, miner, mixhash, nonce, number, "
            "parentbeaconblockroot, parenthash, receiptsroot, sha3uncles, size, stateroot, "
            "timestamp, totaldifficulty, transactionsroot, withdrawalsroot, created_at"
        )
        insert_text_clause: TextClause = text(insert_block)

        unique_ids = [current_input.id for current_input in input]
        print("UNIQUE IDS")
        print(unique_ids)

        _: CursorResult = await async_connection.execute(
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

    @retry.retry(
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        jitter=(-0.01, 0.01),
        backoff=1.5,
    )
    async def _create_temp_table(
        self, conn: AsyncConnection, temp_table: Table
    ) -> None:
        """
        creates a temporary table

        temporary table has the current datetime (resolution set to microseconds to prevent duplications) as the suffix
        - this ensures two parallel insertions won't share the same temporary table

        """
        create_table_ddl = schema.CreateTable(temp_table)
        try:
            await conn.execute(create_table_ddl)
        except SQLAlchemyError:
            logger.exception("Unable to create temporary table")

    @retry.retry(
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        jitter=(-0.01, 0.01),
        backoff=1.5,
    )
    async def _copy_to_temporary_table(
        self, conn: AsyncConnection, temp_table: Table, csv_buffer: io.BytesIO
    ) -> None:
        """
        copy a CSV io.BytesIO into the temporary table
        """
        # for safety, seek(0) to the start of buffer
        csv_buffer.seek(0)

        dbapi_pooled_conn = await conn.get_raw_connection()
        dbapi_conn = dbapi_pooled_conn.driver_connection
        try:
            await dbapi_conn.copy_to_table(  # type: ignore[union-attr]
                temp_table.name, source=csv_buffer, format="csv", header=True
            )
        except SQLAlchemyError:
            logger.exception("Unable to copy to temporary table")
            raise

    @retry.retry(
        exceptions=(OperationalError, DisconnectionError),
        tries=5,
        delay=0.1,
        jitter=(-0.01, 0.01),
        backoff=1.5,
    )
    async def _insert_from_temp_to_main_table(
        self, conn: AsyncConnection, temp_table: Table
    ) -> None:
        """
        Insert from temporary table into main table
        """
        # akin to INSERT INTO eth_blocks VALUES (
        #     SELECT * FROM temp_eth_blocks_20250106000000000000
        # )
        insert_command = insert(self._table).from_select(
            self._table.columns.keys(), select(temp_table)
        )
        try:
            await conn.execute(insert_command)
        except SQLAlchemyError:
            logger.exception("Unable to insert from temporary table to main table")
            raise

    async def insert_csv_to_main_table(self, csv_buffer: io.BytesIO) -> None:
        """
        inserts a csv (io.BytesIO) into the main table

        runs as a single transaction; all or nothing
        1. _create_temp_table
        2. _copy_to_temporary_table
        3. _insert_from_temp_to_main_table
        """
        async with self._engine.begin() as conn:
            temp_table_name: str = (
                f"temp_{self._table.name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
            )
            temp_table = Table(
                temp_table_name,
                metadata,
                *[
                    Column(
                        col.name,
                        col.type,
                        *col.constraints,
                        primary_key=col.primary_key,
                    )
                    for col in self._table.columns
                ],
                prefixes=[
                    "TEMPORARY"
                ],  # temporarry because this will be deleted after the transaction is completed; saves space
            )

            await self._create_temp_table(conn, temp_table)
            await self._copy_to_temporary_table(conn, temp_table, csv_buffer)
            await self._insert_from_temp_to_main_table(conn, temp_table)


if __name__ == "__main__":
    load_dotenv()
    event_loop = asyncio.new_event_loop()

    dao = EthBlockDAO(
        connection_string=os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", "")
    )

    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )

    for file_info in s3_explorer.list_files(
        "chainstack/eth_blocks", last_modified_date=datetime.utcnow()
    ):
        bytes_io: io.BytesIO = s3_explorer.download_to_buffer(file_info.file_path)
        event_loop.run_until_complete(dao.insert_csv_to_main_table(bytes_io))
