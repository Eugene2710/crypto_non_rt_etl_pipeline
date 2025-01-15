import io
import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from src.dao.eth_block_dao import EthBlockDAO
from src.dao.s3_import_status_dao import S3ImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.s3_import_status import S3ImportStatusDTO
from src.models.file_info.file_info import FileInfo


class S3ETLPipeline:
    """
    Responsible for backfillng S3 csv files into DB

    TODO: Create and abstract base class for DAO, then use the abstract base class here
    This will allow the support for all DAOs
    """

    def __init__(
        self,
        s3_import_status_dao: S3ImportStatusDAO,
        data_source: str,
        s3_explorer: S3Explorer,
        s3_prefix_path: str,
        connection_string: str,
        block_dao: EthBlockDAO,
    ) -> None:
        self._s3_import_status_dao: S3ImportStatusDAO = s3_import_status_dao
        self._data_source: str = data_source
        self._s3_explorer: S3Explorer = s3_explorer
        self._s3_prefix_path: str = s3_prefix_path
        self._engine: AsyncEngine = create_async_engine(connection_string)
        self._block_dao: EthBlockDAO = block_dao

    async def run(self) -> None:
        """
        Step 1: Get latest file modified date from s3_import_status table
        Step 2: Get all files whole modified date is after the s3_import_status modified date
        Step 3: For each file, save it into DB with DAO
        Step 4: Insert file latest modified date into DB
        """
        latest_modified_date: datetime | None = (
            await self._s3_import_status_dao.read_latest_import_status(
                self._data_source
            )
        )
        # this is for listing files after import_status_modified_date - for S3 specific
        default_modified_date: datetime = latest_modified_date or datetime(
            year=1970, month=1, day=1
        )

        s3_file_info: Generator[FileInfo, None, None] = self._s3_explorer.list_files(
            self._s3_prefix_path, default_modified_date
        )
        # this is for the current batch's latest modified date - for batch specific
        current_batch_latest_modified_date: datetime = datetime(
            year=1970, month=1, day=1
        )

        # shouldn't there be a part to only download files which are after the latest file modified date
        async with self._engine.begin() as conn:
            for file_info in s3_file_info:
                # get file path of each file in s3
                csv_bytes: io.BytesIO = self._s3_explorer.download_to_buffer(
                    file_info.file_path
                )
                await self._block_dao.insert_csv_to_main_table(csv_bytes)
                current_batch_latest_modified_date = max(
                    current_batch_latest_modified_date, file_info.modified_date
                )

            updated_s3_import_status: S3ImportStatusDTO = S3ImportStatusDTO(
                data_source=self._data_source,
                file_modified_date=current_batch_latest_modified_date,
                created_at=datetime.utcnow(),
            )
            await self._s3_import_status_dao.insert_latest_import_status(
                updated_s3_import_status, conn
            )


def run():
    """
    Responsible for running the S3ETLPipeline
    """
    load_dotenv()
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    s3_import_status_dao: S3ImportStatusDAO = S3ImportStatusDAO(
        os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", "")
    )
    eth_block_dao: EthBlockDAO = EthBlockDAO(
        os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", "")
    )
    s3_etl_pipeline: S3ETLPipeline = S3ETLPipeline(
        s3_import_status_dao=s3_import_status_dao,
        data_source="chainstack_eth_blocks",
        s3_explorer=s3_explorer,
        s3_prefix_path="chainstack/eth_blocks",
        connection_string=os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", ""),
        block_dao=eth_block_dao,
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(s3_etl_pipeline.run())


if __name__ == "__main__":
    run()
