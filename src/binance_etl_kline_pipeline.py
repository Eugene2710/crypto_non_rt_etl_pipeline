import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime

from src.utils.logging_utils import setup_logging
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from dotenv import load_dotenv

from src.dao.s3_binance_import_status_dao import S3ImportStatusDAO
from src.models.binance_models.binance_klines import Klines
from src.extractors.binance_klines_extractor import BinanceKlinesExtractor
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.s3_import_status import S3ImportStatusDTO


logger: logging.Logger = logging.getLogger(__name__)
setup_logging(logger)
"""
BinanceExtractor
Responsible for:
- querying a data provider, binance_klines_extractor
- save the raw data, in JSON, into S3
"""

"""
TO_DOs
Q: how do i save the data into S3
A: refer to S3 file
"""


class BinanceToS3ETLPipeline:
    """
    Responsible for:
    1. Querying a data provider, e.g BinanceKlineExtractor
    2. Save the data as a JSON into S3
    3. Update the import status which contains the last timestamp used to query data from Binance
    """

    def __init__(
        self,
        s3_import_status_dao: S3ImportStatusDAO,
        data_source: str,
        extractor: BinanceKlinesExtractor,
        s3_explorer: S3Explorer,
        connection_string: str,
    ) -> None:
        self._s3_import_status_dao: S3ImportStatusDAO = s3_import_status_dao
        self._data_source: str = data_source
        self._extractor: BinanceKlinesExtractor = extractor
        self._s3_explorer: S3Explorer = s3_explorer
        self._engine: AsyncEngine = create_async_engine(connection_string)

    async def run(self) -> None:
        # step 1: get latest file modified date from s3_import_status table
        latest_s3_modified_date: datetime | None = (
            await self._s3_import_status_dao.read_latest_import_status(
                data_source=self._data_source
            )
        )

        # Step 2: extract klines from Binance API between:
        # start_date: date after the latest date (make this customizable nonetheless in start and end date are before
        # the existing dates in database)
        # end_date: current, or other specified date (make this customizable nonetheless)
        # TO_DO - start_time to include option of: time after latest_import_status, manual input
        # TO_DO - end_time to include option of: datetime.utcnow, manual input
        start_time: datetime = datetime(2026, 3, 30) # TODO: put this as an input param
        # if start_time<=latest_s3_modified_date, data exists in s3 and extraction from binance is not necessary
        # break
        if latest_s3_modified_date is not None and start_time <= latest_s3_modified_date:
            return
        end_time: datetime = datetime.utcnow()
        end_time_str: str = str(end_time)
        end_time_str_formatted: str = end_time_str.replace(" ", "_")

        klines: Klines = await self._extractor.extract(
            symbol="BTCUSDC",
            interval="1m",
            limit=500,
            start_time=start_time,
            end_time=end_time,
        )
        with open(
            f"klines_{end_time_str_formatted}.json", "w", encoding="utf-8"
        ) as json_file:
            json_str = klines.model_dump_json(indent=2)
            json_file.write(json_str)

        # step 3: save the files into S3 using S3Explorer.upload_files method
        self._s3_explorer.upload_file(
            local_file_path=f"klines_{end_time_str_formatted}.json",
            s3_path=f"binance/klines/2026/{end_time_str_formatted}.json",
        )
        # step 4: update s3_import_Status_table
        async with self._engine.begin() as conn:
            updated_s3_import_status: S3ImportStatusDTO = S3ImportStatusDTO(
                data_source=self._data_source,
                file_modified_date=max(end_time, datetime.utcnow()),
                created_at=datetime.utcnow()
            )
            await self._s3_import_status_dao.insert_latest_import_status(
                updated_s3_import_status, conn
            )



def run():
    """
    Responsible for running the BinanceToS3ETLPipeline
    """
    load_dotenv()
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    s3_import_status_dao: S3ImportStatusDAO = S3ImportStatusDAO(
        os.getenv("BINANCE_PG_CONNECTION_STRING", "")
    )
    binance_klines_extractor: BinanceKlinesExtractor = BinanceKlinesExtractor()
    binance_to_s3_etl_pipeline: BinanceToS3ETLPipeline = BinanceToS3ETLPipeline(
        s3_import_status_dao=s3_import_status_dao,
        data_source="binance_klines", #create this data source?
        extractor=binance_klines_extractor,
        s3_explorer=s3_explorer,
        connection_string=os.getenv("BINANCE_PG_CONNECTION_STRING") #create new pg database
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(binance_to_s3_etl_pipeline.run())


if __name__ == "__main__":
    run()

