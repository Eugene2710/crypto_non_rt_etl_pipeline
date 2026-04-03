import os
from asyncio import AbstractEventLoop, new_event_loop
from datetime import datetime

from src.utils.logging_utils import setup_logging
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from dotenv import load_dotenv

from src.dao.binance_s3_import_status_dao import ProviderToS3ImportStatusDAO
from src.models.binance_models.binance_klines import Klines
from src.extractors.binance_klines_extractor import BinanceKlinesExtractor
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.database_transfer_objects.binance_to_s3_import_status import BinanceToS3ImportStatusDTO


logger: logging.Logger = logging.getLogger(__name__)
setup_logging(logger)
"""
BinanceExtractor
Responsible for:
- querying a data provider, binance_klines_extractor
- save the raw data, in JSON, into S3
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
        provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO,
        data_source: str,
        extractor: BinanceKlinesExtractor,
        s3_explorer: S3Explorer,
        connection_string: str,
    ) -> None:
        self._provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = provider_to_s3_import_status_dao
        self._data_source: str = data_source
        self._extractor: BinanceKlinesExtractor = extractor
        self._s3_explorer: S3Explorer = s3_explorer
        self._engine: AsyncEngine = create_async_engine(connection_string)

    async def run(self, symbol: str, kline_open_dt: datetime) -> None:
        # step 1: get latest file modified date from provider_to_s3_import_status_table
        latest_kline_modified_dt: datetime | None = (
            await self._provider_to_s3_import_status_dao.read_latest_kline_import_status(
                table=self._data_source
            )
        )

        # Step 2: extract klines from Binance API between:
        # start_date: date after the latest date (make this customizable nonetheless in start and end date are before
        # the existing dates in database)
        # end_date: current, or other specified date (make this customizable nonetheless)
        # TO_DO - start_time to include option of: time after latest_import_status, manual input
        # TO_DO - end_time to include option of: datetime.utcnow, manual input
        # start_time: datetime = datetime(2026, 3, 30) # TODO: put this as an input param
        # if start_time<=latest_s3_modified_date, data exists in s3 and extraction from binance is not necessary
        # break
        if latest_kline_modified_dt is not None and kline_open_dt <= latest_kline_modified_dt:
            return
        end_time: datetime = datetime.utcnow()
        end_time_str: str = str(end_time)
        end_time_str_formatted: str = end_time_str.replace(" ", "_")

        klines: Klines = await self._extractor.extract(
            symbol=symbol,
            interval="1m",
            limit=500,
            start_time=kline_open_dt,
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
        # step 4: update provider_to_s3_import_status_table
        async with self._engine.begin() as conn:
            binance_to_s3_import_status: BinanceToS3ImportStatusDTO = BinanceToS3ImportStatusDTO(
                data_source=self._data_source,
                symbol=symbol,
                kline_open_time=kline_open_dt,
                created_at=datetime.utcnow()
            )
            await self._provider_to_s3_import_status_dao.insert_latest_import_status(
                import_status=binance_to_s3_import_status
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
    provider_to_s3_import_status_dao: ProviderToS3ImportStatusDAO = ProviderToS3ImportStatusDAO(
        os.getenv("BINANCE_PG_CONNECTION_STRING", "")
    )

    binance_klines_extractor: BinanceKlinesExtractor = BinanceKlinesExtractor()
    binance_to_s3_etl_pipeline: BinanceToS3ETLPipeline = BinanceToS3ETLPipeline(
        provider_to_s3_import_status_dao=provider_to_s3_import_status_dao,
        data_source="binance_klines",
        extractor=binance_klines_extractor,
        s3_explorer=s3_explorer,
        connection_string=os.getenv("BINANCE_PG_CONNECTION_STRING")
    )
    event_loop: AbstractEventLoop = new_event_loop()
    event_loop.run_until_complete(binance_to_s3_etl_pipeline.run(
        symbol="BTCUSDC", kline_open_dt=datetime(2026, 3, 29)
    ))


if __name__ == "__main__":
    run()

