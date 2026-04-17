"""
Integration Tests for S3ETLPipeline (S3 → Postgres)

Tests the end-to-end flow of ingesting klines JSON files from S3 into Postgres:
1. Pipeline reads the latest file_modified_date from s3_to_db_import_status
2. Lists S3 files whose LastModified exceeds that date
3. For each file, downloads it and inserts klines into binance_klines_prices
4. Updates s3_to_db_import_status with the latest file_modified_date from the batch

Setup per test:
- A uniquely named Postgres test database with two tables:
    - binance_klines_prices (the main klines data table)
    - s3_to_db_import_status (tracks which S3 files have been ingested)
- A uniquely named MinIO test bucket

Teardown per test:
- All objects in the test bucket are deleted, then the bucket is deleted
- The test database is dropped regardless of test outcome

Real services used:
- Postgres (localhost:5432)
- MinIO (localhost:9000)
"""

import uuid
import os
from datetime import datetime
from typing import Generator, Sequence

import boto3
import pytest
from dotenv import load_dotenv
from mypy_boto3_s3.client import S3Client
from sqlalchemy import Engine, create_engine, text, CursorResult, Row

from database_management.binance.binance_table import (
    binance_klines_prices_table,
    s3_to_db_import_status_table,
)
from src.dao.kline_binance_dao import KlineBinanceDAO
from src.dao.s3_binance_import_status_dao import S3ImportStatusDAO
from src.file_explorer.s3_file_explorer import S3Explorer
from src.models.binance_models.binance_klines import Klines
from src.s3_to_pg_etl_pipeline import S3ETLPipeline


load_dotenv()

MINIO_ENDPOINT: str = os.getenv("AWS_S3_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY_ID", "")
MINIO_SECRET_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
POSTGRES_HOST: str = "localhost"
POSTGRES_PORT: int = 5432
DATA_SOURCE: str = "binance_klines"
S3_PREFIX: str = "binance/klines"
SYMBOL: str = "BTCUSDC"


@pytest.fixture
def db_name() -> str:
    return "test_db_" + str(uuid.uuid4()).replace("-", "_")


@pytest.fixture
def bucket_name() -> str:
    return "test-bucket-" + str(uuid.uuid4()).replace("-", "")[:8]


@pytest.fixture
def s3_client() -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


@pytest.fixture
def create_and_drop_db(db_name: str) -> Generator[None, None, None]:
    """
    Creates a uniquely named test database with binance_klines_prices and
    s3_to_db_import_status tables, then drops it on teardown regardless of test outcome.
    """
    default_engine: Engine = create_engine(
        f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/postgres"
    )
    test_engine: Engine | None = None
    try:
        with default_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(text(f"CREATE DATABASE {db_name}"))
        default_engine.dispose()
        test_engine = create_engine(
            f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        with test_engine.begin() as conn:
            binance_klines_prices_table.create(conn)
            s3_to_db_import_status_table.create(conn)
        test_engine.dispose()
        yield
    except Exception as e:
        raise e
    finally:
        if test_engine:
            test_engine.dispose()
        with default_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(text(f"DROP DATABASE {db_name}"))
        default_engine.dispose()


@pytest.fixture
def create_and_delete_bucket(
    bucket_name: str, s3_client: S3Client
) -> Generator[None, None, None]:
    """
    Creates a uniquely named test bucket in MinIO, then deletes all objects
    and the bucket on teardown regardless of test outcome.
    """
    s3_client.create_bucket(Bucket=bucket_name)
    try:
        yield
    finally:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=bucket_name)


@pytest.fixture
def s3_import_status_dao(db_name: str) -> S3ImportStatusDAO:
    return S3ImportStatusDAO(
        f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
    )


@pytest.fixture
def kline_binance_dao(db_name: str) -> KlineBinanceDAO:
    return KlineBinanceDAO(
        f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
    )


@pytest.fixture
def s3_explorer(bucket_name: str) -> S3Explorer:
    return S3Explorer(
        bucket_name=bucket_name,
        endpoint_url=MINIO_ENDPOINT,
        access_key_id=MINIO_ACCESS_KEY,
        secret_access_key=MINIO_SECRET_KEY,
    )


def _build_klines_json_bytes() -> bytes:
    """
    Returns a valid Klines JSON payload as UTF-8 bytes containing two BTCUSDC kline entries
    for 2025-02-19 16:01 and 16:02 UTC.
    """
    klines: Klines = Klines.from_json(
        symbol=SYMBOL,
        raw_data=[
            [
                1739980860000,  # open_time (ms) = 2025-02-19 16:01:00 UTC
                "66978.59",
                "66993.88",
                "66978.59",
                "66989.55",
                "0.31338",
                1739980919999,  # close_time (ms) = 2025-02-19 16:01:59 UTC
                "20992.63",
                107,
                "0.21205",
                "14204.20",
                "0",
            ],
            [
                1739980920000,  # open_time (ms) = 2025-02-19 16:02:00 UTC
                "66989.55",
                "66995.00",
                "66989.00",
                "66990.00",
                "0.50000",
                1739980979999,  # close_time (ms) = 2025-02-19 16:02:59 UTC
                "33494.00",
                85,
                "0.30000",
                "20096.00",
                "0",
            ],
        ],
    )
    return klines.model_dump_json().encode("utf-8")


class TestS3ETLPipelineRun:
    @pytest.mark.asyncio
    async def test_pipeline_ingests_s3_json_into_db(
        self,
        create_and_drop_db: None,
        create_and_delete_bucket: None,
        db_name: str,
        s3_import_status_dao: S3ImportStatusDAO,
        kline_binance_dao: KlineBinanceDAO,
        s3_explorer: S3Explorer,
        bucket_name: str,
        s3_client: S3Client,
    ) -> None:
        """
        GIVEN: empty binance_klines_prices and s3_to_db_import_status tables,
               and one klines JSON file uploaded to the test S3 bucket
        WHEN: the pipeline runs
        THEN:
          - two kline rows are inserted into binance_klines_prices (one per kline in the JSON)
          - one record is inserted into s3_to_db_import_status with the correct data_source
        """
        # Arrange: upload a klines JSON file to the test S3 bucket
        s3_key: str = f"{S3_PREFIX}/BTCUSDC_2025-02-19.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=_build_klines_json_bytes(),
        )

        connection_string: str = (
            f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        pipeline: S3ETLPipeline = S3ETLPipeline(
            s3_import_status_dao=s3_import_status_dao,
            data_source=DATA_SOURCE,
            s3_explorer=s3_explorer,
            s3_prefix_path=S3_PREFIX,
            connection_string=connection_string,
            dao=kline_binance_dao,
        )

        try:
            await pipeline.run()

            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                kline_cursor: CursorResult = conn.execute(
                    text(
                        "SELECT symbol, kline_open_time FROM binance_klines_prices "
                        "ORDER BY kline_open_time"
                    )
                )
                kline_rows: Sequence[Row] = kline_cursor.fetchall()

                status_cursor: CursorResult = conn.execute(
                    text(
                        "SELECT data_source, file_modified_date "
                        "FROM s3_to_db_import_status"
                    )
                )
                status_rows: Sequence[Row] = status_cursor.fetchall()
            sync_engine.dispose()

            assert len(kline_rows) == 2, (
                f"Expected 2 kline rows persisted, got {len(kline_rows)}"
            )
            assert kline_rows[0][0] == SYMBOL
            assert kline_rows[1][0] == SYMBOL

            assert len(status_rows) == 1, (
                f"Expected 1 import status row, got {len(status_rows)}"
            )
            assert status_rows[0][0] == DATA_SOURCE
        finally:
            await s3_import_status_dao._engine.dispose()
            await kline_binance_dao._engine.dispose()
            await pipeline._engine.dispose()

    @pytest.mark.asyncio
    async def test_pipeline_skips_files_already_ingested(
        self,
        create_and_drop_db: None,
        create_and_delete_bucket: None,
        db_name: str,
        s3_import_status_dao: S3ImportStatusDAO,
        kline_binance_dao: KlineBinanceDAO,
        s3_explorer: S3Explorer,
        bucket_name: str,
        s3_client: S3Client,
    ) -> None:
        """
        GIVEN: s3_to_db_import_status has a record with file_modified_date = 2100-01-01
               (far in the future), and one klines JSON file exists in S3 (LastModified ~ now)
        WHEN: the pipeline runs
        THEN:
          - no rows are inserted into binance_klines_prices (the S3 file's LastModified
            is before the seeded date, so the pipeline skips it)
          - s3_to_db_import_status still contains only the original seeded row
        """
        # Arrange: seed a future import status so no S3 files pass the date filter
        future_date: datetime = datetime(2100, 1, 1)
        seed_engine: Engine = create_engine(
            f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        with seed_engine.begin() as conn:
            conn.execute(
                s3_to_db_import_status_table.insert().values(
                    data_source=DATA_SOURCE,
                    file_modified_date=future_date,
                    created_at=datetime.utcnow(),
                )
            )
        seed_engine.dispose()

        # Arrange: upload a klines JSON file to S3 (LastModified will be ~now, i.e. 2026)
        s3_key: str = f"{S3_PREFIX}/BTCUSDC_2025-02-19.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=_build_klines_json_bytes(),
        )

        connection_string: str = (
            f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        pipeline: S3ETLPipeline = S3ETLPipeline(
            s3_import_status_dao=s3_import_status_dao,
            data_source=DATA_SOURCE,
            s3_explorer=s3_explorer,
            s3_prefix_path=S3_PREFIX,
            connection_string=connection_string,
            dao=kline_binance_dao,
        )

        try:
            await pipeline.run()

            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                kline_cursor: CursorResult = conn.execute(
                    text("SELECT COUNT(*) FROM binance_klines_prices")
                )
                kline_count: int = kline_cursor.fetchone()[0]  # type: ignore[index]

                status_cursor: CursorResult = conn.execute(
                    text(
                        "SELECT file_modified_date FROM s3_to_db_import_status "
                        "WHERE data_source = :ds"
                    ),
                    {"ds": DATA_SOURCE},
                )
                status_rows: Sequence[Row] = status_cursor.fetchall()
            sync_engine.dispose()

            assert kline_count == 0, (
                f"Expected 0 kline rows (file already ingested), got {kline_count}"
            )
            assert len(status_rows) == 1
            assert status_rows[0][0] == future_date
        finally:
            await s3_import_status_dao._engine.dispose()
            await kline_binance_dao._engine.dispose()
            await pipeline._engine.dispose()
