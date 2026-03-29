"""
Integration Tests for BinanceToS3ETLPipeline

Tests the end-to-end flow of the pipeline:
1. Pipeline reads latest import status from DB (s3_import_status table)
2. Extracts klines from Binance API
3. Uploads raw JSON file to S3/MinIO
4. Updates s3_import_status in DB

Setup per test:
- Real Postgres test database (unique name, created and dropped per test)
- Real MinIO test bucket (unique name, created and deleted per test)
- Real Binance API calls

Teardown per test:
- All objects in test bucket deleted, then bucket deleted
- Test database dropped
"""

import uuid
import os
from datetime import datetime

import boto3
import pytest
from dotenv import load_dotenv
from mypy_boto3_s3.client import S3Client
from sqlalchemy import Engine, create_engine, text, CursorResult

from database_management.binance.binance_table import s3_import_status_table
from src.binance_etl_kline_pipeline import BinanceToS3ETLPipeline
from src.dao.s3_binance_import_status_dao import S3ImportStatusDAO
from src.extractors.binance_klines_extractor import BinanceKlinesExtractor
from src.file_explorer.s3_file_explorer import S3Explorer


load_dotenv()

MINIO_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
DATA_SOURCE = "binance_klines"


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
def create_and_drop_db(db_name: str):
    """Creates a unique test database with the s3_import_status table, then drops it on teardown."""
    default_engine: Engine = create_engine(
        f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/postgres"
    )
    test_engine = None
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
            s3_import_status_table.create(conn)
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
def create_and_delete_bucket(bucket_name: str, s3_client):
    """Creates a unique test bucket in MinIO, then deletes all objects and the bucket on teardown."""
    s3_client.create_bucket(Bucket=bucket_name)
    yield
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
def s3_explorer(bucket_name: str) -> S3Explorer:
    return S3Explorer(
        bucket_name=bucket_name,
        endpoint_url=MINIO_ENDPOINT,
        access_key_id=MINIO_ACCESS_KEY,
        secret_access_key=MINIO_SECRET_KEY,
    )


class TestBinanceToS3ETLPipeline:
    @pytest.mark.asyncio
    async def test_pipeline_uploads_json_to_s3_and_updates_import_status(
        self,
        create_and_drop_db,
        create_and_delete_bucket,
        db_name: str,
        s3_import_status_dao: S3ImportStatusDAO,
        s3_explorer: S3Explorer,
        bucket_name: str,
        s3_client,
    ) -> None:
        """
        GIVEN: an empty s3_import_status table and an empty S3 bucket
        WHEN: the pipeline runs
        THEN:
          - a JSON file is uploaded to the S3 bucket under binance/klines/
          - one record is inserted into s3_import_status with the correct data_source
        """
        connection_string = (
            f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        pipeline = BinanceToS3ETLPipeline(
            s3_import_status_dao=s3_import_status_dao,
            data_source=DATA_SOURCE,
            extractor=BinanceKlinesExtractor(),
            s3_explorer=s3_explorer,
            connection_string=connection_string,
        )

        try:
            await pipeline.run()

            # Assert: a JSON file was uploaded under binance/klines/
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            assert "Contents" in response, "Expected at least one file uploaded to S3"
            uploaded_keys = [obj["Key"] for obj in response["Contents"]]
            assert any(
                key.startswith("binance/klines/") and key.endswith(".json")
                for key in uploaded_keys
            ), f"Expected a klines JSON file in S3, found: {uploaded_keys}"

            # Assert: one row inserted into s3_import_status
            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                cursor: CursorResult = conn.execute(
                    text("SELECT data_source, file_modified_date FROM s3_import_status")
                )
                rows = cursor.fetchall()
            sync_engine.dispose()

            assert len(rows) == 1
            assert rows[0][0] == DATA_SOURCE
        finally:
            await s3_import_status_dao._engine.dispose()
            await pipeline._engine.dispose()

    @pytest.mark.asyncio
    async def test_pipeline_skips_when_start_time_is_before_latest_import_status(
        self,
        create_and_drop_db,
        create_and_delete_bucket,
        db_name: str,
        s3_import_status_dao: S3ImportStatusDAO,
        s3_explorer: S3Explorer,
        bucket_name: str,
        s3_client,
    ) -> None:
        """
        GIVEN: s3_import_status already has a record with file_modified_date >= pipeline start_time (2025-02-20)
        WHEN: the pipeline runs
        THEN:
          - no file is uploaded to S3
          - no new record is inserted into s3_import_status
        """
        # Seed the DB with a future import status (beyond pipeline start_time of 2025-02-20)
        existing_import_date = datetime(2025, 3, 1)
        seed_engine: Engine = create_engine(
            f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        with seed_engine.begin() as conn:
            conn.execute(
                s3_import_status_table.insert().values(
                    data_source=DATA_SOURCE,
                    file_modified_date=existing_import_date,
                    created_at=datetime.utcnow(),
                )
            )
        seed_engine.dispose()

        connection_string = (
            f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
        )
        pipeline = BinanceToS3ETLPipeline(
            s3_import_status_dao=s3_import_status_dao,
            data_source=DATA_SOURCE,
            extractor=BinanceKlinesExtractor(),
            s3_explorer=s3_explorer,
            connection_string=connection_string,
        )

        try:
            await pipeline.run()

            # Assert: no files were uploaded to S3
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            assert "Contents" not in response, (
                "Expected no files uploaded to S3 since data already exists for this date range"
            )

            # Assert: still only the original seeded row, no new insert
            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                cursor: CursorResult = conn.execute(
                    text("SELECT data_source, file_modified_date FROM s3_import_status")
                )
                rows = cursor.fetchall()
            sync_engine.dispose()

            assert len(rows) == 1
            assert rows[0][1] == existing_import_date
        finally:
            await s3_import_status_dao._engine.dispose()
            await pipeline._engine.dispose()
