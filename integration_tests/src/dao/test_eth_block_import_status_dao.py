"""
Prepare
    - Prepare a Test Database
    - Prepare a Test Table
    - Prepare some sample data to query
Act
    - Read
    - Insert
Assert
    - Assert
Teardown
    - Drop the test table
    - Drop the test database

Heuristics
- Ideally, you should create a separate database for each test
- You don't want your test to interfere with each other;
    - Ideally, your test should be isolated from each other
    - Gives you the benefit of running multiple tests in parallel
    - Give your test database a hash in the name
    - "test_db_" + str(uuid.uuid4()).replace("-", "_")
"""

import uuid

import pytest
from sqlalchemy import Engine, create_engine, insert, text, TextClause

from database_management.tables import eth_block_import_status_table
from src.dao.eth_block_import_status_dao import EthBlockImportStatusDAO
from src.models.database_transfer_objects.eth_block_import_status import (
    EthBlockImportStatusDTO,
)


@pytest.fixture
def create_and_drop_db() -> str:
    """
    Responsible for creating a unique database for integration testing
    """
    # SETUP
    db_name: str = "test_db_" + str(uuid.uuid4()).replace("-", "_")
    default_db_engine: Engine = create_engine("postgresql://localhost:5432/postgres")
    test_db_engine = None
    try:
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            text_clause: TextClause = text(f"CREATE DATABASE {db_name}")
            conn.execute(text_clause)
        test_db_engine = create_engine(f"postgresql://localhost:5432/{db_name}")
        with test_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            eth_block_import_status_table.create(conn)
        # yield means, give control to the test
        # ACT + ASSERT HERE after yield
        yield db_name
    except Exception as e:
        raise e
    finally:
        # Ensure all connections to the DB is closed
        # before dropping the database
        if test_db_engine is not None:
            test_db_engine.dispose()
        default_db_engine.dispose()
        # TEARDOWN
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            text_clause: TextClause = text(f"DROP DATABASE {db_name}")
            conn.execute(text_clause)


@pytest.fixture
def dummy_rows() -> list[EthBlockImportStatusDTO]:
    return [
        EthBlockImportStatusDTO.create_import_status(block_number=i)
        for i in range(1, 6)
    ]


@pytest.fixture
def setup_db(create_and_drop_db, dummy_rows: list[EthBlockImportStatusDTO]) -> str:
    engine: Engine = create_engine(f"postgresql://localhost:5432/{create_and_drop_db}")
    try:
        with engine.begin() as conn:
            conn.execute(
                insert(eth_block_import_status_table).values(
                    [single_instance.model_dump() for single_instance in dummy_rows]
                )
            )
        yield create_and_drop_db
    except Exception as e:
        raise e
    finally:
        engine.dispose()


@pytest.fixture
async def dao(setup_db: str) -> EthBlockImportStatusDAO:
    dao_instance = EthBlockImportStatusDAO(
        f"postgresql+asyncpg://localhost:5432/{setup_db}"
    )
    try:
        yield dao_instance
    finally:
        # Properly dispose of the async engine
        await dao_instance._engine.dispose()


class TestEthBlockImportStatusDAO:
    @pytest.mark.asyncio_cooperative
    async def test_read_latest_import_status(
        self, dao: EthBlockImportStatusDAO, dummy_rows: list[EthBlockImportStatusDTO]
    ) -> None:
        result: EthBlockImportStatusDTO | None = await dao.read_latest_import_status()
        expected: EthBlockImportStatusDTO = [
            dummy_row for dummy_row in dummy_rows if dummy_row.block_number == 5
        ][0]
        assert result == expected
