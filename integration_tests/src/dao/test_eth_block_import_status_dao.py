"""
Prepare
    - Prepare a Test Database -> generate a unique database name for each test
        - pytest fixture has three (but lets focus on two) modes:
            - scope="function" (default)
            - scope="module" (file-level) -> integration test
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
from typing import Sequence

import pytest
from sqlalchemy import (
    Engine,
    create_engine,
    insert,
    text,
    TextClause,
    select,
    CursorResult,
    Select,
    Row,
)
from database_management.tables import eth_block_import_status_table
from src.dao.eth_block_import_status_dao import EthBlockImportStatusDAO
from src.models.database_transfer_objects.eth_block_import_status import (
    EthBlockImportStatusDTO,
)


@pytest.fixture
def db_name() -> str:
    return "test_db_" + str(uuid.uuid4()).replace("-", "_")


@pytest.fixture
def create_and_drop_db_and_tables(db_name: str):
    """
    Responsible for creating a unique database for integration testing
    """
    # SETUP
    default_db_engine: Engine = create_engine("postgresql://localhost:5432/postgres")
    test_db_engine = None
    try:
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            text_clause: TextClause = text(f"CREATE DATABASE {db_name}")
            conn.execute(text_clause)
        default_db_engine.dispose()
        test_db_engine = create_engine(f"postgresql://localhost:5432/{db_name}")
        with test_db_engine.begin() as conn:
            eth_block_import_status_table.create(conn)
        test_db_engine.dispose()
        # yield means, give control to the test
        # ACT + ASSERT HERE after yield
        yield
    except Exception as e:
        raise e
    finally:
        # Ensure all connections to the DB is closed
        # before dropping the database
        test_db_engine.dispose()
        # TEARDOWN
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            text_clause: TextClause = text(f"DROP DATABASE {db_name}")
            conn.execute(text_clause)
        default_db_engine.dispose()


@pytest.fixture
def dummy_rows() -> list[EthBlockImportStatusDTO]:
    return [
        EthBlockImportStatusDTO.create_import_status(block_number=i)
        for i in range(1, 6)
    ]


@pytest.fixture
def dao(db_name: str) -> EthBlockImportStatusDAO:
    return EthBlockImportStatusDAO(f"postgresql+asyncpg://localhost:5432/{db_name}")


@pytest.fixture
def setup_test_read_latest_import_status(
    create_and_drop_db_and_tables,
    db_name: str,
    dummy_rows: list[EthBlockImportStatusDTO],
):
    engine: Engine = create_engine(f"postgresql://localhost:5432/{db_name}")
    try:
        with engine.begin() as conn:
            conn.execute(
                insert(eth_block_import_status_table).values(
                    [single_instance.model_dump() for single_instance in dummy_rows]
                )
            )
        engine.dispose()
        yield
    except Exception as e:
        raise e
    finally:
        engine.dispose()


@pytest.fixture
def setup_test_insert_import_status(
    create_and_drop_db_and_tables,
    db_name: str,
):
    yield

> Clement Goh:
class TestEthBlockImportStatusDAO:
    @pytest.mark.asyncio_cooperative
    async def test_read_latest_import_status(
        self,
        setup_test_read_latest_import_status,
        dao: EthBlockImportStatusDAO,
        dummy_rows: list[EthBlockImportStatusDTO],
    ) -> None:
        try:
            result: EthBlockImportStatusDTO | None = (
                await dao.read_latest_import_status()
            )
            expected: EthBlockImportStatusDTO = [
                dummy_row for dummy_row in dummy_rows if dummy_row.block_number == 5
            ][0]
            assert result == expected
        except Exception as err:
            raise err
        finally:
            await dao._engine.dispose()

    @pytest.mark.asyncio_cooperative
    async def test_insert_import_status(
        self,
        setup_test_insert_import_status,
        dao: EthBlockImportStatusDAO,
    ):
        try:
            # GIVEN an empty table
            import_status_dto: EthBlockImportStatusDTO = (
                EthBlockImportStatusDTO.create_import_status(block_number=1)
            )
            # IF i insert a DTO with block_number 1
            async with dao._engine.begin() as conn:
                await dao.insert_import_status(conn, import_status_dto)

            # THEN, i expect to have 1 DTOs
            async with dao._engine.begin() as conn:
                # Strategy 1: querying with Sqlalchemy Select
                # select_object: Select = select(
                #     eth_block_import_status_table.c.id,
                #     eth_block_import_status_table.c.block_number,
                #     eth_block_import_status_table.c.created_at,
                # )
                # cursor_result: CursorResult = await conn.execute(select_object)

                # Strategy 2: you write your sql query yourself
                text_clause: TextClause = text(
                    "SELECT id, block_number, created_at FROM eth_block_import_status"
                )
                cursor_result: CursorResult = await conn.execute(text_clause)
                rows: Sequence[Row] = cursor_result.fetchall()
            assert rows == [
                (
                    import_status_dto.id,
                    import_status_dto.block_number,
                    import_status_dto.created_at,
                )
            ]
        except Exception as e:
            raise e
        finally:
            await dao._engine.dispose()
`
