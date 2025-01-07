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
from typing import Sequence, Generator

import pytest
from sqlalchemy import (
    Engine,
    create_engine,
    insert,
    text,
    TextClause,
    CursorResult,
    Row,
)

from database_management.tables import eth_block_import_status_table
from src.dao.eth_block_import_status_dao import EthBlockImportStatusDAO
from src.models.database_transfer_objects.eth_block_import_status import (
    EthBlockImportStatusDTO,
)


# lowest level fixture - db_name
@pytest.fixture()
def db_name() -> str:
    return "test_db" + str(uuid.uuid4()).replace("-", "_")


@pytest.fixture()
def create_and_drop_db_and_tables(db_name: str) -> Generator[None, None, None]:
    # create unique database for integration testing
    # setup,
    default_db_engine: Engine = create_engine("postgresql://localhost:5432/postgres")
    test_db_engine: Engine | None = None
    # try, except to catch exceptiopns, finally teardown database engine
    try:
        # setting up of db engine has to be in autocommit mode due to the restrictions imposed by postgres
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            # create database
            text_clause: TextClause = text(f"CREATE DATABASE {db_name}")
            conn.execute(text_clause)

        # note: whenever the engine is not required, dispose it before moving on
        default_db_engine.dispose()
        # create test engine to create table
        test_db_engine = create_engine(f"postgresql://localhost:5432/{db_name}")
        with test_db_engine.begin() as conn:
            eth_block_import_status_table.create(conn)
        test_db_engine.dispose()

        # give control to the test, and no need to return anything
        yield
    except Exception as e:
        raise e
    finally:
        # to play safe, dispose engine bc engine might not have been disposed in the try section
        if test_db_engine:
            test_db_engine.dispose()
        # teardown - drop database
        with default_db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            drop_text_clause: TextClause = text(f"DROP DATABASE {db_name}")
            conn.execute(drop_text_clause)
        default_db_engine.dispose()


@pytest.fixture()
def dummy_rows() -> list[EthBlockImportStatusDTO]:
    return [
        EthBlockImportStatusDTO.create_import_status(block_number=i)
        for i in range(1, 6)
    ]


@pytest.fixture()
def setup_test_read_latest_import_status(
    create_and_drop_db_and_tables: str,
    db_name: str,
    dummy_rows: list[EthBlockImportStatusDTO],
) -> Generator[None, None, None]:
    engine: Engine = create_engine(f"postgresql://localhost:5432/{db_name}")
    try:
        # insert data into table
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


@pytest.fixture()
def setup_test_insert_import_status(
    create_and_drop_db_and_tables,
) -> Generator[None, None, None]:
    # to standardise test to have your own setup fixture - a good to have
    yield


@pytest.fixture()
def dao(db_name: str) -> EthBlockImportStatusDAO:
    # creating a DAO connected to the test database
    return EthBlockImportStatusDAO(
        f"postgresql+asyncpg://localhost:5432/{db_name}"
    )  # note that an async engine is required


class TestETHBlockImportStatusDAO:
    @pytest.mark.asyncio_cooperative
    async def test_read_latest_import_status(
        self,
        setup_test_read_latest_import_status: Generator[None, None, None],
        dao: EthBlockImportStatusDAO,
        dummy_rows: list[EthBlockImportStatusDTO],
    ) -> None:
        # this has to be async bc the read_latest_import_status function is async
        try:
            result: EthBlockImportStatusDTO | None = (
                await dao.read_latest_import_status()
            )
            expected: EthBlockImportStatusDTO = dummy_rows[-1]
            assert result == expected
        except Exception as e:
            raise e
        finally:
            await dao._engine.dispose()

    @pytest.mark.asyncio_cooperative
    async def test_insert_import_status(
        self,
        setup_test_insert_import_status: Generator[None, None, None],
        dao: EthBlockImportStatusDAO,
    ) -> None:
        try:
            # given an empty table
            import_status_dto: EthBlockImportStatusDTO = (
                EthBlockImportStatusDTO.create_import_status(block_number=1)
            )
            # if i insert a DTO with block number 1 -
            async with dao._engine.begin() as conn:
                await dao.insert_import_status(conn, import_status_dto)

            # I should have 1 DTO
            async with dao._engine.begin() as conn:
                # strategy 1: write your sql yourself
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
