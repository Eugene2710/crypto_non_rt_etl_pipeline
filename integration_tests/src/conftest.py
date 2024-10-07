import uuid

import pytest
from sqlalchemy import (
    Engine,
    create_engine,
    text,
    TextClause,
    Table,
)


@pytest.fixture
def db_name() -> str:
    return "test_db_" + str(uuid.uuid4()).replace("-", "_")


@pytest.fixture
def create_and_drop_db_and_tables(db_name: str, input_tables: list[Table]):
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
            for table in input_tables:
                table.create(conn)
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
