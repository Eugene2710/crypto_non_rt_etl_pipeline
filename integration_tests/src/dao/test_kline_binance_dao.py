"""
Integration Tests for KlineBinanceDAO

Tests the DAO's read and insert operations against a real Postgres database.
Each test gets its own isolated database (created and dropped per test) to
prevent interference between tests.

Setup per test:
- A uniquely named Postgres test database is created
- The binance_klines_prices table is created via SQLAlchemy metadata

Teardown per test:
- The test database is dropped regardless of test outcome

Real services used:
- Postgres (localhost:5432)
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Generator, Sequence

import pytest
from sqlalchemy import Engine, create_engine, text, TextClause, CursorResult, Row

from database_management.binance.binance_table import binance_klines_prices_table
from src.dao.kline_binance_dao import KlineBinanceDAO
from src.models.database_transfer_objects.binance.binance_klines import BinanceKlinePriceDTO
from src.models.binance_models.binance_klines import Klines

POSTGRES_HOST: str = "localhost"
POSTGRES_PORT: int = 5432
SYMBOL: str = "BTCUSDC"


@pytest.fixture
def db_name() -> str:
    return "test_db_" + str(uuid.uuid4()).replace("-", "_")


@pytest.fixture
def create_and_drop_db(db_name: str) -> Generator[None, None, None]:
    """
    Creates a uniquely named test database with the binance_klines_prices table,
    then drops the database on teardown regardless of test outcome.
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
def dao(db_name: str) -> KlineBinanceDAO:
    return KlineBinanceDAO(
        f"postgresql+asyncpg://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
    )


@pytest.fixture
def sample_kline_dto() -> BinanceKlinePriceDTO:
    return BinanceKlinePriceDTO(
        symbol=SYMBOL,
        kline_open_time=datetime(2025, 2, 19, 16, 1, 0),
        kline_close_time=datetime(2025, 2, 19, 16, 1, 59),
        open_price=Decimal("66978.59"),
        high_price=Decimal("66993.88"),
        low_price=Decimal("66978.59"),
        close_price=Decimal("66989.55"),
        volume=Decimal("0.31338"),
        quote_asset_volume=Decimal("20992.63"),
        number_of_trades=107,
        taker_buy_base_asset_vol=Decimal("0.21205"),
        taker_buy_quote_asset_vol=Decimal("14204.20"),
        created_at=datetime(2025, 2, 19, 16, 1, 0),
    )


class TestKlineBinanceDAOInsertKline:
    @pytest.mark.asyncio
    async def test_insert_kline_persists_row_to_db(
        self,
        create_and_drop_db: None,
        dao: KlineBinanceDAO,
        db_name: str,
        sample_kline_dto: BinanceKlinePriceDTO,
    ) -> None:
        """
        GIVEN: an empty binance_klines_prices table
        WHEN: a single BinanceKlinePriceDTO is inserted via insert_kline
        THEN: exactly one row exists in the table with the correct symbol and kline_open_time
        """
        try:
            async with dao._engine.begin() as conn:
                await dao.insert_kline(async_connection=conn, input=[sample_kline_dto])

            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                cursor: CursorResult = conn.execute(
                    text("SELECT symbol, kline_open_time FROM binance_klines_prices")
                )
                rows: Sequence[Row] = cursor.fetchall()
            sync_engine.dispose()

            assert len(rows) == 1
            assert rows[0][0] == SYMBOL
            assert rows[0][1] == sample_kline_dto.kline_open_time
        finally:
            await dao._engine.dispose()

    @pytest.mark.asyncio
    async def test_insert_kline_on_conflict_does_not_duplicate(
        self,
        create_and_drop_db: None,
        dao: KlineBinanceDAO,
        db_name: str,
        sample_kline_dto: BinanceKlinePriceDTO,
    ) -> None:
        """
        GIVEN: a binance_klines_prices table with one existing row
        WHEN: the same kline (same primary key: symbol + kline_open_time) is inserted again
        THEN: ON CONFLICT DO NOTHING ensures only one row remains — no error, no duplicate
        """
        try:
            async with dao._engine.begin() as conn:
                await dao.insert_kline(async_connection=conn, input=[sample_kline_dto])
            async with dao._engine.begin() as conn:
                await dao.insert_kline(async_connection=conn, input=[sample_kline_dto])

            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                cursor: CursorResult = conn.execute(
                    text("SELECT COUNT(*) FROM binance_klines_prices")
                )
                count: int = cursor.fetchone()[0]  # type: ignore[index]
            sync_engine.dispose()

            assert count == 1
        finally:
            await dao._engine.dispose()


class TestKlineBinanceDAOReadKline:
    @pytest.mark.asyncio
    async def test_read_kline_returns_correct_dto(
        self,
        create_and_drop_db: None,
        dao: KlineBinanceDAO,
        sample_kline_dto: BinanceKlinePriceDTO,
    ) -> None:
        """
        GIVEN: a binance_klines_prices table with one seeded row
        WHEN: read_kline is called with the matching symbol and kline_open_time
        THEN: the returned DTO matches the seeded row's symbol and kline_open_time
        """
        try:
            async with dao._engine.begin() as conn:
                await dao.insert_kline(async_connection=conn, input=[sample_kline_dto])

            result: BinanceKlinePriceDTO | None = await dao.read_kline(
                symbol=SYMBOL,
                kline_open_time=sample_kline_dto.kline_open_time,
            )

            assert result is not None
            assert result.symbol == sample_kline_dto.symbol
            assert result.kline_open_time == sample_kline_dto.kline_open_time
            assert result.open_price == sample_kline_dto.open_price
            assert result.number_of_trades == sample_kline_dto.number_of_trades
        finally:
            await dao._engine.dispose()

    @pytest.mark.asyncio
    async def test_read_kline_returns_none_when_not_found(
        self,
        create_and_drop_db: None,
        dao: KlineBinanceDAO,
    ) -> None:
        """
        GIVEN: an empty binance_klines_prices table
        WHEN: read_kline is called with a symbol and time that does not exist
        THEN: None is returned
        """
        try:
            result: BinanceKlinePriceDTO | None = await dao.read_kline(
                symbol=SYMBOL,
                kline_open_time=datetime(2025, 1, 1, 0, 0, 0),
            )
            assert result is None
        finally:
            await dao._engine.dispose()


class TestKlineBinanceDAOInsertJsonToMainTable:
    @pytest.mark.asyncio
    async def test_insert_json_to_main_table_persists_klines(
        self,
        create_and_drop_db: None,
        dao: KlineBinanceDAO,
        db_name: str,
    ) -> None:
        """
        GIVEN: an empty binance_klines_prices table and a valid Klines JSON in a BytesIO buffer
        WHEN: insert_json_to_main_table is called with the buffer
        THEN: the klines from the JSON are persisted to the database
        """
        klines: Klines = Klines.from_json(
            symbol=SYMBOL,
            raw_data=[
                [
                    1739980860000,   # open_time (ms) = 2025-02-19 16:01:00 UTC
                    "66978.59",
                    "66993.88",
                    "66978.59",
                    "66989.55",
                    "0.31338",
                    1739980919999,   # close_time (ms) = 2025-02-19 16:01:59 UTC
                    "20992.63",
                    107,
                    "0.21205",
                    "14204.20",
                    "0",
                ],
                [
                    1739980920000,   # open_time (ms) = 2025-02-19 16:02:00 UTC
                    "66989.55",
                    "66995.00",
                    "66989.00",
                    "66990.00",
                    "0.50000",
                    1739980979999,   # close_time (ms) = 2025-02-19 16:02:59 UTC
                    "33494.00",
                    85,
                    "0.30000",
                    "20096.00",
                    "0",
                ],
            ],
        )
        import io
        json_buffer: io.BytesIO = io.BytesIO(klines.model_dump_json().encode("utf-8"))

        try:
            await dao.insert_json_to_main_table(json_buffer)

            sync_engine: Engine = create_engine(
                f"postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{db_name}"
            )
            with sync_engine.begin() as conn:
                cursor: CursorResult = conn.execute(
                    text("SELECT symbol, kline_open_time FROM binance_klines_prices ORDER BY kline_open_time")
                )
                rows: Sequence[Row] = cursor.fetchall()
            sync_engine.dispose()

            assert len(rows) == 2
            assert rows[0][0] == SYMBOL
            assert rows[1][0] == SYMBOL
        finally:
            await dao._engine.dispose()
