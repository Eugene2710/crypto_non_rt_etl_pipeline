import uuid
from datetime import datetime

import pytest
from sqlalchemy import (
    Engine,
    create_engine,
    insert,
    Table,
)

from src.dao.eth_withdrawals_dao import EthWithdrawalDAO
from src.models.database_transfer_objects.eth_blocks import EthBlockDTO
from src.models.database_transfer_objects.eth_withdrawals import EthWithdrawalDTO
from database_management.tables import eth_withdrawals_table, eth_block_table


@pytest.fixture
def input_tables() -> list[Table]:
    return [eth_block_table, eth_withdrawals_table]


@pytest.fixture
def dummy_blocks() -> list[EthBlockDTO]:
    return [
        EthBlockDTO(
            block_number="1",
            id=1,
            jsonrpc="jsonrpc",
            baseFeePerGas=None,
            blobGasUsed=None,
            difficulty="hard",
            excessBlobGas=None,
            extraData="someData",
            gasLimit="100",
            gasUsed="100",
            hash=None,
            logsBloom="bloomFilter",
            miner="0x123",
            mixHash="0x123",
            nonce="nonce",
            number="0x123",
            parentBeaconBlockRoot=None,
            parentHash="0x123",
            receiptsRoot="123",
            sha3Uncles="123",
            size="123",
            stateRoot="123",
            timestamp="123",
            totalDifficulty="123",
            transactionsRoot="123",
            withdrawalsRoot=None,
            created_at=datetime(year=2024, month=10, day=7),
        )
    ]


@pytest.fixture
def dummy_withdrawal_rows() -> list[EthWithdrawalDTO]:
    return [
        EthWithdrawalDTO(
            id=uuid.uuid4(),
            block_number="1",
            address="0x123",
            amount="1",
            index="1",
            validatorIndex="123",
            created_at=datetime(year=2024, month=10, day=7),
        )
    ]


@pytest.fixture
def dao(db_name: str) -> EthWithdrawalDAO:
    return EthWithdrawalDAO(f"postgresql+asyncpg://localhost:5432/{db_name}")


@pytest.fixture
def setup_test_read_block_by_block_number(
    create_and_drop_db_and_tables,
    db_name: str,
    dummy_blocks: list[EthBlockDTO],
    dummy_withdrawal_rows: list[EthWithdrawalDTO],
):
    engine: Engine = create_engine(f"postgresql://localhost:5432/{db_name}")
    try:
        with engine.begin() as conn:
            # insert block first; so we don't get foreign key constraint on eth_withdrawals.block_numner with eth_blocks.block_number
            conn.execute(
                insert(eth_block_table).values(
                    [
                        {
                            "block_number": single_input.block_number,
                            "id": single_input.id,
                            "jsonrpc": single_input.jsonrpc,
                            "basefeepergas": single_input.baseFeePerGas,
                            "blobgasused": single_input.blobGasUsed,
                            "difficulty": single_input.difficulty,
                            "excessblobgas": single_input.excessBlobGas,
                            "extradata": single_input.extraData,
                            "gaslimit": single_input.gasLimit,
                            "gasused": single_input.gasUsed,
                            "hash": single_input.hash,
                            "logsbloom": single_input.logsBloom,
                            "miner": single_input.miner,
                            "mixhash": single_input.mixHash,
                            "nonce": single_input.nonce,
                            "number": single_input.number,
                            "parentbeaconblockroot": single_input.parentBeaconBlockRoot,
                            "parenthash": single_input.parentHash,
                            "receiptsroot": single_input.receiptsRoot,
                            "sha3uncles": single_input.sha3Uncles,
                            "size": single_input.size,
                            "stateroot": single_input.stateRoot,
                            "timestamp": single_input.timestamp,
                            "totaldifficulty": single_input.totalDifficulty,
                            "transactionsroot": single_input.transactionsRoot,
                            "withdrawalsroot": single_input.withdrawalsRoot,
                            "created_at": single_input.created_at,
                        }
                        for single_input in dummy_blocks
                    ]
                )
            )
            conn.execute(
                insert(eth_withdrawals_table).values(
                    [
                        {
                            "id": single_input.id,
                            "block_number": single_input.block_number,
                            "address": single_input.address,
                            "amount": single_input.amount,
                            "index": single_input.index,
                            "validatorindex": single_input.validatorIndex,
                            "created_at": single_input.created_at,
                        }
                        for single_input in dummy_withdrawal_rows
                    ]
                )
            )
        engine.dispose()
        yield
    except Exception as e:
        raise e
    finally:
        engine.dispose()


class TestEthWithdrawalsDAO:
    @pytest.mark.asyncio_cooperative
    async def test_read_block_by_block_numbers(
        self,
        setup_test_read_block_by_block_number,
        dao: EthWithdrawalDAO,
        dummy_withdrawal_rows: list[EthWithdrawalDTO],
    ) -> None:
        try:
            id: str = str(dummy_withdrawal_rows[0].id)
            result: EthWithdrawalDTO | None = await dao.read_withdrawal_by_id(id=id)
            expected: EthWithdrawalDTO = dummy_withdrawal_rows[0]
            assert result == expected
        except Exception as err:
            raise err
        finally:
            await dao._engine.dispose()
