import asyncio
import os
from asyncio import Future
from pprint import pprint

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from src.dao.eth_block_dao import EthBlockDAO
from src.dao.eth_block_import_status_dao import EthBlockImportStatusDAO
from src.dao.eth_transaction_access_list_dao import EthTransactionAccessListDAO
from src.dao.eth_transactions_dao import EthTransactionDAO
from src.dao.eth_withdrawals_dao import EthWithdrawalDAO
from src.extractors.chain_stack_block_extractor import ChainStackBlockExtractor
from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)
from src.models.database_transfer_objects.eth_block_import_status import (
    EthBlockImportStatusDTO,
)
from src.models.database_transfer_objects.eth_blocks import EthBlockDTO
from src.models.database_transfer_objects.eth_transaction import EthTransactionDTO
from src.models.database_transfer_objects.eth_transaction_access_list import (
    EthTransactionAccessListDTO,
)
from src.models.database_transfer_objects.eth_withdrawals import EthWithdrawalDTO
from dotenv import load_dotenv

load_dotenv()


class ChainStackEthBlockETLPipeline:
    def __init__(
        self,
        import_status_dao: EthBlockImportStatusDAO,
        block_dao: EthBlockDAO,
        transaction_dao: EthTransactionDAO,
        transaction_access_list_dao: EthTransactionAccessListDAO,
        withdrawal_dao: EthWithdrawalDAO,
        extractor: ChainStackBlockExtractor,
        batch_size: int = 100,
    ) -> None:
        self._engine: AsyncEngine = create_async_engine(
            os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", "")
        )
        self._import_status_dao: EthBlockImportStatusDAO = import_status_dao
        self._block_dao: EthBlockDAO = block_dao
        self._transaction_dao: EthTransactionDAO = transaction_dao
        self._transaction_access_list_dao: EthTransactionAccessListDAO = (
            transaction_access_list_dao
        )
        self._withdrawal_dao: EthWithdrawalDAO = withdrawal_dao
        self._extractor: ChainStackBlockExtractor = extractor
        self._batch_size: int = batch_size

    async def run(self) -> None:
        """
        Entry point of ETLPipeline

        Runs the ETL Pipeline to
        1. Fetch latest ingested eth block_number from quick_node.eth_block_import_status
        2. Fetch current latest block_number in quick node
        3. Extract and Load
            Extract the new ethereum blocks from quicknode (latest ingest eth block_number + 1 to current latest block number in quick node)
            - Done in batches of 100
                3.1: Extract 100 block information from QuickNode
                3.2: Convert the 100 blocks (service level data class) into DTOs (This is nested, you will get 4 types of DTOs from a single block)
                    - Block
                    - Transaction
                        - Transaction Access List Item
                    - Withdrawal
                3.3 Insert all into postgres (DAO)
                3.4 Insert latest block number into quick_node.eth_block_import_status

        Step 3 must be atomic; single transaction; all or nothing.
            - We don't want to miss out any blocks
            - If any fails, we rollback, and let next run fix it
        """
        # Step 1: Fetch latest ingested eth block_number from quick_node.eth_block_import_status
        latest_import_status: EthBlockImportStatusDTO | None = (
            await self._import_status_dao.read_latest_import_status()
        )
        # if import status doesn't exist; first time ingesting
        # default block_number to
        if latest_import_status is None:
            latest_import_status = EthBlockImportStatusDTO.create_import_status(
                block_number=0
            )

        # TODO: temporarily make the ETL pipeline run for only block number 0 to 1 for testing (2 blocks)
        # Remove this after testing
        start_block_number: int = latest_import_status.block_number + 1
        # Step 2: Fetch current latest block_number in quick node
        # IMPORTANT: THIS SINGLE LINE PROTECTS YOUR WALLET
        # Temporarily ingest block 0 to block 10
        end_block_number: str = hex(start_block_number + 100)  # await get_latest_block_number()
        end_block_number_int: int = int(end_block_number[2:], 16)

        for start in range(
            start_block_number, end_block_number_int + 1, self._batch_size
        ):
            # Step 3: Extract, and Load
            await self.run_for_batch(
                start, min(start + self._batch_size, end_block_number_int)
            )

    async def run_for_batch(
        self, start_block_number: int, end_block_number: int
    ) -> None:
        """
        Running the ETL pipeline to ingest ethereum blocks from start_block_number to end_block_number

        1 to 100 -> 100 blocks

        1. Use extractor to fetch 100 blocks (service level data class instances)
        2. Convert the service level data class instances into database transfer objects
        3. Insert the 100 DTOs into database
        4. insert end_block_number into import status table

        All 4 steps must be atomic; under a single transaction. It's all or nothing, so we don't miss out any blocks

        If anything goes wrong, panic, raise an exception, and let the data pipeline crash
        """

        # Step 3.1: Extract 100 block information from QuickNode
        batch_of_blocks: list[ChainStackEthBlockInformationResponse] = (
            await self._extractor.extract(
                start_block_number=start_block_number, end_block_number=end_block_number
            )
        )

        # Step 3.2: Convert the 100 blocks (service level data class) into DTOs (This is nested, you will get 4 types of DTOs from a single block)
        (
            eth_block_dtos,
            eth_transaction_dtos,
            eth_withdrawal_dtos,
            eth_transaction_access_list_dtos,
        ) = self.blocks_to_dto(input=batch_of_blocks)

        # Step 3.3 Insert all into postgres (DAO)
        # both save and insert step must be done with a single shared sqlalcheny.AsyncConnection (to be part of a single transaction)
        await self.insert_dtos_and_update_import_status(
            eth_block_dtos=eth_block_dtos,
            eth_transaction_dtos=eth_transaction_dtos,
            eth_withdrawal_dtos=eth_withdrawal_dtos,
            eth_transaction_access_list_dtos=eth_transaction_access_list_dtos,
            end_block_number=end_block_number,
        )

    @staticmethod
    def blocks_to_dto(
        input: list[ChainStackEthBlockInformationResponse],
    ) -> tuple[
        list[EthBlockDTO],
        list[EthTransactionDTO],
        list[EthWithdrawalDTO],
        list[EthTransactionAccessListDTO],
    ]:
        """
        TODO: unit test this
        """
        batch_of_blocks_dto: list[EthBlockDTO] = []
        batch_of_transactions_dto: list[EthTransactionDTO] = []
        batch_of_withdrawals_dto: list[EthWithdrawalDTO] = []
        batch_of_transactions_access_list_items_dto: list[
            EthTransactionAccessListDTO
        ] = []

        for single_block in input:
            eth_block_dto: EthBlockDTO = EthBlockDTO.from_block_information_response(
                single_block
            )
            withdrawal_dto_list: list[EthWithdrawalDTO] = [
                EthWithdrawalDTO.from_quick_node_withdrawal(
                    block_number=single_block.block_number, input=single_withdrawal
                )
                for single_withdrawal in single_block.result.withdrawals
            ]
            transaction_dto_list: list[EthTransactionDTO] = []
            access_list_items_dto_list: list[EthTransactionAccessListDTO] = []

            for single_transaction in single_block.result.transactions:
                eth_transaction_dto: EthTransactionDTO = (
                    EthTransactionDTO.from_eth_transaction(
                        block_id=single_block.id, input=single_transaction
                    )
                )
                single_transaction_eth_access_list_items: list[
                    EthTransactionAccessListDTO
                ] = (
                    [
                        EthTransactionAccessListDTO.from_eth_access_list_item(
                            transaction_hash=single_transaction.hash,
                            input=single_access_list_item,
                        )
                        for single_access_list_item in single_transaction.accessList
                    ]
                    if single_transaction.accessList
                    else []
                )
                transaction_dto_list.append(eth_transaction_dto)
                access_list_items_dto_list.extend(
                    single_transaction_eth_access_list_items
                )

            batch_of_blocks_dto.append(eth_block_dto)
            batch_of_transactions_dto.extend(batch_of_transactions_dto)
            batch_of_withdrawals_dto.extend(withdrawal_dto_list)
            batch_of_transactions_access_list_items_dto.extend(
                access_list_items_dto_list
            )
        return (
            batch_of_blocks_dto,
            batch_of_transactions_dto,
            batch_of_withdrawals_dto,
            batch_of_transactions_access_list_items_dto,
        )

    async def insert_dtos_and_update_import_status(
        self,
        eth_block_dtos: list[EthBlockDTO],
        eth_transaction_dtos: list[EthTransactionDTO],
        eth_withdrawal_dtos: list[EthWithdrawalDTO],
        eth_transaction_access_list_dtos: list[EthTransactionAccessListDTO],
        end_block_number: int,
    ) -> None:
        """
        TODO: integration test this
        """
        async with self._engine.begin() as async_connection:
            # fire inserts in order
            # insert block first; withdrawals, transactions has a foreign key constraint to block.id and can only be inserted after blocks are inserted
            await self._block_dao.insert_blocks(
                async_connection=async_connection, input=eth_block_dtos
            )
            print("insert_dtos_and_update_import_status:")
            pprint(eth_transaction_dtos)
            # then, insert withdrawals and transactions in parallel
            insert_transaction_future: Future = asyncio.ensure_future(
                self._transaction_dao.insert_transactions(
                    async_connection=async_connection, input=eth_transaction_dtos
                )
            )
            insert_withdrawal_future: Future = asyncio.ensure_future(
                self._withdrawal_dao.insert_withdrawals(
                    async_connection=async_connection, input=eth_withdrawal_dtos
                )
            )
            insert_both_transaction_and_withdrawal_future: Future = asyncio.gather(
                insert_transaction_future, insert_withdrawal_future
            )
            await insert_both_transaction_and_withdrawal_future
            # then, insert transaction_access_list, it has a foreign key constraint to transactions, and can only be done after transactions are inserted
            await self._transaction_access_list_dao.insert_transaction_access_list(
                async_connection=async_connection,
                input=eth_transaction_access_list_dtos,
            )

            # Step 3.4 Insert latest block number into quick_node.eth_block_import_status
            # I.E, we don't update import status table, if any insertion fails
            new_import_status: EthBlockImportStatusDTO = (
                EthBlockImportStatusDTO.create_import_status(
                    block_number=end_block_number
                )
            )
            await self._import_status_dao.insert_import_status(
                async_connection=async_connection, input=new_import_status
            )


def trigger_etl_pipeline() -> None:
    connection_string: str = os.getenv("CHAIN_STACK_PG_CONNECTION_STRING", "")
    import_status_dao: EthBlockImportStatusDAO = EthBlockImportStatusDAO(
        connection_string=connection_string
    )
    block_dao: EthBlockDAO = EthBlockDAO(connection_string=connection_string)
    transaction_dao: EthTransactionDAO = EthTransactionDAO(
        connection_string=connection_string
    )
    transaction_access_list_dao: EthTransactionAccessListDAO = (
        EthTransactionAccessListDAO(connection_string=connection_string)
    )
    withdrawal_dao: EthWithdrawalDAO = EthWithdrawalDAO(
        connection_string=connection_string
    )
    extractor: ChainStackBlockExtractor = ChainStackBlockExtractor()
    etl_pipeline: ChainStackEthBlockETLPipeline = ChainStackEthBlockETLPipeline(
        import_status_dao=import_status_dao,
        block_dao=block_dao,
        transaction_dao=transaction_dao,
        transaction_access_list_dao=transaction_access_list_dao,
        withdrawal_dao=withdrawal_dao,
        extractor=extractor,
        batch_size=100,
    )
    asyncio.run(etl_pipeline.run())


if __name__ == "__main__":
    trigger_etl_pipeline()
