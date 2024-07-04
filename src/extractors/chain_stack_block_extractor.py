import asyncio
from asyncio import Future

from src.extractors.abstract_extractor import BaseExtractor
from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)
from src.chainstack.asynchronous.get_block_information import get_block_information


class ChainStackBlockExtractor(BaseExtractor[ChainStackEthBlockInformationResponse]):
    async def extract(
        self, start_block_number: int, end_block_number: int
    ) -> list[ChainStackEthBlockInformationResponse]:
        """
        Fires (end_block_number - start_block_number + 1) async queries to quicknode

        100 - 1 + 1 = 100 queries

        Await for all blocks to return, then return
        """
        for curr_block_number in range(start_block_number, end_block_number + 1):
            print(hex(curr_block_number))

        async_futures: list[Future[ChainStackEthBlockInformationResponse]] = [
            asyncio.ensure_future(get_block_information(hex(curr_block_number)))
            for curr_block_number in range(start_block_number, end_block_number + 1)
        ]

        all_blocks_future: Future[list[ChainStackEthBlockInformationResponse]] = (
            asyncio.gather(*async_futures)
        )
        result: list[ChainStackEthBlockInformationResponse] = await all_blocks_future
        return result
