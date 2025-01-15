import asyncio
from asyncio import Future

from src.extractors.abstract_extractor import BaseExtractor
from src.models.quick_node_models.eth_blocks import QuickNodeEthBlockInformationResponse
from src.quick_node.asynchronous.get_block_information import get_block_information


class QuickNodeBlockExtractor(BaseExtractor[QuickNodeEthBlockInformationResponse]):
    async def extract(  # type: ignore[override]
        self, start_block_number: int, end_block_number: int
    ) -> list[QuickNodeEthBlockInformationResponse]:
        """
        Fires (end_block_number - start_block_number + 1) async queries to quicknode

        100 - 1 + 1 = 100 queries

        Await for all blocks to return, then return
        """
        print(
            f"start_block_number: {start_block_number}, end_block_number: {end_block_number}"
        )
        for curr_block_number in range(start_block_number, end_block_number + 1):
            print(hex(curr_block_number))

        async_futures: list[Future[QuickNodeEthBlockInformationResponse]] = [
            asyncio.ensure_future(get_block_information(hex(curr_block_number)))
            for curr_block_number in range(start_block_number, end_block_number + 1)
        ]

        all_blocks_future: Future[list[QuickNodeEthBlockInformationResponse]] = (
            asyncio.gather(*async_futures)
        )
        result: list[QuickNodeEthBlockInformationResponse] = await all_blocks_future
        return result
