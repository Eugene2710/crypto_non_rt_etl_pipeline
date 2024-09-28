import asyncio
import json
from asyncio import Future
from typing import Any

from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)
from src.chainstack.asynchronous.get_block_information import get_block_information
from asyncio import AbstractEventLoop, new_event_loop


class ChainStackBlockExtractor:
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


if __name__ == "__main__":
    extractor: ChainStackBlockExtractor = ChainStackBlockExtractor()
    event_loop: AbstractEventLoop = new_event_loop()
    result: list[ChainStackEthBlockInformationResponse] = event_loop.run_until_complete(
        extractor.extract(start_block_number=20846330, end_block_number=20846334)
    )
    serialized_result_dict: list[dict[str, Any]] = [
        single_model.model_dump() for single_model in result
    ]
    # write the expected result of the integration file into a file
    with open(
        "integration_tests/src/extractors/test_files/expected_transaction_results.json",
        "w",
    ) as file:
        serialized_results_str: str = json.dumps(serialized_result_dict)
        file.write(serialized_results_str)
