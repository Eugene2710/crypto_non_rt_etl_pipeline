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
    # each block is a separate HTTP request on its own session, so these genuinely
    # run in parallel. Left unbounded, a batch fires one request per block at once,
    # which a rate-limited RPC endpoint will start rejecting under a real backfill.
    DEFAULT_MAX_CONCURRENT_REQUESTS: int = 10

    def __init__(
        self, max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS
    ) -> None:
        self._max_concurrent_requests: int = max_concurrent_requests

    async def _get_block_information_with_limit(
        self, semaphore: asyncio.Semaphore, block_number: str
    ) -> ChainStackEthBlockInformationResponse:
        async with semaphore:
            return await get_block_information(block_number)

    async def extract(
        self, start_block_number: int, end_block_number: int
    ) -> list[ChainStackEthBlockInformationResponse]:
        """
        Fires (end_block_number - start_block_number + 1) async queries to quicknode

        100 - 1 + 1 = 100 queries

        At most self._max_concurrent_requests are in flight at a time.

        Await for all blocks to return, then return
        """
        # created per call so the semaphore always belongs to the running event loop
        semaphore: asyncio.Semaphore = asyncio.Semaphore(self._max_concurrent_requests)

        async_futures: list[Future[ChainStackEthBlockInformationResponse]] = [
            asyncio.ensure_future(
                self._get_block_information_with_limit(
                    semaphore, hex(curr_block_number)
                )
            )
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
