import asyncio
from typing import Any

import aiohttp
from dotenv import load_dotenv
import json
import os

from retry import retry

from src.chainstack.exceptions.chainstack_client_error import ChainStackClientError
from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)

load_dotenv()


@retry(
    exceptions=(aiohttp.ClientError, ChainStackClientError),
    tries=5,
    delay=0.1,
    max_delay=0.3375,
    backoff=1.5,
    jitter=(-0.01, 0.01),
)
async def get_block_information(
    block_number: str,
) -> ChainStackEthBlockInformationResponse:
    url = os.getenv("CHAIN_STACK_URL", "")
    payload: str = json.dumps(
        {
            "method": "eth_getBlockByNumber",
            "params": [block_number, True],  # set to True
            "id": 1,
            "jsonrpc": "2.0",
        }
    )
    headers: dict[str, str] = {"Content-Type": "application/json"}

    async with aiohttp.ClientSession() as client:
        async with client.post(url, headers=headers, data=payload) as response:
            if response.status == 200:
                response_dict: dict[str, Any] = await response.json()
            else:
                # can happen when chainstack server is down
                raise ChainStackClientError(
                    f"Received non-status code 200: {response.status}"
                )

    response_model: ChainStackEthBlockInformationResponse = (
        ChainStackEthBlockInformationResponse.from_json(block_number, response_dict)
    )
    return response_model


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    for i in range(0, 100):
        block_number: str = hex(
            i
        )  # event_loop.run_until_complete(get_latest_block_number())
        block_information: ChainStackEthBlockInformationResponse = (
            event_loop.run_until_complete(get_block_information(block_number))
        )
        print(block_information.id)
