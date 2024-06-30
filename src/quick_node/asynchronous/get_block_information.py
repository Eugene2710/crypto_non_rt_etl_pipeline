import asyncio
from pprint import pprint
from typing import Any

import aiohttp
from dotenv import load_dotenv
import json
import os

from retry import retry

from src.models.quick_node_models.eth_blocks import (
    QuickNodeEthBlockInformationResponse,
)
from src.quick_node.exceptions.quick_node_client_error import QuickNodeClientError

load_dotenv()


@retry(
    exceptions=(aiohttp.ClientError, QuickNodeClientError),
    tries=5,
    delay=0.1,
    max_delay=0.3375,
    backoff=1.5,
    jitter=(-0.01, 0.01),
)
async def get_block_information(
    block_number: str,
) -> QuickNodeEthBlockInformationResponse:
    url: str = os.getenv("QUICK_NODE_URL")
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
                # can happen when quicknode server is down
                raise QuickNodeClientError(
                    f"Received non-status code 200: {response.status}"
                )

    pprint(response_dict)
    response_model: QuickNodeEthBlockInformationResponse = (
        QuickNodeEthBlockInformationResponse.from_json(response_dict)
    )
    return response_model


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    for i in range(0, 100):
        block_number: str = hex(
            i
        )  # event_loop.run_until_complete(get_latest_block_number())
        block_information: QuickNodeEthBlockInformationResponse = (
            event_loop.run_until_complete(get_block_information(block_number))
        )
        print(block_information)
