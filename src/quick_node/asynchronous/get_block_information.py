import asyncio
from typing import Any

import aiohttp
from dotenv import load_dotenv
import json
import os
from src.models.quick_node_models.eth_blocks import (
    QuickNodeEthBlockInformationResponse,
)
from src.quick_node.asynchronous.get_latest_block import get_latest_block_number

load_dotenv()


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
                raise Exception(f"Received non-status code 200: {response.status}")

    response_model: QuickNodeEthBlockInformationResponse = (
        QuickNodeEthBlockInformationResponse.from_json(response_dict)
    )
    return response_model


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    block_number: str = event_loop.run_until_complete(get_latest_block_number())
    block_information: QuickNodeEthBlockInformationResponse = (
        event_loop.run_until_complete(get_block_information(block_number))
    )
    print(block_information)
