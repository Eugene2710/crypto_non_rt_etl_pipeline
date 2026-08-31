import json
import os
from typing import Any
import aiohttp
import asyncio
import dotenv
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from src.chainstack.exceptions.chainstack_client_error import ChainStackClientError

dotenv.load_dotenv()


@retry(
    retry=retry_if_exception_type((aiohttp.ClientError, ChainStackClientError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.1, exp_base=1.5, max=0.3375)
    + wait_random(-0.01, 0.01),
    reraise=True,
)
async def get_latest_block_number() -> str:
    url = os.getenv("CHAIN_STACK_URL", "")
    payload: str = json.dumps(
        {"method": "eth_blockNumber", "params": [], "id": 1, "jsonrpc": "2.0"}
    )
    headers = {"Content-Type": "application/json"}

    async with aiohttp.ClientSession() as client:
        async with client.post(
            url, headers=headers, data=payload, ssl=False
        ) as response:
            if response.status == 200:
                # result is the json
                result: dict[str, Any] = await response.json()
            else:
                # can happen when quicknode server is down
                raise ChainStackClientError(
                    f"Received non-status code 200: {response.status}"
                )

    latest_block_number: str = result["result"]
    return latest_block_number


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    result: str = event_loop.run_until_complete(get_latest_block_number())
    print(result)
