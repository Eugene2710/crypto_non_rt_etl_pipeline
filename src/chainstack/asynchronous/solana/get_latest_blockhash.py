import json
import os
from typing import Any
import aiohttp
import asyncio
import dotenv
from retry import retry

from src.chainstack.exceptions.chainstack_client_error import ChainStackClientError

dotenv.load_dotenv()


@retry(
    exceptions=(aiohttp.ClientError, ChainStackClientError),
    tries=5,
    delay=0.1,
    max_delay=0.3375,
    backoff=1.5,
    jitter=(-0.01, 0.01),
)
async def get_latest_blockhash() -> int:
    url = os.getenv("CHAIN_STACK_SOL_URL", "")
    payload: str = json.dumps(
        {"method": "getLatestBlockhash", "params": [], "id": 1, "jsonrpc": "2.0"}
    )
    headers = {"accept": "application/json", "content-type": "application/json"}

    async with aiohttp.ClientSession() as client:
        async with client.post(
            url, headers=headers, data=payload, ssl=False
        ) as response:
            if response.status == 200:
                result: dict[str, Any] = await response.json()
            else:
                # can happen when chainstack server is down
                raise ChainStackClientError(
                    f"Received non-status code 200: {response.status}"
                )

    latest_blockhash_slot: int = result["result"]["context"]["slot"]
    return latest_blockhash_slot


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    result: int = event_loop.run_until_complete(get_latest_blockhash())
    print(result)
