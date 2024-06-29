import json
import os
from typing import Any
import aiohttp
import asyncio
import dotenv


dotenv.load_dotenv()


async def get_latest_block_number() -> str:
    url = os.getenv("QUICK_NODE_URL")
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
                raise Exception(f"Received non-status code 200: {response.status}")

    latest_block_number: str = result["result"]
    return latest_block_number


if __name__ == "__main__":
    event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
    result: str = event_loop.run_until_complete(get_latest_block_number())
    print(result)
