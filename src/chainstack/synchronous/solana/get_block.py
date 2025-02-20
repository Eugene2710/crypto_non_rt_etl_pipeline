import os
from typing import Any
import json

from dotenv import load_dotenv
import requests
from pprint import pformat

from src.chainstack.synchronous.solana.get_latest_blockhash import get_latest_blockhash

load_dotenv()


def get_block(slot_number: int) -> Any:
    url: str | None = os.getenv("CHAIN_STACK_SOL_URL", "")

    payload: str = json.dumps(
        {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "getBlock",
            "params": [
                slot_number,
                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
            ],
        }
    )
    headers: dict[str, str] = {
        "accept": "application/json",
        "content-type": "application/json",
    }
    response: requests.Response = requests.request(
        "POST", url, headers=headers, data=payload
    )

    with open("response.json", mode="w") as file:
        file.write(pformat(response))

    return response.text


if __name__ == "__main__":
    latest_slot: int = get_latest_blockhash()
    latest_block_info = get_block(latest_slot)

    print(pformat(latest_block_info))
