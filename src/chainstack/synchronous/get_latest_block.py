import os
from typing import Any

from dotenv import load_dotenv
import requests
import json

load_dotenv()


def get_latest_block_number() -> str:
    url = os.getenv("CHAIN_STACK_URL", "")
    payload = json.dumps(
        {"method": "eth_blockNumber", "params": [], "id": 1, "jsonrpc": "2.0"}
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)

    # data in this format
    # {"jsonrpc":"2.0","id":1,"result":"0x13258e9"}
    latest_block_response: dict[str, Any] = response.json()
    latest_block_number: str = latest_block_response["result"]
    return latest_block_number


if __name__ == "__main__":
    latest_block_number: str = get_latest_block_number()
    print(latest_block_number)
