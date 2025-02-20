import os
from typing import Any

from dotenv import load_dotenv
import requests

load_dotenv()


def get_latest_blockhash() -> int:
    url = os.getenv("CHAIN_STACK_SOL_URL", "")

    payload = {"id": 1, "jsonrpc": "2.0", "method": "getLatestBlockhash"}
    headers = {"accept": "application/json", "content-type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    # sample of returned data
    # {
    #   "jsonrpc":"2.0",
    #   "result":{"context":{"apiVersion":"2.0.21","slot":319096662},"value":{"blockhash":"gkSKTHS1PpL7TMhKSHmwERKKk8TXv9XpRYoggwBsQBj","lastValidBlockHeight":297375920}},
    #   "id":1
    # }
    latest_blockhash_slot: int = response.json()["result"]["context"]["slot"]
    return latest_blockhash_slot


if __name__ == "__main__":
    latest_blockhash = get_latest_blockhash()
    print(latest_blockhash)
