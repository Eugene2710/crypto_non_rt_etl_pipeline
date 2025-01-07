from typing import Any

import requests
import json
from dotenv import load_dotenv
import os

# search for .env file in your project, and import them as env vars
load_dotenv()


def get_blob_base_fee() -> dict[str, Any]:
    url: str = os.getenv("QUICK_NODE_URL", "")
    payload: str = json.dumps(
        {"method": "eth_blobBaseFee", "params": [], "id": 1, "jsonrpc": "2.0"}
    )
    headers = {"Content-Type": "application/json"}
    response: requests.Response = requests.request(
        "POST", url, headers=headers, data=payload
    )
    response_dict: dict[str, Any] = response.json()
    return response_dict


if __name__ == "__main__":
    blob_base_fee: dict[str, Any] = get_blob_base_fee()
    # {'jsonrpc': '2.0', 'result': '0x1', 'id': 1}
    print(blob_base_fee)
