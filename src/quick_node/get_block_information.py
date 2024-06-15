from typing import Any
from dotenv import load_dotenv
import requests
import json
import os
from pprint import pformat

from src.models.block_models import BlockInformationResponse
from src.quick_node.get_latest_block import get_latest_block_number

load_dotenv()


def get_block_information(block_number: str) -> BlockInformationResponse:
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

    response: requests.Response = requests.request(
        "POST", url, headers=headers, data=payload
    )
    response_dict: dict[str, Any] = response.json()

    with open("response.json", mode="w") as file:
        file.write(pformat(response_dict))

    response_model: BlockInformationResponse = BlockInformationResponse.from_json(
        response_dict
    )
    return response_model


if __name__ == "__main__":
    latest_block: str = get_latest_block_number()
    latest_block_information: BlockInformationResponse = get_block_information(
        latest_block
    )
    print(f"latest_block_information for {latest_block}")
    print(pformat(latest_block_information))

    # previous_block: str = decrement_block_number(latest_block)
    # print(f"previous_block: {previous_block}")
    # previous_block_information: dict[str, Any] = get_block_information(previous_block)
    # print(f"previous_block_information for {previous_block}:")
    # print(pformat(previous_block_information))
