from typing import Any
from dotenv import load_dotenv
import requests
import json
import os
from pprint import pformat

from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)
from src.chainstack.synchronous.get_latest_block import get_latest_block_number

load_dotenv()


def get_block_information(block_number: str) -> ChainStackEthBlockInformationResponse:
    url = os.getenv("CHAIN_STACK_URL")
    print(f"url: {url}")
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

    response_model: ChainStackEthBlockInformationResponse = (
        ChainStackEthBlockInformationResponse.from_json(block_number, response_dict)
    )
    return response_model


if __name__ == "__main__":
    latest_block: str = get_latest_block_number()
    latest_block_information: ChainStackEthBlockInformationResponse = (
        get_block_information(latest_block)
    )
    print(f"latest_block_information for {latest_block}")
    print(pformat(latest_block_information))

    # previous_block: str = decrement_block_number(latest_block)
    # print(f"previous_block: {previous_block}")
    # previous_block_information: dict[str, Any] = get_block_information(previous_block)
    # print(f"previous_block_information for {previous_block}:")
    # print(pformat(previous_block_information))
