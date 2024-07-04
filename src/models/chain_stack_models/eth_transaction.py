from typing import Any

from pydantic import BaseModel, Field, ConfigDict

from src.models.chain_stack_models.eth_access_list_item import (
    ChainStackEthAccessListItem,
)


class ChainStackEthTransaction(BaseModel):
    accessList: list[ChainStackEthAccessListItem] | None = (
        None  # make access list into a separate table
    )
    blockHash: str | None  # can be null for unsealed block
    blockNumber: str
    chainId: str | None = None
    from_: str = Field(..., alias="from")
    gas: str
    gasPrice: str
    hash: str  # hash
    input: str
    maxFeePerGas: str | None = None
    maxPriorityFeePerGas: str | None = None
    nonce: str
    r: str
    s: str
    to: str
    transactionIndex: str
    type: str
    v: str
    value: str
    yParity: str | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_json(input: dict[str, Any]) -> "ChainStackEthTransaction":
        if "accessList" in input:
            return ChainStackEthTransaction.model_validate(
                {
                    **input,
                    "accessList": [
                        ChainStackEthAccessListItem.model_validate(single_access)
                        for single_access in input["accessList"]
                    ],
                }
            )
        else:
            return ChainStackEthTransaction.model_validate({**input})
