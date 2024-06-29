from typing import Any

from pydantic import BaseModel, Field

from src.models.quick_node_models.eth_access_list_item import QuickNodeEthAccessListItem


class QuickNodeEthTransaction(BaseModel):
    accessList: list[QuickNodeEthAccessListItem] | None = (
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

    @staticmethod
    def from_json(input: dict[str, Any]) -> "QuickNodeEthTransaction":
        if "accessList" in input:
            return QuickNodeEthTransaction.model_validate(
                {
                    **input,
                    "accessList": [
                        QuickNodeEthAccessListItem.model_validate(single_access)
                        for single_access in input["accessList"]
                    ],
                }
            )
        else:
            return QuickNodeEthTransaction.model_validate({**input})
