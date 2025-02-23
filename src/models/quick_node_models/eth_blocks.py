from typing import Any

from pydantic import BaseModel, ConfigDict

from src.models.quick_node_models.eth_transaction import QuickNodeEthTransaction
from src.models.quick_node_models.eth_withdrawal import QuickNodeEthWithdrawal


class QuickNodeEthBlockInformationResult(
    BaseModel
):  # create a block table -> foreign key constraint to transaction table
    """
    Data class for data from QuickNode

    This data class contains nested objects, we can't save this to DB directly
    We have to convert this to a BlockDTO, to be ready to be saved in the DB
    """

    baseFeePerGas: str | None = None
    blobGasUsed: str | None = None
    difficulty: str
    excessBlobGas: str | None = None
    extraData: str
    gasLimit: str
    gasUsed: str
    hash: str | None = None  # can be nullable for unsealed block
    logsBloom: str
    miner: str
    mixHash: str
    nonce: str
    number: str
    parentBeaconBlockRoot: str | None = None
    parentHash: str
    receiptsRoot: str
    sha3Uncles: str
    size: str
    stateRoot: str
    timestamp: str
    totalDifficulty: str
    transactions: list[QuickNodeEthTransaction]  # create a transaction table
    transactionsRoot: str
    uncles: list[str]
    withdrawals: list[QuickNodeEthWithdrawal]
    withdrawalsRoot: str | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_json(input: dict[str, Any]) -> "QuickNodeEthBlockInformationResult":
        return QuickNodeEthBlockInformationResult.model_validate(
            {
                **input,
                "transactions": [
                    QuickNodeEthTransaction.from_json(single_transaction)
                    for single_transaction in input["transactions"]
                ],
                "withdrawals": (
                    [
                        QuickNodeEthWithdrawal.model_validate(single_withdrawal)
                        for single_withdrawal in input.get("withdrawals", [])
                    ]
                    if "withdrawals" in input
                    else []
                ),
            }
        )


class QuickNodeEthBlockInformationResponse(BaseModel):
    """
    Top level data-class
    Represents the payload schema
    """

    block_number: str
    id: int  # id from quick node
    jsonrpc: str
    result: QuickNodeEthBlockInformationResult

    @staticmethod
    def from_json(
        block_number: str, input: dict[str, Any]
    ) -> "QuickNodeEthBlockInformationResponse":
        return QuickNodeEthBlockInformationResponse.model_validate(
            {
                "block_number": block_number,
                **input,
                "result": QuickNodeEthBlockInformationResult.from_json(input["result"]),
            }
        )
