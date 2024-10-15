from typing import Any

from pydantic import BaseModel, ConfigDict

from src.models.chain_stack_models.eth_transaction import ChainStackEthTransaction
from src.models.chain_stack_models.eth_withdrawal import ChainStackEthWithdrawal


class ChainStackEthBlockInformationResult(
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
    totalDifficulty: str | None = None
    transactions: list[ChainStackEthTransaction]  # create a transaction table
    transactionsRoot: str
    uncles: list[str]
    withdrawals: list[ChainStackEthWithdrawal]
    withdrawalsRoot: str | None = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_json(input: dict[str, Any]) -> "ChainStackEthBlockInformationResult":
        return ChainStackEthBlockInformationResult.model_validate(
            {
                **input,
                "transactions": [
                    ChainStackEthTransaction.from_json(single_transaction)
                    for single_transaction in input["transactions"]
                ],
                "withdrawals": (
                    [
                        ChainStackEthWithdrawal.model_validate(single_withdrawal)
                        for single_withdrawal in input.get("withdrawals")
                    ]
                    if "withdrawals" in input
                    else []
                ),
            }
        )


class ChainStackEthBlockInformationResponse(BaseModel):
    """
    Top level data-class
    Represents the payload schema
    """

    block_number: str
    id: int  # id from quick node
    jsonrpc: str
    result: ChainStackEthBlockInformationResult

    @staticmethod
    def from_json(
        block_number: str, input: dict[str, Any]
    ) -> "ChainStackEthBlockInformationResponse":
        return ChainStackEthBlockInformationResponse.model_validate(
            {
                "block_number": block_number,
                **input,
                "result": ChainStackEthBlockInformationResult.from_json(
                    input["result"]
                ),
            }
        )
