from typing import Any

from pydantic import BaseModel, Field


class AccessListItem(BaseModel):
    address: str
    storageKeys: list[str]


class Withdrawal(BaseModel):
    address: str
    amount: str
    index: str
    validatorIndex: str


class Transaction(BaseModel):
    accessList: list[AccessListItem] | None = None
    blockHash: str
    blockNumber: str
    chainId: str | None = None
    from_: str = Field(..., alias="from")
    gas: str
    gasPrice: str
    hash: str
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
    def from_json(input: dict[str, Any]) -> "Transaction":
        if "accessList" in input:
            return Transaction.model_validate(
                {
                    **input,
                    "accessList": [
                        AccessListItem.model_validate(single_access)
                        for single_access in input["accessList"]
                    ],
                }
            )
        else:
            return Transaction.model_validate({**input})


class BlockInformationResult(BaseModel):
    baseFeePerGas: str
    blobGasUsed: str
    difficulty: str
    excessBlobGas: str
    extraData: str
    gasLimit: str
    gasUsed: str
    hash: str
    logsBloom: str
    miner: str
    mixHash: str
    nonce: str
    number: str
    parentBeaconBlockRoot: str
    parentHash: str
    receiptsRoot: str
    sha3Uncles: str
    size: str
    stateRoot: str
    timestamp: str
    totalDifficulty: str
    transactions: list[Transaction]
    transactionsRoot: str
    uncles: list[str]
    withdrawals: list[Withdrawal]
    withdrawalsRoot: str

    @staticmethod
    def from_json(input: dict[str, Any]) -> "BlockInformationResult":
        return BlockInformationResult.model_validate(
            {
                **input,
                "transactions": [
                    Transaction.from_json(single_transaction)
                    for single_transaction in input["transactions"]
                ],
                "withdrawals": [
                    Withdrawal.model_validate(single_withdrawal)
                    for single_withdrawal in input["withdrawals"]
                ],
            }
        )


class BlockInformationResponse(BaseModel):
    """
    Top level data-class
    Represents the payload schema
    """

    id: int
    jsonrpc: str
    result: BlockInformationResult

    @staticmethod
    def from_json(input: dict[str, Any]) -> "BlockInformationResponse":
        return BlockInformationResponse.model_validate(
            {**input, "result": BlockInformationResult.from_json(input["result"])}
        )
