import datetime

from pydantic import BaseModel, ConfigDict

from src.models.quick_node_models.eth_blocks import (
    QuickNodeEthBlockInformationResponse,
)


class EthBlockDTO(BaseModel):
    """
    Database Transfer Object (data class for data in DB)

    Converted from QuickNodeEthBlockInformationResponse
    - Added QuickNodeEthBlockInformationResponse.jsonrpc
    - Fields from QuickNodeEthBlockInformationResponse, but omit nested objects QuickNodeEthWithdrawal and QuickNodeEthTransaction

    Table: quick_node.blocks
    """

    id: int  # from QuickNodeEthBlockInformationResponse.id, don't generate this
    jsonrpc: str
    baseFeePerGas: str | None
    blobGasUsed: str | None
    difficulty: str
    excessBlobGas: str | None
    extraData: str
    gasLimit: str
    gasUsed: str
    hash: str | None  # can be null for unsealed block
    logsBloom: str
    miner: str
    mixHash: str
    nonce: str
    number: str
    parentBeaconBlockRoot: str | None
    parentHash: str
    receiptsRoot: str
    sha3Uncles: str
    size: str
    stateRoot: str
    timestamp: str
    totalDifficulty: str
    transactionsRoot: str
    withdrawalsRoot: str | None
    created_at: datetime.datetime
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_quick_node_block_information_response(
        input: QuickNodeEthBlockInformationResponse,
    ) -> "EthBlockDTO":
        """
        Smart constructor to create a DTO from QuickNode service class
        """
        return EthBlockDTO(
            id=input.id,
            jsonrpc=input.jsonrpc,
            baseFeePerGas=input.result.baseFeePerGas,
            blobGasUsed=input.result.blobGasUsed,
            difficulty=input.result.difficulty,
            excessBlobGas=input.result.excessBlobGas,
            extraData=input.result.extraData,
            gasLimit=input.result.gasLimit,
            gasUsed=input.result.gasUsed,
            hash=input.result.hash,
            logsBloom=input.result.logsBloom,
            miner=input.result.miner,
            mixHash=input.result.mixHash,
            nonce=input.result.nonce,
            number=input.result.number,
            parentBeaconBlockRoot=input.result.parentBeaconBlockRoot,
            parentHash=input.result.parentHash,
            receiptsRoot=input.result.receiptsRoot,
            sha3Uncles=input.result.sha3Uncles,
            size=input.result.size,
            stateRoot=input.result.stateRoot,
            timestamp=input.result.timestamp,
            totalDifficulty=input.result.totalDifficulty,
            transactionsRoot=input.result.transactionsRoot,
            withdrawalsRoot=input.result.withdrawalsRoot,
            created_at=datetime.datetime.now(
                datetime.UTC
            ),  # set created_at to UTC timezone
        )
