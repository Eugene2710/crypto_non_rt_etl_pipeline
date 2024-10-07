import datetime

from pydantic import BaseModel, ConfigDict, Field

from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)
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

    block_number: str = Field(serialization_alias="block_number")
    id: int = Field(
        serialization_alias="id"
    )  # from QuickNodeEthBlockInformationResponse.id
    jsonrpc: str = Field(serialization_alias="jsonrpc")
    baseFeePerGas: str | None = Field(None, serialization_alias="basefeepergas")
    blobGasUsed: str | None = Field(None, serialization_alias="blobgasused")
    difficulty: str = Field(serialization_alias="difficulty")
    excessBlobGas: str | None = Field(None, serialization_alias="excessblobgas")
    extraData: str = Field(serialization_alias="extradata")
    gasLimit: str = Field(serialization_alias="gaslimit")
    gasUsed: str = Field(serialization_alias="gasused")
    hash: str | None = Field(
        None, serialization_alias="hash"
    )  # can be null for unsealed block
    logsBloom: str = Field(serialization_alias="logsbloom")
    miner: str = Field(serialization_alias="miner")
    mixHash: str = Field(serialization_alias="mixhash")
    nonce: str = Field(serialization_alias="nonce")
    number: str = Field(serialization_alias="number")
    parentBeaconBlockRoot: str | None = Field(
        None, serialization_alias="parentbeaconblockroot"
    )
    parentHash: str = Field(serialization_alias="parenthash")
    receiptsRoot: str = Field(serialization_alias="receiptsroot")
    sha3Uncles: str = Field(serialization_alias="sha3uncles")
    size: str = Field(serialization_alias="size")
    stateRoot: str = Field(serialization_alias="stateroot")
    timestamp: str = Field(serialization_alias="timestamp")
    totalDifficulty: str = Field(serialization_alias="totaldifficulty")
    transactionsRoot: str = Field(serialization_alias="transactionsroot")
    withdrawalsRoot: str | None = Field(None, serialization_alias="withdrawalsroot")
    created_at: datetime = Field(
        default_factory=datetime.datetime.utcnow, serialization_alias="created_at"
    )
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_block_information_response(
        input: (
            QuickNodeEthBlockInformationResponse | ChainStackEthBlockInformationResponse
        ),
    ) -> "EthBlockDTO":
        """
        Smart constructor to create a DTO from QuickNode service class
        """
        return EthBlockDTO(
            block_number=input.block_number,
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
            created_at=datetime.datetime.utcnow(),  # set created_at to UTC timezone
        )
