import datetime
from pydantic import BaseModel

from src.models.quick_node_models.eth_blocks import QuickNodeEthTransaction


class EthTransactionDTO(BaseModel):
    """
    DTO for eth_transactions
    """

    hash: str  # transaction hash, identifier
    block_id: str
    blockHash: str | None = None
    blockNumber: str
    chainId: str | None = None
    from_address: str
    gas: str
    gasPrice: str
    input: str
    maxFeePerGas: str | None = None
    maxPriorityFeePerGas: str
    nonce: str
    r: str
    s: str
    to_address: str
    transactionIndex: str
    type: str
    v: str
    value: str
    yParity: str | None = None
    created_at: datetime.datetime

    class Config:
        orm_mode = True

    def from_quick_node_eth_transaction(
        self, block_id: str, input: QuickNodeEthTransaction
    ) -> "EthTransactionDTO":
        return EthTransactionDTO(
            hash=input.hash,
            block_id=block_id,
            blockHash=input.blockHash,
            blockNumber=input.blockNumber,
            chainId=input.chainId,
            from_address=input.from_,
            gas=input.gas,
            gasPrice=input.gasPrice,
            input=self.input,
            maxFeePerGas=input.maxFeePerGas,
            maxPriorityFeePerGas=input.maxPriorityFeePerGas,
            nonce=input.nonce,
            r=input.r,
            s=input.s,
            to_address=input.to_address,
            transactionIndex=input.transactionIndex,
            type=input.type,
            v=input.v,
            value=input.value,
            yParity=input.yParity,
            created_at=datetime.datetime.now(datetime.UTC),
        )