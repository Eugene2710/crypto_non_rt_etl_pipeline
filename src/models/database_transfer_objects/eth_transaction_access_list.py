from pydantic import BaseModel, ConfigDict
import uuid
import datetime

from src.models.chain_stack_models.eth_access_list_item import (
    ChainStackEthAccessListItem,
)
from src.models.quick_node_models.eth_access_list_item import QuickNodeEthAccessListItem


class EthTransactionAccessListDTO(BaseModel):
    """
    DTO for table quick_node.eth_transaction_access_list
    """

    id: str
    transaction_hash: str
    # position of this entry in the transaction's accessList array; part of the
    # natural key, since EIP-2930 permits the same address to appear more than once
    item_index: int
    address: str
    storageKeys: list[str]
    created_at: datetime.datetime
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_eth_access_list_item(
        transaction_hash: str,
        item_index: int,
        input: QuickNodeEthAccessListItem | ChainStackEthAccessListItem,
    ) -> "EthTransactionAccessListDTO":
        return EthTransactionAccessListDTO(
            id=str(uuid.uuid4()),
            transaction_hash=transaction_hash,
            item_index=item_index,
            address=input.address,
            storageKeys=input.storageKeys,
            created_at=datetime.datetime.utcnow(),
        )
