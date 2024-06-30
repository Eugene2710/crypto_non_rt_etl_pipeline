from pydantic import BaseModel, ConfigDict
import uuid
import datetime

from src.models.quick_node_models.eth_access_list_item import QuickNodeEthAccessListItem


class EthTransactionAccessListDTO(BaseModel):
    """
    DTO for table quick_node.eth_transaction_access_list
    """

    id: str
    transaction_hash: str
    address: str
    storageKeys: list[str]
    created_at: datetime.datetime
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_quick_node_eth_access_list_item(
        transaction_hash: str, input: QuickNodeEthAccessListItem
    ) -> "EthTransactionAccessListDTO":
        return EthTransactionAccessListDTO(
            id=uuid.uuid4(),
            transaction_hash=transaction_hash,
            address=input.address,
            storageKeys=input.storageKeys,
            created_at=datetime.datetime.utcnow(),
        )
