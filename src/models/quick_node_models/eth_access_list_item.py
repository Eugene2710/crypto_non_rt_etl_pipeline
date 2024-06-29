# table: transaction_access_list
from pydantic import BaseModel


class QuickNodeEthAccessListItem(BaseModel):
    address: str
    storageKeys: list[str]
