# table: transaction_access_list
from pydantic import BaseModel


class ChainStackEthAccessListItem(BaseModel):
    address: str
    storageKeys: list[str]
