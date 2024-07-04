from pydantic import BaseModel


class ChainStackEthWithdrawal(BaseModel):
    address: str
    amount: str
    index: str
    validatorIndex: str
