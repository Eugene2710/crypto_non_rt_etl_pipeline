from pydantic import BaseModel


class QuickNodeEthWithdrawal(BaseModel):
    address: str
    amount: str
    index: str
    validatorIndex: str
