from pydantic import BaseModel, ConfigDict
import uuid
import datetime

from src.models.chain_stack_models.eth_withdrawal import ChainStackEthWithdrawal
from src.models.quick_node_models.eth_blocks import QuickNodeEthWithdrawal


class EthWithdrawalDTO(BaseModel):
    """
    DTO for quick_node.eth_withdrawals_table
    """

    id: uuid.UUID
    block_number: str
    address: str
    amount: str
    index: str
    validatorIndex: str
    created_at: datetime.datetime
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def from_quick_node_withdrawal(
        block_number: str, input: QuickNodeEthWithdrawal | ChainStackEthWithdrawal
    ) -> "EthWithdrawalDTO":
        return EthWithdrawalDTO(
            id=uuid.uuid4(),
            block_number=block_number,
            address=input.address,
            amount=input.amount,
            index=input.index,
            validatorIndex=input.validatorIndex,
            created_at=datetime.datetime.utcnow(),
        )
