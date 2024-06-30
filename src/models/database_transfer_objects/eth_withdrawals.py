from pydantic import BaseModel, ConfigDict
import uuid
import datetime

from src.models.quick_node_models.eth_blocks import QuickNodeEthWithdrawal


class EthWithdrawalDTO(BaseModel):
    """
    DTO for quick_node.eth_withdrawals_table
    """

    id: uuid.UUID
    block_id: str
    address: str
    amount: str
    index: str
    validatorIndex: str
    created_at: datetime
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def from_quick_node_withdrawal(
        self, block_id: str, input: QuickNodeEthWithdrawal
    ) -> "EthWithdrawalDTO":
        return EthWithdrawalDTO(
            id=uuid.uuid4(),
            block_id=block_id,
            address=input.address,
            amount=input.amount,
            index=input.index,
            validatorIndex=input.validatorIndex,
            created_at=datetime.datetime.now(datetime.UTC),
        )
