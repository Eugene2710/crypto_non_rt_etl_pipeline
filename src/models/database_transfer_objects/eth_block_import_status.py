import uuid
import datetime
from pydantic import BaseModel, ConfigDict


class EthBlockImportStatusDTO(BaseModel):
    id: uuid.UUID
    block_number: int
    created_at: datetime.datetime
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @staticmethod
    def create_import_status(block_number: int) -> "EthBlockImportStatusDTO":
        return EthBlockImportStatusDTO(
            id=uuid.uuid4(),
            block_number=block_number,
            created_at=datetime.datetime.utcnow(),
        )
