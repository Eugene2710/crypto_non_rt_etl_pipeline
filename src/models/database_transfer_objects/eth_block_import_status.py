import uuid
import datetime
from pydantic import BaseModel


class EthBlockImportStatusDTO(BaseModel):
    id: uuid.UUID
    block_number: int
    created_at: datetime.datetime

    @staticmethod
    def create_import_status(block_number: int) -> "EthBlockImportStatusDTO":
        return EthBlockImportStatusDTO(
            id=uuid.uuid4(),
            block_number=block_number,
            created_at=datetime.datetime.now(datetime.UTC),
        )
