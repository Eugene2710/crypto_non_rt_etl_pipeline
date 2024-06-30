import uuid
import datetime
from pydantic import BaseModel


class EthBlockImportStatusDTO(BaseModel):
    id: uuid.UUID
    block_number: int
    created_at: datetime.datetime
