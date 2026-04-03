from pydantic import BaseModel
from datetime import datetime


class S3ToDBImportStatusDTO(BaseModel):
    data_source: str
    file_modified_date: datetime
    created_at: datetime
