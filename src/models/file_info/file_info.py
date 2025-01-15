from datetime import datetime

from pydantic import BaseModel


class FileInfo(BaseModel):
    """
    Represents a S3 file with its path and modified date
    """

    file_path: str
    modified_date: datetime
