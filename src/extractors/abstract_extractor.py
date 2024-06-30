from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

EXTRACTED_BASE_MODEL = TypeVar("EXTRACTED_BASE_MODEL", bound=BaseModel)


class BaseExtractor(ABC, Generic[EXTRACTED_BASE_MODEL]):
    """
    An extractor is responsible for extracting data from a data source like quicknode

    this is an abstract extractor (unimplemented extractor) that enforces extractors to follow this implementation:

    they must implement an async extract method
    """

    @abstractmethod
    async def extract(
        self, start_block_number: int, end_block_number: int
    ) -> EXTRACTED_BASE_MODEL:
        raise NotImplementedError()
