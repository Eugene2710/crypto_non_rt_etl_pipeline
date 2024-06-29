from src.extractors.abstract_extractor import BaseExtractor
from src.models.quick_node_models.eth_blocks import QuickNodeEthBlockInformationResponse


class QuickNodeBlockExtractor(BaseExtractor[QuickNodeEthBlockInformationResponse]):
    def __init__(self, import_status_dao: "ImportStatusDAO") -> None:
        self.import_status_dao: "ImportStatusDAO" = import_status_dao

    async def extract(self) -> QuickNodeEthBlockInformationResponse:
        pass
