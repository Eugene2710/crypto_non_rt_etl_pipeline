import json
from typing import Any

import pytest

from src.extractors.chain_stack_block_extractor import ChainStackBlockExtractor
from src.models.chain_stack_models.eth_blocks import (
    ChainStackEthBlockInformationResponse,
)


@pytest.fixture
def expected_result() -> list[ChainStackEthBlockInformationResponse]:
    with open(
        "integration_tests/src/extractors/test_files/expected_transaction_results.json",
        "r",
    ) as file:
        result_str = file.read()
    deserialized_result_dict: list[dict[str, Any]] = json.loads(result_str)
    deserialized_result_model: list[ChainStackEthBlockInformationResponse] = [
        ChainStackEthBlockInformationResponse.model_validate(single_result)
        for single_result in deserialized_result_dict
    ]
    return deserialized_result_model


@pytest.fixture
def chain_stack_extractor() -> ChainStackBlockExtractor:
    return ChainStackBlockExtractor()


class TestChainStackBlockExtractor:
    """
    Integration Test:
    Query for 5 blocks worth of data

    0x13e16fa -> start_block_number = 20846330
    0x13e16fe -> end_block_number = 20846334

    Prepare -> pytest fixture
    Act
    Assert
    Tear Down -> pytest fixture
    """

    @pytest.mark.asyncio_cooperative
    async def test_extract(
        self,
        chain_stack_extractor: ChainStackBlockExtractor,
        expected_result: list[ChainStackEthBlockInformationResponse],
    ) -> None:
        """
        Test Plan:

        Fire extract to get 5 blocks worth of transactions

        The method should return the results as expected
        """
        # GIVEN we provide a range of 5 block numbers from 20846330 to 20846334
        # WHEN we query chain-stack
        result: list[ChainStackEthBlockInformationResponse] = (
            await chain_stack_extractor.extract(
                start_block_number=20846330, end_block_number=20846334
            )
        )
        # THEN i expect to get back 5 block numbers worth of results
        assert result == expected_result
