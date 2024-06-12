import pytest

from src.utils.get_previous_block import decrement_block_number


class TestPreviousBlock:
    @pytest.mark.parametrize(
        "current_block, previous_block",
        [["0x10", "0xf"], ["0xf", "0xe"], ["0x2", "0x1"]],
    )
    def test_previous_block_should_decrement(
        self, current_block: str, previous_block: str
    ) -> None:
        assert decrement_block_number(current_block) == previous_block
