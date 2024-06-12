def decrement_block_number(hexadecimal_block_number: str) -> str:
    """
    Given a hexadecimal string e.g 0x132582d
    convert it to an int (decimal), decrement it, and convert it back to a hexadecimal
    """
    if not hexadecimal_block_number.startswith("0x"):
        raise ValueError(
            f"Incorrect hexadecimal block number. does not start with 0x: {hexadecimal_block_number}"
        )
    base_10: int = int(hexadecimal_block_number[2:], 16)
    decremented_base_10: int = base_10 - 1
    decremented_block_number: str = hex(decremented_base_10)
    return decremented_block_number


if __name__ == "__main__":
    block_number: str = "0x5"  # expected decremented should be "0x4"
    decremented_block_number: str = decrement_block_number(block_number)
    print(decremented_block_number)
