from sqlalchemy import (MetaData, Table, Column, String, DateTime, ForeignKey, UUID, Integer, Index)

import uuid

from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

# Origin of sqlalchemy; used to define the origin of sqlalchemy
Base = declarative_base()

# The orchestrator keeping track of all sqlalchemy.Table
# We assign this to alembic's target_metadata variable, to let alembic keep track of existing tables
Base = declarative_base()
metadata: MetaData = MetaData()

# https://docs.chainstack.com/reference/solana-getlatestblockhash
sol_blockhash_table: Table = Table(
    "sol_blockhash",
    metadata,
    Column("slot", Integer, primary_key=True),
    Column("blockhash", String, nullable=False),
    Column("lastValidBlockHeight", Integer, nullable=False)
)

# https://docs.chainstack.com/reference/solana-getblock
sol_block_table: Table = Table(
    "sol_block",
    metadata,
    Column("slot", Integer, primary_key=True),
    Column("block_height", Integer, primary_key=True),
    Column("block_time", DateTime, nullable=False),
    Column("blockhash", String, nullable=False),
    Column("parent_slot", Integer, nullable=False) # there must be a parent except for the genesis block which we likely not fetch anyway
    Column("previous_blockhash", String, nullable=False) # same as above
)

# https://docs.chainstack.com/reference/gettransaction
sol_transaction_table: Table = Table(
    "sol_transaction",
    metadata,
    Column("")
)