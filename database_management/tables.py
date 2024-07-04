import uuid

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    DateTime,
    ForeignKey,
    UUID,
    Integer,
    Index,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

# Origin of sqlalchemy; used to define the origin of sqlalchemy
Base = declarative_base()

# The orchestrator keeping track of all sqlalchemy.Table
# We assign this to alembic's target_metadata variable, to let alembic keep track of existing tables
metadata: MetaData = MetaData()

# data class: EthBlockDTO
eth_block_table = Table(
    "eth_blocks",
    metadata,
    Column("block_number", String, primary_key=True),
    Column(
        "id", Integer, nullable=False
    ),  # this id is from quick node; don't generate this
    Column("jsonrpc", String, nullable=False),
    Column("basefeepergas", String, nullable=True),
    Column("blobgasused", String, nullable=True),
    Column("difficulty", String, nullable=False),
    Column("excessblobgas", String, nullable=True),
    Column("extradata", String, nullable=False),
    Column("gaslimit", String, nullable=False),
    Column("gasused", String, nullable=False),
    Column("hash", String, nullable=True),  # null for an unsealed block
    Column("logsbloom", String, nullable=False),
    Column("miner", String, nullable=False),
    Column("mixhash", String, nullable=False),
    Column("nonce", String, nullable=False),
    Column("number", String, nullable=False),
    Column("parentbeaconblockroot", String, nullable=True),
    Column("parenthash", String, nullable=False),
    Column("receiptsroot", String, nullable=False),
    Column("sha3uncles", String, nullable=False),
    Column("size", String, nullable=False),
    Column("stateroot", String, nullable=False),
    Column("timestamp", String, nullable=False),
    Column("totaldifficulty", String, nullable=False),
    Column("transactionsroot", String, nullable=False),
    Column("withdrawalsroot", String, nullable=True),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)


# Transactions has a many to one relationship with blocks
# data class: EthTransactionDTO
eth_transaction_table: Table = Table(
    "eth_transactions",
    metadata,
    Column("hash", String, primary_key=True),  # transaction hash from quicknode
    Column(
        "block_number",
        String,
        ForeignKey("eth_blocks.block_number", name="transactions_to_blocks_fk"),
        nullable=False,
    ),  # this id is from quick node; don't generate this
    Column("block_id", Integer, nullable=True),  # from eth_blocks.block_id
    Column("blockhash", String, nullable=True),  # can be null for unsealed block
    Column("chainid", String, nullable=True),
    Column("from_address", String, nullable=False),
    Column("gas", String, nullable=False),
    Column("gasprice", String, nullable=False),
    Column("input", String, nullable=False),
    Column("maxfeepergas", String, nullable=True),
    Column("maxpriorityfeepergas", String, nullable=True),
    Column("nonce", String, nullable=False),
    Column("r", String, nullable=False),
    Column("s", String, nullable=False),
    Column("to_address", String, nullable=False),
    Column("transactionindex", String, nullable=False),
    Column("type", String, nullable=False),
    Column("v", String, nullable=False),
    Column("value", String, nullable=False),
    Column("yparity", String, nullable=True),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)


# AccessList has a many to one relationship with transactions
# data class: EthTransactionAccessListDTO
eth_transaction_access_list_table: Table = Table(
    "eth_transaction_access_list",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    # generate a unique id with uuid.uuid4() -> this is our own id as they didn't provide it
    Column(
        "transaction_hash",
        String,
        ForeignKey(
            "eth_transactions.hash", name="transaction_access_list_to_transactions_fk"
        ),
        nullable=False,
    ),
    Column("address", String, nullable=False),
    Column("storagekeys", ARRAY(String), nullable=False),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)

# Withdrawals has a many to one relationship with blocks
# data class: EthWithdrawalDTO
eth_withdrawals_table: Table = Table(
    "eth_withdrawals",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    # generate a unique id with uuid.uuid4() -> this is our own id as they didn't provide it
    Column(
        "block_number",
        String,
        ForeignKey("eth_blocks.block_number", name="withdrawals_to_blocks_fk"),
        nullable=False,
    ),
    Column("address", String, nullable=False),
    Column("amount", String, nullable=False),
    Column("index", String, nullable=False),
    Column("validatorindex", String, nullable=False),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)

# Q: Postgres has BTree and Hash index. Why did we use BTree?
# A: We will use the index to get the latest block_number; the query relies on an order on block_number
# Hash indexes don't support ordering
# BTree indexes do. Hence, we use BTree
eth_block_import_status_table: Table = Table(
    "eth_block_import_status",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("block_number", Integer, nullable=False),
    Column("created_at", DateTime, nullable=False),  # date you insert the row,
    Index("block_number_index", "block_number", postgresql_using="btree"),
)
