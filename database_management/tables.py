import uuid

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    DateTime,
    ForeignKey,
    UUID,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

# Origin of sqlalchemy; used to define the origin of sqlalchemy
Base = declarative_base()

# The orchestrator keeping track of all sqlalchemy.Table
# We assign this to alembic's target_metadata variable, to let alembic keep track of existing tables
metadata: MetaData = MetaData()

# data class: EthBlockDTO
eth_block_table: Table = Table(
    "eth_blocks",
    metadata,
    Column(
        "id", String, primary_key=True
    ),  # this id is from quick node; don't generate this
    Column("jsonrpc", String, nullable=False),
    Column("baseFeePerGas", String, nullable=False),
    Column("blobGasUsed", String, nullable=False),
    Column("difficulty", String, nullable=False),
    Column("excessBlobGas", String, nullable=False),
    Column("extraData", String, nullable=False),
    Column("gasLimit", String, nullable=False),
    Column("gasUsed", String, nullable=False),
    Column("hash", String, nullable=True),  # null for an unsealed block
    Column("logsBloom", String, nullable=False),
    Column("miner", String, nullable=False),
    Column("mixHash", String, nullable=False),
    Column("nonce", String, nullable=False),
    Column("number", String, nullable=False),
    Column("parentBeaconBlockRoot", String, nullable=False),
    Column("parentHash", String, nullable=False),
    Column("receiptsRoot", String, nullable=False),
    Column("sha3Uncles", String, nullable=False),
    Column("size", String, nullable=False),
    Column("stateRoot", String, nullable=False),
    Column("timestamp", String, nullable=False),
    Column("totalDifficulty", String, nullable=False),
    Column("transactionsRoot", String, nullable=False),
    Column("withdrawalsRoot", String, nullable=False),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)


# Transactions has a many to one relationship with blocks
# data class: EthTransactionDTO
eth_transaction_table: Table = Table(
    "eth_transactions",
    metadata,
    Column("hash", String, primary_key=True),  # transaction hash from quicknode
    Column(
        "block_id",
        String,
        ForeignKey("eth_blocks.id", name="transactions_to_blocks_fk"),
        nullable=False,
    ),  # this id is from quick node; don't generate this
    Column("blockHash", String, nullable=True),  # can be null for unsealed block
    Column("blockNumber", String, nullable=False),
    Column("chainId", String, nullable=True),
    Column("from", String, nullable=False),
    Column("gas", String, nullable=False),
    Column("gasPrice", String, nullable=False),
    Column("input", String, nullable=False),
    Column("maxFeePerGas", String, nullable=True),
    Column("maxPriorityFeePerGas", String, nullable=True),
    Column("nonce", String, nullable=False),
    Column("r", String, nullable=False),
    Column("s", String, nullable=False),
    Column("to", String, nullable=False),
    Column("transactionIndex", String, nullable=False),
    Column("type", String, nullable=False),
    Column("v", String, nullable=False),
    Column("value", String, nullable=False),
    Column("yParity", String, nullable=True),
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
    Column("storageKeys", ARRAY(String), nullable=False),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)

# Withdrawals has a many to one relationship with blocks
# data class: EthWithdrawalDTO
eth_withdrawals_table: Table = Table(
    "eth_withdrawals_table",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    # generate a unique id with uuid.uuid4() -> this is our own id as they didn't provide it
    Column(
        "block_id",
        String,
        ForeignKey("eth_blocks.id", name="withdrawals_to_blocks_fk"),
        nullable=False,
    ),
    Column("address", String, nullable=False),
    Column("amount", String, nullable=False),
    Column("index", String, nullable=False),
    Column("validatorIndex", String, nullable=False),
    Column("created_at", DateTime, nullable=False),  # date you insert the row
)
