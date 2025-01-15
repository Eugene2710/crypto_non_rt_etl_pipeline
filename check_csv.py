from database_management.tables import eth_block_table
from sqlalchemy.sql.base import ReadOnlyColumnCollection
import pandas as pd

# To copy the entire table into CSV
# \COPY eth_blocks TO '/Users/eugeneleejunping/crypto_non_rt_etl_pipeline/eth_blocks_20250106.csv' WITH (FORMAT 'csv', header TRUE)

# To copy part of the table into CSV (not needed)
# \COPY eth_block(block_number, id) TO '/Users/eugeneleejunping/crypto_non_rt_etl_pipeline/eth_blocks_partial_20250106.csv' WITH (FORMAT 'csv', header TRUE)

# Step 1: Get the column order of the eth_blocks sqlAlchemy.Table
# expected output: ['block_number', 'id', 'jsonrpc', 'basefeepergas', 'blobgasused', 'difficulty', 'excessblobgas', 'extradata', 'gaslimit', 'gasused', 'hash', 'logsbloom', 'miner', 'mixhash', 'nonce', 'number', 'parentbeaconblockroot', 'parenthash', 'receiptsroot', 'sha3uncles', 'size', 'stateroot', 'timestamp', 'totaldifficulty', 'transactionsroot', 'withdrawalsroot', 'created_at']
eth_block_table_columns: ReadOnlyColumnCollection = eth_block_table.columns
eth_block_table_columns_names: list[str] = [
    single_column.name for single_column in eth_block_table_columns
]

print("eth block table column names")
print(eth_block_table_columns_names)

# Step 2: Read the csv file into a pd.DataFrame
# A pd.DataFrame is a column attribute as well, it returns a pd.Index containing columns
# pd.Index is a sequence of column names; list-like
# But we want to do a direct comparison between two lists, so we will cast this into a list
df: pd.DataFrame = pd.read_csv("eth_blocks_20250106.csv")
df_columns: pd.Index = df.columns
df_columns_list: list[str] = list(df_columns)

print("df column names")
print(df_columns_list)

assert eth_block_table_columns_names == df_columns_list, "column names do not match"
