import streamlit as st
import os
from typing import Sequence
import pandas as pd
from sqlalchemy import create_engine, Engine, TextClause, CursorResult, RowMapping, text
from src.chain_stack_eth_block_etl_pipeline import trigger_etl_pipeline

DATABASE_URL = os.getenv("POSTGRES_URL", "")

engine: Engine = create_engine(DATABASE_URL)


def toggle_button():
    st.session_state.button_state = not st.session_state.button_state


def query_database(query: str) -> pd.DataFrame:
    st.text(f"Running query: {query}")
    with engine.begin() as conn:
        query_text_clause: TextClause = text(query)
        cursor_result: CursorResult = conn.execute(query_text_clause)
        sequence_of_rows: Sequence[RowMapping] = cursor_result.mappings().all()
    df = pd.DataFrame(sequence_of_rows)
    return df


if "button_state" not in st.session_state:
    st.session_state.button_state = False

st.set_page_config(layout="wide")

st.header("Ethereum ETL Pipeline Dashboard")

# TODO: use a function to query the latest block number from your DAO
block_number: int = 0

st.text(f"Block Number: {block_number}")

if st.button("Trigger Pipeline"):
    st.text("Pipeline triggered!")
    trigger_etl_pipeline()

left_col, right_col = st.columns(2)

with left_col:
    st.button("Show Tables", on_click=toggle_button)
    if st.session_state.button_state:
        with st.expander("Table: eth_blocks"):
            st.code(
                """CREATE TABLE eth_blocks (
        block_number VARCHAR PRIMARY KEY,
        id INTEGER NOT NULL,  -- This ID comes from QuickNode; do not generate it
        jsonrpc VARCHAR NOT NULL,
        basefeepergas VARCHAR,
        blobgasused VARCHAR,
        difficulty VARCHAR NOT NULL,
        excessblobgas VARCHAR,
        extradata VARCHAR NOT NULL,
        gaslimit VARCHAR NOT NULL,
        gasused VARCHAR NOT NULL,
        hash VARCHAR,  -- Nullable for unsealed blocks
        logsbloom VARCHAR NOT NULL,
        miner VARCHAR NOT NULL,
        mixhash VARCHAR NOT NULL,
        nonce VARCHAR NOT NULL,
        number VARCHAR NOT NULL,
        parentbeaconblockroot VARCHAR,
        parenthash VARCHAR NOT NULL,
        receiptsroot VARCHAR NOT NULL,
        sha3uncles VARCHAR NOT NULL,
        size VARCHAR NOT NULL,
        stateroot VARCHAR NOT NULL,
        timestamp VARCHAR NOT NULL,
        totaldifficulty VARCHAR NOT NULL,
        transactionsroot VARCHAR NOT NULL,
        withdrawalsroot VARCHAR,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL  -- Date the row was inserted
    );"""
            )
        with st.expander("Table: eth_transactions"):
            st.code(
                """CREATE TABLE eth_transactions (
        hash VARCHAR PRIMARY KEY,  -- Transaction hash from QuickNode
        block_number VARCHAR NOT NULL,  -- Foreign key from eth_blocks.block_number
        block_id INTEGER,  -- From eth_blocks.block_id (can be NULL)
        blockhash VARCHAR,  -- Can be NULL for unsealed blocks
        chainid VARCHAR,
        from_address VARCHAR NOT NULL,
        gas VARCHAR NOT NULL,
        gasprice VARCHAR NOT NULL,
        input VARCHAR NOT NULL,
        maxfeepergas VARCHAR,
        maxpriorityfeepergas VARCHAR,
        nonce VARCHAR NOT NULL,
        r VARCHAR NOT NULL,
        s VARCHAR NOT NULL,
        to_address VARCHAR NOT NULL,
        transactionindex VARCHAR NOT NULL,
        type VARCHAR NOT NULL,
        v VARCHAR NOT NULL,
        value VARCHAR NOT NULL,
        yparity VARCHAR,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,  -- Date of row insertion
        CONSTRAINT transactions_to_blocks_fk FOREIGN KEY (block_number)
            REFERENCES eth_blocks (block_number)
    );"""
            )
        with st.expander("Table: eth_transaction_access_list"):
            st.code(
                """CREATE TABLE eth_transaction_access_list (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Generate UUID using PostgreSQL function
        transaction_hash VARCHAR NOT NULL,  -- Foreign key to eth_transactions.hash
        address VARCHAR NOT NULL,
        storagekeys TEXT[] NOT NULL,  -- PostgreSQL ARRAY of strings
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,  -- Date of row insertion
        CONSTRAINT transaction_access_list_to_transactions_fk FOREIGN KEY (transaction_hash)
            REFERENCES eth_transactions (hash)
    );"""
            )
        with st.expander("Table: eth_withdrawals"):
            st.code(
                """CREATE TABLE eth_withdrawals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Generate UUID using PostgreSQL function
    block_number VARCHAR NOT NULL,  -- Foreign key to eth_blocks.block_number
    address VARCHAR NOT NULL,
    amount VARCHAR NOT NULL,
    index VARCHAR NOT NULL,
    validatorindex VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,  -- Date of row insertion
    CONSTRAINT withdrawals_to_blocks_fk FOREIGN KEY (block_number)
        REFERENCES eth_blocks (block_number)
);"""
            )
        with st.expander("Table: eth_block_import_status"):
            st.code(
                """CREATE TABLE eth_block_import_status (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- Generate UUID using PostgreSQL function
    block_number INTEGER NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL  -- Date of row insertion
);
        );"""
            )
    st.text_area(
        "Edit your SQL Query here:",
        """SELECT * from eth_block_import_status;""",
        height=300,
        key="query",
    )

with right_col:
    # TODO: populate this dataframe with the query
    df = query_database(st.session_state["query"])
    st.dataframe(df)
