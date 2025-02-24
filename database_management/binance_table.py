from sqlalchemy import MetaData, Table, Column, String, DateTime, Numeric, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.functions import now

Base = declarative_base()
metadata: MetaData = MetaData()

"""
CREATE TABLE IF NOT EXITS binance_ticker_metadata (
  symbol VARCHAR(20) NOT NULL, -- e.g. ETHBTC
  server_time TIMESTAMP NOT NULL, -- e.g time of data, returned by binance
  status VARCHAR(20) NOT NULL, -- e.g. TRADING
  base_asset VARCHAR(20) NOT NULL, -- e.g. ETH
  quote_asset VARCHAR(20) NOT NULL, -- e.g BTC
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  PRIMARY_KEY (symbol, server_time)
);
"""

binance_ticker_metadata_table = Table(
    "binance_ticker_metadata",
    metadata,
    Column("symbol", String, nullable=False, primary_key=True),
    Column("server_time", DateTime, nullable=False, primary_key=True),
    Column("status", String, nullable=False),
    Column("base_asset", String, nullable=False),
    Column("quote_asset", String, nullable=False),
    Column("created_at", DateTime, nullable=False, default=now()),
)

"""
CREATE TABLE IF NOT EXISTS binance_klines_prices(
  symbol VARCHAR(20) NOT NULL,
  kline_open_time TIMESTAMP NOT NULL,
  kline_close_time TIMESTAMP NOT NULL,
  open_price DOUBLE NOT NULL,
  high_price DOUBLE NOT NULL,
  low_price DOUBLE NOT NULL
  close_price DOUBLE NOT NULL,
  volume DOUBLE NOT NULL,
  quote_asset_vol DOUBLE NOT NULL,
  number_of_trades INTEGER NOT NULL,
  taker_buy_base_asset_vol DOUBLE NOT NULL,
  taker_buy_quote_asset_vol DOUBLE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  PRIMARY_KEY (symbol, kline_open_time)
)
"""

binance_klines_prices_table = Table(
    "binance_klines_prices",
    metadata,
    Column("symbol", String, nullable=False, primary_key=True),
    Column("kline_open_time", DateTime, nullable=False, primary_key=True),
    Column("kline_close_time", DateTime, nullable=False),
    Column("open_price", Numeric(precision=38, scale=18), nullable=False),
    Column("high_price", Numeric(precision=38, scale=18), nullable=False),
    Column("low_price", Numeric(precision=38, scale=18), nullable=False),
    Column("close_price", Numeric(precision=38, scale=18), nullable=False),
    Column("volume", Numeric(precision=38, scale=18), nullable=False),
    Column("quote_asset_volume", Numeric(precision=38, scale=18), nullable=False),
    Column("number of trades", Integer, nullable=False),
    Column("taker_buy_base_asset_vol", Numeric(precision=38, scale=18), nullable=False),
    Column(
        "taker_buy_quote_asset_vol", Numeric(precision=38, scale=18), nullable=False
    ),
    Column("created_at", DateTime, nullable=False, default=now()),
)
