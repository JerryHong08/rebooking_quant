import pandas as pd
from dataclasses import dataclass, field

TRADE_BAR_PROPERTIES = ["time", "open", "high", "low", "close", "volume"]
DEFAULT_MARKET_DATA_ID = 55

@dataclass
class Tick:
    time : int
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    timestamp_: pd.Timestamp = field(init=False)
    def __post_init_(self):
        self.timestamp_ = pd.to_datetime(self.time, unit="s")
        self.bid_price = float(self.bid_price)
        self.ask_price = float(self.ask_price)
        self.bid_size = int(self.bid_size)
        self.ask_size = int(self.ask_size)

CREATE_BID_ASK_DATA = """
    CREATE TABLE IF NOT EXISTS bid_ask_data
        (
            timestamp DATETIME,
            symbol, STRING,
            bid_price REAL,
            ask_price, REAL,
            bid_size, INTEGER,
            ask_size, INTEGER
        )"""

CREATE_OPEN_ORDERS = """
    CREATE TABLE IF NOT EXISTS open_orders
    (
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        order_id INTEGER,
        symbol STRING,
        sec_type STRING,
        exchange STRING,
        action STRING,
        order_type STRING,
        quantity INTEGER,
        status STRING
    )"""

CREATE_TRADES = """
    CREATE TABLE IF NOT EXISTS trades
    (
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        request_id INTEGER,
        order_id INTERGER,
        execution_id INTEGER,
        symbol STRING,
        sec_type STRING,
        currency STRING,
        quantity INTEGER,
        last_liquidity REAL
    )"""
