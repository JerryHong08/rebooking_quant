"""
thetadata python sdk is deprecated, so the python library thetadata cannot be installed, 
this code won't work. See other code file in the Chapter 13 directory.
"""
import datetime as dt
import thetadata.client


ticker = "SPY"
expiration_date = dt.date(2025, 5, 2)
strike = 474

from thetadata import (
        Quote,
        StreamMsg,
        ThetaClient,
        OptionRight,
        StreamMsgType,
        StreamResponseType
)

def callback(msg):
    if msg.type == StreamMsgType.TRADE:
        print(
                "----------------------"
        )
        print(f"Contract: {msg.contract.to_string()}")
        print(f"Trade: {msg.trade.to_string()}")
        print(f"Last quote at time of trade: {
            msg.quote.to_string()}")

def stream_all_trades():
    client = ThetaClient(
            username="strimp101@gmail.com",
            passwd="PASSWORD"
    )
    client.connect_stream(
            callback
    )
    req_id = client.req_full_trade_stream_opt()
    response = client.verify(req_id)
    if (
            client.verify(req_id) != StreamResponseType.SUBSCRIBED
    ):
        raise Exception("Unable to stream.")

def stream_contract():
    client = ThetaClient(
            username="strimp101@gmail.com",
            passwd="PASSWORD"
    )
    client.connect_stream(
            callback
    )
    req_id = client.req_trade_stream_opt(
            ticker, expiration_date, strike, OptionRight.CALL)
    response = client.verify(req_id)
    if (
            client.verify(req_id) != StreamResponseType.SUBSCRIBED
    ):
        raise Exception("Unable to stream.")


stream_contract()
# stream_all_trades()
