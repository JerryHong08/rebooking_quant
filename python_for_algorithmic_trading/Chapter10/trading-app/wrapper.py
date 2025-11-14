from ibapi.wrapper import EWrapper
import threading

class IBWrapper(EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        self.historical_data = {}
        self.streaming_data = {}
        self.stream_event = threading.Event()

    def historicalData(self, request_id, bar):
        bar_data = (
                bar.date,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
        )
        if request_id not in self.historical_data.keys():
            self.historical_data[request_id] = []
        self.historical_data[request_id].append(
                bar_data)

    def tickByTickBidAsk(
            self,
            request_id,
            time,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            tick_atrrib_last
    ):
        tick_data = (
                time,
                bid_price,
                ask_price,
                bid_size,
                ask_size,
        )
        self.streaming_data[request_id] = tick_data
        self.stream_event.set()
