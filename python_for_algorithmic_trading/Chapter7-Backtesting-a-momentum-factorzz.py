import pandas as pd
import numpy as np
from zipline import run_algorithm
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, CustomFactor, Returns
from zipline.api import (
        attach_pipeline,
        calendars,
        pipeline_output,
        date_rules,
        time_rules,
        set_commission,
        set_slippage,
        record,
        order_target_percent,
        get_open_orders,
        get_datetime,
        schedule_function
)
import pandas_datareader as web
import os
from zipline.data.bundles.core import load
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.engine import SimplePipelineEngine

# import sys
# sys.path.append(os.path.expanduser('~/.zipline'))
# try:
#     import extension
#     print("✅ Extension loaded successfully")
# except ImportError as e:
#     print(f"❌ Failed to load extension: {e}")

# define the number of long and short stocks we want in out portfolio:
N_LONGS = N_SHORTS = 50

# Custom momentum factor
class MomentumFactor(CustomFactor):
    inputs = [USEquityPricing.close, Returns(window_length=126)]
    window_length = 40
    def compute(self, today, assets, out, prices, returns):
        out[:] = (
                (prices[-21] - prices[-40]) / prices[-40]
                - (prices[-1] - prices[-21]) / prices[-21]
        ) / np.nanstd(returns, axis=0)
# Pipeline
def make_pipeline():
    momentum = MomentumFactor()
    dollar_volume = AverageDollarVolume(
            window_length=30)
    return Pipeline(
            columns={
                "factor": momentum,
                "longs": momentum.top(N_LONGS),
                "shorts": momentum.bottom(N_SHORTS),
                "ranking": momentum.rank(),
            },
            screen=dollar_volume.top(100),
    )
# Zipline Reloaded is an event-driven backtesting framework that allows us to "hook" into differnt events, including an events that fires before trading starts. We use this hook to "install" our factor pipeline:
def before_trading_start(context, data):
    context.factor_data = pipeline_output("factor_pipeline")

# define the initialize function, which is run when backtest starts:
def initialize(context):
    attach_pipeline(make_pipeline(), "factor_pipeline")
    schedule_function(
            rebalance,
            date_rules.week_start(),
            time_rules.market_open(),
            calendar=calendars.US_EQUITIES,
    )

# define a function that contains the logic to rebalance our portfolio. Here we buy the top N_LONGS stocks with the highest ranking factor the short the bottom N_SHORTS stocks with the lowest ranking factor:
def rebalance(context, data):
    factor_data = context.factor_data
    record(factor_data=factor_data.ranking)
    assets = factor_data.index
    record(prices=data.current(assets, "price"))
    longs = assets[factor_data.longs]
    shorts = assets[factor_data.shorts]
    divest = set(context.portfolio.positions.keys()) - set(longs.union(shorts))
    exec_trades(
        data,
        assets=divest,
        target_percent=0
    )
    exec_trades(
        data,
        assets=longs,
        target_percent= 1 / N_LONGS
    )
    exec_trades(
        data,
        assets=shorts,
        target_percent = -1 / N_SHORTS
    )

# we abstract wat the order execution in an exec_trades function, which loops through the provided assets and executes the orders:
def exec_trades(data, assets, target_percent):
    for asset in assets:
         if data.can_trade(
                 asset) and not get_open_orders(asset):
             order_target_percent(asset, target_percent)

# Finally, we run the backtesting using the run_algorithm function:
start = pd.Timestamp("2015")
end = pd.Timestamp("2016")
perf = run_algorithm(
    start=start,
    end=end,
    initialize=initialize,
    before_trading_start=before_trading_start,
    capital_base=100000,
    bundle="my-local",
)
# The output is a DataFrame that contains traing, riskm and performance statistics for each day in the backtest:

import matplotlib.pyplot as plt
print(perf.info())

perf.portfolio_value.plot(title="Cumulative returns")
plt.show()

perf.returns.hist(bins=50)
plt.show()

perf.sharpe.plot()
plt.show()
