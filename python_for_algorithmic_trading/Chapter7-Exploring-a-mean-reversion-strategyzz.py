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
from zipline.finance import commission, slippage
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.engine import SimplePipelineEngine
import matplotlib.pyplot as plt

# Set the number of longs and shorts and the lookback periods: 
N_LONGS = N_SHORTS = 50
MONTH = 21
YEAR = 12 * MONTH

# Create the mean reversion factor:
class MeanReversion(CustomFactor):
    inputs = [Returns(window_length=MONTH)]
    window_length = YEAR
    def compute(self, today, assets, out, monthly_returns):
        df = pd.DataFrame(monthly_returns)
        out[:] = df.iloc[-1].sub(
                df.mean()).div(df.std())

# Implement the fuction that returns the pipeline using the factor:
def make_pipeline():
    mean_reversion = MeanReversion()
    dollar_volume = AverageDollarVolume(
            window_length=30)
#     mean_reversion = MeanReversion(mask=dollar_volume.top(100)) 
    return Pipeline(
            columns={
                "longs": mean_reversion.bottom(N_LONGS),
                "shorts": mean_reversion.top(N_SHORTS),
                "ranking": mean_reversion.rank(
                    ascending=False),
            },
            screen=dollar_volume.top(100),
    )

# run in first time
def initialize(context):
    attach_pipeline(make_pipeline(), "factor_pipeline")
    schedule_function(
            rebalance,
            date_rules.week_start(), # trade frequency
        #     date_rules.every_day(),
            time_rules.market_open(),
            calendar=calendars.US_EQUITIES,
    )
    set_commission(
            us_equities=commission.PerShare(
                cost=0.00075, min_trade_cost=0.01
            )
    )
    set_slippage(
            us_equities=slippage.VolumeShareSlippage(
                volume_limit=0.00025, price_impact=0.01
            )
    )

# run before every trading day start
def before_trading_start(context, data):
    context.factor_data = pipeline_output("factor_pipeline")

# triggered by schedule_funtion
def rebalance(context, data):
    factor_data = context.factor_data
    record(factor_data=factor_data.ranking)
    assets = factor_data.index
    record(prices=data.current(assets, "price"))
    longs = assets[factor_data.longs]
    shorts = assets[factor_data.shorts]
    divest = set(context.portfolio.positions.keys()) - set(longs.union(shorts))
    print(
            f"{get_datetime().date()} | Longs {len(longs)} | Shorts | {len(shorts)} | {context.portfolio.portfolio_value}"
    )
    exec_trades(
            data,
            assets=divest,
            target_percent=0
    )
    exec_trades(
            data,
            assets=longs,
            target_percent=1/N_LONGS
    )
    exec_trades(
            data,
            assets=shorts,
            target_percent=-1/N_SHORTS
    )

# trade execution rules
def exec_trades(data, assets, target_percent):
    for asset in assets:
        if data.can_trade(
                asset) and not get_open_orders(asset):
            order_target_percent(
                    asset, target_percent)

# analyze after backtest, output a plot
def analyze(context, perf):
    perf.portfolio_value.plot()
    plt.show()

start = pd.Timestamp("2015")
end = pd.Timestamp("2016")
sp500 = web.DataReader('SP500', 'fred', start, end).SP500
benchmark_returns = sp500.pct_change()
# print(benchmark_returns.tail())

perf = run_algorithm(
        start=start,
        end=end,
        initialize=initialize,
        analyze=analyze,
        benchmark_returns=benchmark_returns,
        before_trading_start=before_trading_start,
        capital_base=1000000,
        bundle="my-local"
)

print(perf.info())

# save
perf.to_pickle("mean_reversion.pickle")

p = make_pipeline()
try:
    result = p.show_graph()
    if result:
        print(f"Graph result type: {type(result)}")
        # save svg
        svg_content = result.data
        with open('pipeline_graph.svg', 'w', encoding='utf-8') as f:
            f.write(svg_content)
        print("Pipeline graph saved as 'pipeline_graph.svg'")
        
except Exception as e:
    print(f"Error generating pipeline graph: {e}")
    
perf.beta.plot(title="Rolling beta of our algo's returns against the bechmark")
plt.show()

perf.alpha.plot(title="Rolling alpha of our algo's returns against the bechmark")
plt.show()