import pandas as pd
import vectorbt as vbt
from openbb import obb
import matplotlib.pyplot as plt
obb.user.preferences.output_type='dataframe'

start = "2016-01-01"
end = "2020-01-01"
prices = obb.equity.price.historical(
        ["FB", "AAPL", "AMZN", "NFLX", "GOOG"],
        start_date=start,
        end_date=end,
        provider='polygon'
).pivot(columns='symbol', values='close')

print(prices.head())

fast_ma = vbt.MA.run(prices, 10, short_name="fast")
slow_ma = vbt.MA.run(prices, 30, short_name="slow")

print(type(fast_ma))
print(dir(fast_ma))
print(fast_ma.ma_crossed_above)

entries = fast_ma.ma_crossed_above(slow_ma)
exits = fast_ma.ma_crossed_below(slow_ma)


# buy and also sell
pf = vbt.Portfolio.from_signals(prices, entries, exits)
# print(pf.orders.stats(group_by=True))

strategy_returns = pf.total_return().groupby("symbol").mean()
print("Strategy Returns:")
print(strategy_returns)

plt.figure(figsize=(10, 6))
strategy_returns.plot(kind='bar')
plt.title("Strategy Returns")
plt.ylabel("Total Return")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# only buy and hold
buy_hold_pf = vbt.Portfolio.from_holding(prices, freq='1d')
buy_hold_returns = buy_hold_pf.total_return().groupby("symbol").mean()
print("\nBuy and Hold Returns:")
print(buy_hold_returns)

plt.figure(figsize=(10, 6))
buy_hold_returns.plot(kind='bar')
plt.title("Buy and Hold Returns")
plt.ylabel("Total Return")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# There's more
mult_prices, _ = prices.vbt.range_split(n=4)

fast_ma = vbt.MA.run(mult_prices, [10,20], short_name="fast")
slow_ma = vbt.MA.run(mult_prices, [30,30], short_name="slow")

entries = fast_ma.ma_crossed_above(slow_ma)
exits = fast_ma.ma_crossed_below(slow_ma)

pf = vbt.Portfolio.from_signals(
        mult_prices,
        entries,
        exits,
        freq="1D"
)

mult_returns = pf.total_return().groupby(['split_idx', 'symbol']).mean().unstack(level=-1)
plt.figure(figsize=(10,6))
mult_returns.plot(kind='bar')
plt.title('Multi Returns')
plt.ylabel('Total Return')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()