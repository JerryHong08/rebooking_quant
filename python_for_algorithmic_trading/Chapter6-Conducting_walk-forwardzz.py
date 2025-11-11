import numpy as np
import scipy.stats as stats
import vectorbt as vbt
from openbb import obb
obb.user.preferences.output_type="dataframe"

start = "2016-01-01"
end = "2020-01-01"
prices = obb.equity.price.historical(
        "AAPL",
        start_date=start,
        end_date=end,
        provider='polygon'
).get('close')
# print(prices.head())
# print(prices.head())

# Create data splits for the walk-forward optimization. This code segments the prices into 30 splits, each two years long, and reserves 180 days for the test:
# in_prices -> train prices, out_price -> test prices
(in_price, in_indexes), (out_price, out_indexes) = prices.vbt.rolling_split(
        n=30,
        window_len=365 * 2,
        set_lens=(180,), # set_lens(90, 180) -> [========== train ==========][-- test_90 --][-- test_180 --]
        left_to_right=False,
)

print(type(in_price), np.shape(in_price))
print(type(out_price), np.shape(out_price))
# <class 'pandas.core.frame.DataFrame'> (550, 210)
# <class 'pandas.core.frame.DataFrame'> (180, 210)
i = 0

print("In-sample first date:", in_price[i].index[0], "last date:", in_price[i].index[-1])
print("Out-sample first date:", out_price[i].index[0], "last date:", out_price[i].index[-1])
# In-sample first date: 0 last date: 549
# Out-sample first date: 0 last date: 179

# 看第一个 split（注意 left_to_right 影响第一个是早期还是最近）
print("In-sample indices for split", i, ":", in_indexes[i])
print("Out-sample indices for split", i, ":", out_indexes[i])
# In-sample indices for split 0 : 
# Index([2016-01-04, 2016-01-05, 2016-01-06, 2016-01-07, 2016-01-08, 2016-01-11,
#        2016-01-12, 2016-01-13, 2016-01-14, 2016-01-15,
#        ...
#        2018-02-26, 2018-02-27, 2018-02-28, 2018-03-01, 2018-03-02, 2018-03-05,
#        2018-03-06, 2018-03-07, 2018-03-08, 2018-03-09],
#       dtype='object', name='split_0', length=550)
# Out-sample indices for split 0 : 
# Index([2018-03-12, 2018-03-13, 2018-03-14, 2018-03-15, 2018-03-16, 2018-03-19,
#        2018-03-20, 2018-03-21, 2018-03-22, 2018-03-23,
#        ...
#        2018-11-09, 2018-11-12, 2018-11-13, 2018-11-14, 2018-11-15, 2018-11-16,
#        2018-11-19, 2018-11-20, 2018-11-21, 2018-11-23],
#       dtype='object', name='split_0', length=180)

# This function returns the Sharpe ratios for all combinations of moving average windows:
def simulate_all_params(price, windows, **kwargs):
    fast_ma, slow_ma = vbt.MA.run_combs(
            price,
            windows,
            r=2,  # 表示「两两组合」，即生成所有 (fast, slow) 配对。
            short_names=["fast", "slow"]
    )
    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)
    pf = vbt.Portfolio.from_signals(price, entries, exits, **kwargs)

    return pf.sharpe_ratio()

# These two functions return the indexes and parameters where the performance is maximized:
def get_best_index(performance):
    return performance[
            performance.groupby("split_idx").idxmax()
    ].index
def get_best_params(best_index, level_name):
    return best_index.get_level_values(
        level_name).to_numpy()

# Function that runs the backtest given the best moving average values and returns the associated Sharpe ratio:
def simulate_best_params(price, best_fast_windows,
  best_slow_windows, **kwargs):
    fast_ma = vbt.MA.run(
            price,
            window=best_fast_windows,
            per_column=True
    )
    slow_ma = vbt.MA.run(
            price,
            window=best_slow_windows,
            per_column=True
    )
    entries = fast_ma.ma_crossed_above(slow_ma)
    eixts = fast_ma.ma_crossed_below(slow_ma)
    pf = vbt.Portfolio.from_signals(
        price, entries, eixts, **kwargs)
    return pf.sharpe_ratio()

# Run the analysis by passing in a range of moving average windows to simulate_all_params. This returns the Sharpe ratio for every combination of moving average windows for every data split. In other words, these are the in-sample Sharpe ratios:
# windows = [10 11 12 ... 39]
windows = np.arange(10, 40)
# test data sharpe ratio for all windows value simulated with vbt rum_comb method.
in_sharpe = simulate_all_params(
        in_price,
        windows,
        direction="both",
        freq="d"
)
print(in_sharpe)
# Next, we will get the best in-sample moving average windows and conbine them into a single array:
in_best_index = get_best_index(in_sharpe)
in_best_fast_windows = get_best_params(in_best_index,"fast_window")
in_best_slow_windows = get_best_params(in_best_index,"slow_window")
# in_best_window_pairs = np.array(
#         list(
#             zip(
#                 in_best_fast_windows,
#                 in_best_slow_windows
#             )
#         )
# )

# retrive the out-of-sample Sharpe ratios using the optimized moving average windows
out_test_sharpe = simulate_best_params(
        out_price,
        in_best_fast_windows,
        in_best_slow_windows,
        direction="both",
        freq="d"
)
print(out_test_sharpe)

# It's common to overfit bakctesting models to market noise. This is especially acute
# when brute force optimizing techinical analysis strategies. To collect evidence to this 
# effect, we can use a one-sided independent t-test to assess the statistical significance 
# between the means of Sharpe ratios for in-sample and out-of-sample datasets:
in_sample_best = in_sharpe[in_best_index].values
out_sample_test = out_test_sharpe.values
t, p = stats.ttest_ind(
        a=out_sample_test,
        b=in_sample_best,
        alternative="greater"
)

# t stands for mean difference relative to volatility/noise
# t > 0 -> a > b; t < 0 -> a < b;

# p stands for credibility, the lower p (p < 0.05) means the null hypothesis can not be true.
# for single-sided tttest, null hypothesis: a <= b, H1: a > b.  (be carefull for the a,b value chosen)
print(t, p)

# the t is negative, p is around 0.756.
# it means a < b, and we don't have the statistical evidence of (a > b).

# which in conclusion, we can't trust this strategy will work out in unseen data.
