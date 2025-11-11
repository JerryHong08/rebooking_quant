import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from openbb import obb
from scipy.stats import spearmanr
obb.user.preferences.output_type = "dataframe"

symbols = ["NEM", "RGLD", "SSRM", "CDE", "LLY", "UNH", "JNJ", "MRK"]
data = obb.equity.price.historical(
        symbols,
        start_date="2015-01-01",
        end_date="2022-12-31",
        provider="yfinance"
)
prices = data[["high", "low", "close", "volume", "symbol"]]

# preprocessing step, make sure all our tickers have at least two years of data.
# We'll create a mask and grab the stocks that meet our criteria
# size
nobs = prices.groupby("symbol").size()
# make sure size/nobs is over the minimum value.
mask = nobs[nobs > 2 * 12 *21].index
prices = prices[prices.symbol.isin(mask)]

# set the symbol column as an index, reorder, and drop duplicates
prices = (
        prices
        .set_index("symbol", append=True)
        .reorder_levels(["symbol", "date"])
        .sort_index(level=0)
).drop_duplicates()

def parkinson(data, window=14, trading_days=252):
    rs = (1.0 / (4.0 * np.log(2.0))) * ((
        data.high / data.low).apply(np.log))**2.0
    def f(v):
        return (trading_days * v.mean())**0.5
    result = rs.rolling(
            window=window,
            center=False
    ).apply(func=f)
    return result.sub(result.mean()).div(result.std())

# Volatility
prices["vol"] = (
        prices
        .groupby("symbol", group_keys=False)
        .apply(parkinson)
)
prices.dropna(inplace=True)


# Now that we have the normalized Parkinson volatility, we can compute historic and forward returns. 
# First, compute the historic returns over 1,5,10,21,42,and 63 periods representing one day through three month:
lags = [1, 5, 10, 21, 42, 63]
for lag in lags:
    prices[f"return_{lag}d"] = (
        prices
        .groupby(level="symbol")
        .close
        .pct_change(lag)
    )

# Compute the forward returns for the same periods
for t in lags:
    prices[f"target_{t}d"] = (
            prices
            .groupby(level="symbol")[f"return_{t}d"]
            .shift(-t)
    )

# Visualization
target = "target_1d"
metric = "vol"
j = sns.jointplot(x=metric, y=target, data=prices)
plt.tight_layout()
df = prices[[metric, target]].dropna()
# The stat coefficient measures the strength and direction of the relationship between the two variables, 
# with a value between -1 and 1. A value of 0 indicates no correlation, 1 indicates a perfect positive correlation, 
# and -1 indicates a perfect negative correlation.
stas, pvalue = spearmanr(df[metric], df[target])
# The pvalue tests the null hypothesis that the data is uncorrelated. A small pvalue(typically <= 0.05) indicates 
# that you can reject the null hypothesis, which suggests there is a statistically correlation between 
# the Parkinson volatility and the returns of this portfolio.

