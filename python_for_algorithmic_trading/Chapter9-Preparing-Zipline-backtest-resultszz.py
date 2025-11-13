import pandas as pd
from openbb import obb
import pyfolio as pf
obb.user.preferences.output_type = "dataframe"

perf = pd.read_pickle("mean_reversion.pickle")

# Use the Pyfolio helper function to extract returns, positions, and transactions from the DataFrame:
returns, positions, transactions = \
        pf.utils.extract_rets_pos_txn_from_zipline(perf)

print(f"Debug returns :\n{returns}")
print(f"Debug positions:\n{positions}")
print(f"Debug transactions:\n{transactions}")

# The positions DataFrames contain the Zipline Equity object as column labels. Replace the object with the string representations:
positions.columns = [col.symbol for col in positions.columns[:-1]] + ["cash"]

print(f"Debug positions:\n{positions}")

# The symbol column in the transactions DataFrame also contains the Zipline Equity object. Replace the object with the string representations.
transactions.symbol = transactions.symbol.apply(
        lambda s: s.symbol)

symbols = positions.columns[:-1].tolist()
screener_data = obb.equity.profile(
        symbols, provider="fmp")
print(f'Debug screener_data:{screener_data}')
sector_map = (
        screener_data[["symbol", "sector"]]
        .set_index("symbol")
        .reindex(symbols)
        .fillna("Unknow")
        .to_dict()["sector"]
)
print('Debug sector_map: \n{sector_map}')


spy = obb.equity.price.historical(
        "SPY",
        start_date=returns.index.min(),
        end_date=returns.index.max(),
        provider='polygon'
)
spy.index = pd.to_datetime(spy.index)
benchmark_returns = spy.close.pct_change()
benchmark_returns.name = "SPY"
benchmark_returns = benchmark_returns.tz_localize(
        "UTC").filter(returns.index)
