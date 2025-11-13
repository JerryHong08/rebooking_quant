# This is a example of my data loader from another project.
# You can use it as a reference of how to load and save it locally,
# then write the .zipline/extension.py to register your local data.
# After that, you can finnally ingest your local data for subsequent 
# zipline backtest.
from cores.data_loader import stock_load_process
import pandas as pd
from cores.config import data_dir
import os

df = stock_load_process(
    tickers=None,
    start_date='2015-01-01',
    end_date='2016-01-01',
    use_cache=False
).collect().to_pandas()

# make sure the timezone is UTC
df["timestamps"] = pd.to_datetime(df["timestamps"], utc=True)

df = df.rename(columns={
    "ticker": "symbol",
    "timestamps": "date"
})
df = df[["symbol","date","open","high","low","close","volume"]]

file_path = os.path.join(data_dir, "processed/us_stocks_sip/zipline_input.parquet")
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df.to_parquet(file_path)
