# ~/.zipline/extension.py
# This file is for ~/.zipline/extension.py, since I have the local stock data but don't have the access of quandl/Nasdaq Data Link api.
# I first load my local data and write the a-year data into my shared data path. and then zipline ingest it.
# This extension.py file is for zipline to ingest your custom local data. For my case, the splits&dividends are already adjusted,
# So I just the splits&divideneds are created empty but should be passed into the zipline. Others will be nomarl.
# You can write your own load_data and save the shared data into your file path.
# Then you can use your local data instead of 'quandl' to walk through the Chapter5 and Chapter7 or any other situations when it comes to Zipline-Reloaded.
# Use `zipline_data_checker.py` to check the data health.
from zipline.data.bundles import register
import pandas as pd
import os

def my_bundle(environ,
              asset_db_writer,
              minute_bar_writer,
              daily_bar_writer,
              adjustment_writer,
              calendar,
              start_session,
              end_session,
              cache,
              show_progress,
              output_dir):

    # you should replace to your file path.
    df = pd.read_parquet("/mnt/blackdisk/quant_data/polygon_data/processed/us_stocks_sip/zipline_input.parquet")

    # turn into UTC for zipline
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values(["symbol", "date"])
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")

    before, after = len(df), len(df.drop_duplicates(subset=["symbol", "date"]))
    print(f"before: {before}, after: {after}")

    # group
    grouped = {}
    for symbol, data in df.groupby("symbol"):
        symbol = symbol.strip().upper()
        data = data.set_index("date")[["open", "high", "low", "close", "volume"]].sort_index()
        data = data.astype(float).ffill()
        grouped[symbol] = data

    # metadata
    metadata = []
    for symbol, data in grouped.items():
        metadata.append({
            "symbol": symbol.strip().upper(),
            "asset_name": symbol.strip().upper(),
            "start_date": data.index.min(),
            "end_date": data.index.max(),
            "first_traded": data.index.min(),
            "auto_close_date": data.index.max() + pd.Timedelta(days=1),
            "exchange": "NASDAQ",
            "country_code": "US",
        })

    metadata = pd.DataFrame(metadata).reset_index(drop=True)

    print(f"Country codes: {metadata['country_code'].unique()}")

    dups = metadata["symbol"].duplicated(keep=False)
    if dups.any():
        print("⚠️ Duplicate symbols found:")
        print(metadata.loc[dups, "symbol"].unique())
        metadata = metadata.drop_duplicates(subset=["symbol"], keep="first")

    # write asset data
    asset_db_writer.write(
        equities=metadata,
        exchanges=pd.DataFrame({
            "exchange": ["NASDAQ"],
            "canonical_name": ["NASDAQ"],
            "country_code": ["US"],
        })
    )

    # empty splits dataframe, since my local data has been adjusted already.
    splits = pd.DataFrame({
        'sid': [],
        'effective_date': [],
        'ratio': []
    }).astype({
        'sid': 'int64',
        'effective_date': 'int64',
        'ratio': 'float64'
    })

    dividends = pd.DataFrame({
        'sid': [],
        'ex_date': [],
        'pay_date': [],
        'record_date': [],
        'declared_date': [],
        'amount': []
    }).astype({
        'sid': 'int64',
        'ex_date': 'int64',
        'pay_date': 'int64',
        'record_date': 'int64',
        'declared_date': 'int64',
        'amount': 'float64'
    })

    mergers = pd.DataFrame({
        'sid': [],
        'effective_date': [],
        'ratio': []
    }).astype({
        'sid': 'int64',
        'effective_date': 'int64',
        'ratio': 'float64'
    })

    stock_dividends = pd.DataFrame({
        'sid': [],
        'ex_date': [],
        'pay_date': [],
        'record_date': [],
        'declared_date': [],
        'payment_sid': [],
        'ratio': []
    }).astype({
        'sid': 'int64',
        'ex_date': 'int64',
        'pay_date': 'int64',
        'record_date': 'int64',
        'declared_date': 'int64',
        'payment_sid': 'int64',
        'ratio': 'float64'
    })

    adjustment_writer.write(
        splits=splits,
        mergers=mergers,
        dividends=dividends,
        stock_dividends=stock_dividends
    )

    sids = range(len(metadata))
    data_iter = ((sid, grouped[symbol]) for sid, symbol in enumerate(metadata["symbol"]))
    daily_bar_writer.write(data_iter, show_progress=show_progress)

    print(f"✅ Loaded {len(metadata)} assets")
    print(f"📅 Date range: {df['date'].min()} — {df['date'].max()}")
    print(f"🧾 Bundle output directory: {output_dir}")

register("my-local", my_bundle)