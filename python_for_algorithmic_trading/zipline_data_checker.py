import os
import numpy as np
import pandas as pd
from zipline.data.bundles.core import load
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.factors import AverageDollarVolume, CustomFactor, Returns
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.data.bundles.core import bundles

# 手动导入扩展文件
import sys
sys.path.append(os.path.expanduser('~/.zipline'))
try:
    import extension
    print("✅ Extension loaded successfully")
except ImportError as e:
    print(f"❌ Failed to load extension: {e}")
    
print("Registered bundles:")
for name, bundle in bundles.items():
    print(f"  {name}: {bundle}")

bundle_data = load("my-local")

# 调试：查看原始数据
print("\n=== 检查原始数据文件 ===")
df = pd.read_parquet("/mnt/blackdisk/quant_data/polygon_data/processed/us_stocks_sip/zipline_input.parquet")
print(f"原始数据形状: {df.shape}")
print(f"列名: {df.columns.tolist()}")
print(f"日期范围: {df['date'].min()} 到 {df['date'].max()}")
print(f"股票数量: {df['symbol'].nunique()}")
print(f"样本数据:")
print(df.head())

# 调试：查看 asset finder
print("\n=== 检查 Asset Finder ===")
print(f"Asset finder 类型: {type(bundle_data.asset_finder)}")
try:
    # 获取所有资产
    all_assets = bundle_data.asset_finder.retrieve_all(bundle_data.asset_finder.sids)
    print(f"总资产数量: {len(all_assets)}")
    
    if len(all_assets) > 0:
        # 查看前几个资产的信息
        print(f"\n前5个资产信息:")
        for i, asset in enumerate(all_assets[:5]):
            print(f"  Asset {i}: {asset}")
            print(f"    Symbol: {asset.symbol}")
            print(f"    Exchange: {asset.exchange}")
            print(f"    Country: {getattr(asset, 'country_code', 'N/A')}")
            print(f"    Start date: {asset.start_date}")
            print(f"    End date: {asset.end_date}")
            
    # 检查特定日期范围内的资产
    print(f"\n=== 检查2015年的资产 ===")
    start_date = pd.Timestamp("2015-01-05")
    end_date = pd.Timestamp("2015-12-30")
    
    # 查找在这个日期范围内交易的资产
    active_assets = []
    for asset in all_assets:
        if asset.start_date <= end_date and asset.end_date >= start_date:
            active_assets.append(asset)
    
    print(f"在 {start_date} 到 {end_date} 期间交易的资产数量: {len(active_assets)}")
    
    if len(active_assets) > 0:
        print(f"前5个活跃资产:")
        for i, asset in enumerate(active_assets[:5]):
            print(f"  {i}: {asset.symbol} (country: {getattr(asset, 'country_code', 'N/A')})")
    
except Exception as e:
    print(f"获取资产信息时出错: {e}")

# 调试：检查数据读取器
print("\n=== 检查数据读取器 ===")
try:
    reader = bundle_data.equity_daily_bar_reader
    print(f"数据读取器类型: {type(reader)}")
    print(f"可用的资产 sids: {reader.sids[:10]}...")  # 显示前10个
    
    # 检查第一个资产的数据
    if len(reader.sids) > 0:
        first_sid = reader.sids[0]
        print(f"第一个 SID: {first_sid}")
        
        # 尝试读取数据
        try:
            data = reader.load_raw_arrays(
                ["close"], 
                start_date, 
                end_date, 
                [first_sid]
            )
            print(f"成功读取数据，形状: {data[0].shape}")
        except Exception as e:
            print(f"读取数据时出错: {e}")
            
except Exception as e:
    print(f"检查数据读取器时出错: {e}")

print("\n=== 如果没有错误，将尝试运行 pipeline ===")