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

# os.environ["QUANDL_API_KEY"] = "6zUv7adBiJQdG_DZsAxz"
# bundle_data = load("quandl", os.environ, None)
bundle_data = load("my-local")
pipeline_loader = USEquityPricingLoader(
        bundle_data.equity_daily_bar_reader,
        bundle_data.adjustment_reader,
        fx_reader=None
)

engine = SimplePipelineEngine(
        get_loader=lambda col: pipeline_loader,
        asset_finder=bundle_data.asset_finder
)

class MomentumFactor(CustomFactor):
    inputs = [USEquityPricing.close, Returns(window_length=126)]
    window_length = 252
    def compute(self, today, assets, out, prices, returns):
        out[:] = (
                # I only prepare one year length of local data so I changed 252 to 40.
                (prices[-21] - prices[-40]) / prices[-40]
                - (prices[-1] - prices[-21]) / prices[-21]
        ) / np.nanstd(returns, axis=0)

def make_pipeline():
    momentum = MomentumFactor()
    dollar_volume = AverageDollarVolume(
            window_length=30)
    return Pipeline(
            columns={
                "factor": momentum,
                "longs": momentum.top(50),
                "shorts": momentum.bottom(50),
                "rank": momentum.rank()
            },
            screen=dollar_volume.top(100)
    )

# Pipeline:
#   factor
#   longs
#   shorts
#   rank
#   screen/pre-filter
#   start
#   end
results = engine.run_pipeline(
        make_pipeline(),
        pd.to_datetime("2015-01-05"),
        pd.to_datetime("2015-12-30")
)

results.dropna(subset="factor", inplace=True)
results.index.names = ["date", "symbol"]
results.sort_values(by=["date", "factor"], inplace=True)

print(results)