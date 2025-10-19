# Chapter 3
# Yield Animation
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import animation
from mpl_toolkits.mplot3d import Axes3D
from openbb import obb
obb.user.preferences.output_type = 'dataframe'

maturities = ['3m', '6m', '1y', '2y', '3y', '5y', '7y', '10y', '30y']
data = obb.fixedincome.government.treasury_rates(
    start_date='1985-01-01',
    provider='federal_reserve'
).dropna(how='all').drop(columns=['month_1', 'year_20'])
# print(data.columns)
data.columns = maturities

data['inverted'] = data['3m'] > data['10y']

print(data.head())

fig = plt.figure()
ax = fig.add_subplot(1,1,1)
line, = ax.plot([], [])

y_min = data[maturities].min().min()
y_max = data[maturities].max().max()

ax.set_xlim(0, len(maturities)-1)
ax.set_ylim(y_min, y_max)

ax.set_xticks(range(len(maturities)))
ax.set_xticklabels(maturities, rotation=45)

# ax.set_xticklabels(maturities)
# ax.set_yticklabels([i for i in range(2, 20, 2)])

ax.yaxis.set_label_position("left")
ax.yaxis.tick_left()

ax.set_xlabel('Time to Maturity')
ax.set_ylabel("Yield %")
ax.set_title("US Treasury Yield Curve")

def init_func():
    line.set_data([], [])
    return line,

def animate(i):
    x = range(0, len(maturities))
    y = data[maturities].iloc[i].values
    dt_ = data.index[i].strftime('%Y-%m-%d')
    if data.inverted.iloc[i]:
        line.set_color('r')
    else:
        line.set_color('y')
    line.set_data(x, y)
    ax.set_title(f'US Treasury Yield Curve {dt_}')
    return line,

ani = animation.FuncAnimation(
    fig,
    animate,
    init_func=init_func,
    frames=len(data.index),
    interval=10,
    blit=False
)
plt.show()