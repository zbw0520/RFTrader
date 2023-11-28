"""
@author: Bowen Zhang
@software: pycharm
@file: ma_strategy
@time: 2023/1/18 16:56
@desc:
"""

import data.bstock as bs
import pandas as pd
import strategy.ma_strategy as ma
import matplotlib.pyplot as plt

code = "sh.000001"

price = bs.get_single_price_from_local(code)
rs = ma.ma_strategy(price)
print(pd.DataFrame(rs["profit_pct"]).describe())
plt.hist(rs["profit_pct"],bins=40)
plt.show()