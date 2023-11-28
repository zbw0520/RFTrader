"""
@author: Bowen Zhang
@software: pycharm
@file: Test3Stocks
@time: 2023/1/14 22:52
@desc:
"""

import strategy.week_strategy as strategy
import pandas as pd
import matplotlib.pyplot as plot

code1 = "601166.XSHG"  # 兴业银行
code2 = "601318.XSHG"  # 中国平安
code3 = "601168.XSHG"  # 西部矿业

data1 = strategy.week_period_strategy(code1, "daily", None, "2023-01-01")
data2 = strategy.week_period_strategy(code2, "daily", None, "2023-01-01")
data3 = strategy.week_period_strategy(code3, "daily", None, "2023-01-01")

data_sum = pd.DataFrame()

data_sum["sum_product_601166"] = data1[["cum_profit"]]
data_sum["sum_product_601318"] = data2[["cum_profit"]]
data_sum["sum_product_601168"] = data3[["cum_profit"]]

data_sum.plot()
plot.show()


