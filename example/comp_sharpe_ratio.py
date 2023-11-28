"""
@author: Bowen Zhang
@software: pycharm
@file: comp_sharpe_ratio
@time: 2023/1/15 12:29
@desc:
"""
import datetime

import pandas as pd

import data.stock as st
import strategy.base as stb
import matplotlib.pyplot as plot

# 获取三只股票的数据：比亚迪、宁德时代、隆基
codes = ["002594.XSHE", "300750.XSHE", "600009.XSHG"]
# 容器：存放夏普
sharpes = []
for code in codes:
    data = st.get_single_price(code, "daily", None, datetime.date.today())
    print(data.head())
    # 计算每支股票的夏普比率
    daily_sharpe, annual_sharpe = stb.calculate_sharpe(data)
    sharpes.append([code, annual_sharpe])  # 存放[[c1,s1],[s2,s2],...]
    print(sharpes)
# 可视化3只股票并比较
sharpes = pd.DataFrame(sharpes, columns=["code", "sharpe"]).set_index("code")
print(sharpes)
# 绘制bar图
sharpes.plot.bar(title="Compare Annual Sharpe Ratio")
plot.xticks(rotation=0)
plot.show()
