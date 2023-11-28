"""
@author: Bowen Zhang
@software: pycharm
@file: find_best_param_for_ma_strategy
@time: 2023/1/22 19:54
@desc: 寻找最有参数（以MA双均线策略为例）
"""
import pandas as pd
import strategy.ma_strategy as ma
import data.bstock as st

# 参数1：股票池
stocks = ["sh.601166"]
data = st.get_single_price_from_local(stocks[0])
# 参数2：周期参数
params = [5, 10, 20, 60, 120, 250]

# 存放参数与收益
result = []

# 匹配并计算不同的周期参数对：5-10、5-20......120-250

for short in params:
    for long in params:
        if long > short:
            ma_result = ma.ma_strategy(data, short_window=short, long_window=long)
            cum_profit = ma_result['cum_profit'].iloc[-1]
            result.append([short, long, cum_profit])
print(result)
result = pd.DataFrame(result, columns=["short_window", "long_window", "cum_profit"])
print(result.sort_values(by="cum_profit", ascending=False))
