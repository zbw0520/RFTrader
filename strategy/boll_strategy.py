"""
@author: Bowen Zhang
@software: pycharm
@file: boll_strategy.py
@time: 2023/1/23 11:30
@desc: Strategy on bolling band
"""

import pandas as pd
import strategy.base as strat
import numpy as np
import data.bstock_utils as bs
import matplotlib.pyplot as plt


def boll_strategy(data, window=60):
    # 计算技术指标
    mid = pd.DataFrame(data["close"]).rolling(window=window).mean()
    std = pd.DataFrame(data["close"]).rolling(window=window).std()
    upper = mid + std
    lower = mid - std
    data["boll_mid"] = mid
    data["boll_upper"] = upper
    data["boll_lower"] = lower
    # 计算交易信号
    data["buy_signal"] = np.where((data["close"] < data["boll_lower"]) & (data["open"] > data["boll_lower"]), 1, 0)
    data["sell_signal"] = np.where((data["close"] > data["boll_upper"]) & (data["open"] < data["boll_upper"]), -1, 0)
    # 过滤信号：bs.compose_signal
    data = strat.compose_signal(data)
    # 计算单次收益
    data = strat.calculate_prof_pct(data)
    # 计算累计收益
    data = strat.calculate_cum_prof(data)
    print(data[["close", "signal", "cum_profit"]])
    return data


if __name__ == "__main__":
    stock = "西部矿业"
    code = bs.convert_stock_list_name_2_code([stock])[0]
    data = boll_strategy(bs.get_single_price_from_local(code), 60)
    data["cum_profit"].plot()
    plt.show()
