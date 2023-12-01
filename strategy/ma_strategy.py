"""
@author: Bowen Zhang
@software: pycharm
@file: ma_strategy
@time: 2023/1/15 21:37
@desc: 双均线策略
"""
import datetime

import pandas as pd

import data.bstock_utils as bs
import numpy as np

import strategy.base as strat
import matplotlib.pyplot as plot


def ma_strategy(data, short_window=5, long_window=20):
    """

    :param data: 投资标的行情数据（必须包含收盘价）
    :type data: pandas.DataFrame
    :param short_window: 短期n日移动平均线，默认5
    :param long_window: 长期n日移动平均线，默认20
    :return:
    """
    print("=========当前周期参数对：", short_window, long_window)
    # 计算技术指标：MA_short_term, MA_long_term
    data["MA_short_term"] = pd.DataFrame(data["close"]).rolling(window=short_window).mean()
    data["MA_long_term"] = pd.DataFrame(data["close"]).rolling(window=long_window).mean()
    # 生成信号：金叉买入、死叉卖出
    data["buy_signal"] = np.where(data["MA_short_term"] > data["MA_long_term"], 1, 0)
    data["sell_signal"] = np.where(data["MA_short_term"] < data["MA_long_term"], -1, 0)
    # 过滤信号：bs.compose_signal
    data = strat.compose_signal(data)
    # 计算单次收益
    data = strat.calculate_prof_pct(data)
    # 计算累计收益
    data = strat.calculate_cum_prof(data)
    # 数据预览
    print(data[["close", "MA_short_term", "MA_long_term", "signal", "cum_profit"]])
    # 删除多余的columns
    # data.drop(labels=["buy_signal", "sell_signal"], axis=1)
    return data


if __name__ == "__main__":
    # 获取上市时间晚于2016年的股票列表
    # 获取所有股票列表
    stock_lists = bs.get_stock_list()
    print(len(stock_lists))
    # 存放累计收益率
    # cum_profits = pd.DataFrame()
    # for code in stocks:
    #     df = bs.get_csv_price(code, "2016-01-01", "2021-01-01")
    #     # 调用
    #     df = ma_strategy(df, 5, 20)
    #     cum_profits[code] = pd.DataFrame(df["cum_profit"]).reset_index(drop=True)
    #     df["cum_profit"].plot()
    # # 可视化
    # plot.title("Comparison of MA Strategy Profits")
    # plot.show()
    # print(df[["profit_pct", "cum_profit"]])
    # pd.DataFrame(df["cum_profit"]).plot()
    # plot.show()
    # # 筛选信号点
    # df = df[df["signal"] != 0]
    # # 计算交易对数量
    # print("开仓次数：", int(len(df)))
    # # print(df[["close", "MA_short_term", "MA_long_term", "signal"]])
