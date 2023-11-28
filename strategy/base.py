"""
@author: Bowen Zhang
@software: pycharm
@file: strategy
@time: 2023/1/14 16:16
@desc: 用来创建交易策略、生成交易信号
"""
import pandas as pd

import data.stock as st
import numpy as np
import datetime
import matplotlib.pyplot as plot

pd.options.mode.chained_assignment = None


def compose_signal(data):
    """
    整合信号
    :param data:
    :return:
    """
    data = data[
        (data['buy_signal'] != 0) |
        (data['sell_signal'] != 0)].copy()  # 去除无买卖日期的信号
    data['buy_signal'] = np.where((data['buy_signal'] == 1)
                                  & (data['buy_signal'].shift(1) == 1), 0, data['buy_signal'])
    data['sell_signal'] = np.where((data['sell_signal'] == -1)
                                   & (data['sell_signal'].shift(1) == -1), 0, data['sell_signal'])
    data = data[
        (data['buy_signal'] != 0) |
        (data['sell_signal'] != 0)].copy()  # 去除无买卖日期的信号
    data['signal'] = data['buy_signal'] + data['sell_signal']

    return data


def calculate_prof_pct(data):
    """
    计算单次收益率：开仓、平仓（开仓的全部股数）
    :param data:
    :return:
    """
    data = data[data['signal'] != 0]  # 筛选
    # data.loc[:, "profit_pct"] = (data.loc[:, "close"] - data.loc[:, 'close'].shift(1)) / data.loc[:, 'close']
    data["profit_pct"] = data["close"].pct_change()  # 计算涨跌幅
    data = data[data['signal'] == -1]  # 筛选平仓后数据
    return data


def calculate_cum_prof(data):
    """
    计算累计收益率
    :param data: dataframe数据
    :return: dataframe数据（包含累计收益率）
    """
    data['cum_profit'] = pd.DataFrame(1 + data["profit_pct"]).cumprod() - 1
    return data


def calculate_max_drawdown(data):
    """
    计算最大回撤比
    :param data:
    :return:
    """
    # 选取时间周期（时间窗口）
    window = 252
    # 选取时间周期中的最大净值
    data["roll_max"] = data['close'].rolling(window=window, min_periods=1).max()
    # 计算当天的回撤比 = （谷值-峰值）/峰值=谷值/峰值-1
    data["daily_drawndown"] = data['close'] / data["roll_max"] - 1
    # 选取时间周期内最大的回撤比，即最大回撤
    data["max_drawndown"] = data["daily_drawndown"].rolling(window=window, min_periods=1).min()
    return data


def calculate_sharpe(data):
    """
    计算夏普比率，返回
    :param data: dataframe, stock
    :return: float
    """
    # 公式 sharpe = （回报率期望值-无风险利率）/回报率标准差
    # 填充公式内部变量
    # 回报率的均值 = 日涨跌幅.mean()
    return_mean = data["close"].pct_change().mean()
    # 回报率的标准差 = 日涨跌幅.stddeviation()
    return_std = data["close"].pct_change().std()
    # 计算夏普
    # sharpe = 回报率的均值/回报率的标准差
    sharpe = return_mean / return_std
    sharpe_year = sharpe * np.sqrt(252)
    return sharpe, sharpe_year
