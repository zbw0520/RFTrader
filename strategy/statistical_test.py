"""
@author: Bowen Zhang
@software: pycharm
@file: statistical_test
@time: 2023/1/18 17:11
@desc:
"""

import data.bstock_utils as bs
import strategy.ma_strategy as ma
import matplotlib.pyplot as plt
from scipy import stats


def t_test(samples):
    """
    对策略收益进行t检验
    :param strat_return: dataframe，单次收益率
    :return: t值和p值
    :rtype: float
    """

    # 调用假设检验ttest函数：scipy
    t, p = stats.ttest_1samp(samples, 0, nan_policy="omit")

    # 判断是否与理论均值有显著性差异: alpha = 0.05
    p_value = p / 2
    print("是否拒绝H0：收益均值=0：", p_value < 0.05)
    return t, p_value


if __name__ == '__main__':
    print(bs.get_single_stock_code_using_name("兴业银行"))
    # # 对多个股票进行计算、测试
    # stocks = ["sz.000001", "sz.000858", "sz.002594"]
    # code = stocks[1]
    # print(code)
    # df = bs.get_single_price_from_local(code)
    # # 策略的单次收益率是样本
    # df = ma.ma_strategy(df, 5, 15 * 5)
    # samples = df["profit_pct"]
    # t, p = t_test(samples)
    # print("t = ", t)
    # print("p = ", p)
    # # 绘制一下分布图用于观察
    # plt.hist(samples, bins=30)
    # plt.show()
