"""
@author: Bowen Zhang
@software: pycharm
@file: statistical_testing
@time: 2023/1/18 18:31
@desc: 寻找最优参数
1. 任选 10 只股票
2. 调整均线周期参数，如：MA5-MA10、MA5-MA20、MA5-MA60、MA5-MA120、MA10-MA20 等
3. 计算不同周期参数的收益率
4. 找到每只股票收益率最高的周期参数
5. 并对该周期参数的收益率进行假设检验
6. 所得 p 值是否能拒绝 H0：收益率均值 = 0
"""

# 任选 10 只股票
stocks = ["sh.601166",""]
