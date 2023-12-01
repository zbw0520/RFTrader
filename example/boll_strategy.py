"""
@author: Bowen Zhang
@software: pycharm
@file: boll_strategy
@time: 2023/2/1 14:30
@desc: 测试布林带策略
"""

import strategy.boll_strategy as boll
import data.bstock_utils as bs

stocks = ["兴业银行"]
codes = bs.convert_stock_list_name_2_code(stocks)
for code in codes:
    data = bs.get_single_price_from_local(code)
    boll.boll_strategy(data,5)