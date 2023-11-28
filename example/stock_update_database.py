'''
@author: Bowen Zhang
@software: pycharm
@file: stock_update_database.py
@time: 2023/1/14 14:10
@desc: 用于更新每日股票行情的python脚本
'''
import pandas as pd

import data.bstock as bs
import datetime

# 更新股票每日价格日K数据
# bs.update_daily_price()
# 更新股票基本信息
bs.update_basic_info()
