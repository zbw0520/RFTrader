"""
@author: Bowen Zhang
@software: pycharm
@file: ma_strategy
@time: 2023/1/16 18:32
@desc:
"""
import datetime

import data.bstock_utils as bs
import numpy as np
import pandas as pd
import strategy.ma_strategy as ma
import matplotlib.pyplot as plot

# 更新所有股票列表
# bs.update_daily_price("price")


# # 获取本地所有股票列表
# stock_list = bs.get_stock_list_from_local()
# print(len(stock_list))
# # 获取本地所有股票的上市日期
# for stock in stock_list.itertuples():
#     # 获取股票上市时间
#     start_date = bs.get_single_start_date_from_local(stock.stock_code, type)
#     start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
#     # 如果上市时间在2016年之前，则删去此条记录
#     if start_date < datetime.datetime.strptime("2016-01-01", "%Y-%m-%d"):
#         stock_list = pd.DataFrame(stock_list).drop(stock.Index)
#     print(stock.Index)
# print(len(stock_list))
# stock_list.reset_index(drop=True)
# bs.export_data(stock_list, "after2016", "stock", None)

# 从磁盘中读取股票数据
stock_list = bs.get_csv_data("after2016", "stock").reset_index(drop=True)["stock_code"].to_numpy()
rs_list = pd.DataFrame()
for stock in stock_list.flat[:1]:
    # 从本地获取股票行情数据：
    price = bs.get_single_price_from_local(stock)
    rs = ma.ma_strategy(price, 5, 20)
    rs = pd.DataFrame(rs).reset_index(drop=True)
    rs_list[str(stock)] = rs[["cum_profit"]]
print(rs_list)
rs_list.plot()
plot.show()
