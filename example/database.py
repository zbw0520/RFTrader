"""
@author: Bowen Zhang
@software: pycharm
@file: database.py
@time: 2023/1/14 15:59
@desc: 测试数据库操作
"""
import datetime
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

import data.bstock as bs_utils
import baostock as bs
import sqlite3

database = "/Users/bowenzhang/PycharmProjects/RFTrader/Astock_database.db"
# 初始化
bs_utils = bs_utils.bstock(database)

# 从baostock服务器更新上一个交易日（today=0）股票的基本信息并存入本地数据库
# bs_utils.get_basic_info_into_db(today=0)

# 从本地sqlite数据库中获取存储的基本信息并存入本地数据库
# stock_list = bs_utils.get_basic_info_from_db()
# 从本地数据库中获取的基本面信息中拿取全部股票代码DataFrame对象
# code_list = stock_list.loc[:, ["code"]]

# 从baostock服务器中获取基本面信息并存入本地数据库
# stock_list = bs_utils.get_basic_info_from_db()
# code_list = stock_list.loc[:, ["code"]]
# bs_utils.get_all_fundamentals_into_db(code_list)

# 从本地数据库中获取存储的名为兴业银行的基本面信息
# code = bs_utils.get_basic_info_from_db(result_column="code"
#                                        , query_column="code_name", query_string="兴业银行")
# code_target = code["code"].iloc[0]
# print(code_target)
# print(bs_utils.get_single_stock_fundamentals_from_db(code_target))
# 获取兴业银行对应的上市时间
# print(bs_utils.get_single_stock_start_date_from_db("bj.430017"))

# 从baostock服务器更新所有股票的日线行情信息并存入本地数据库
# stock_list = bs_utils.get_basic_info_from_db()
# bs_utils.get_all_price_into_db(stock_list.loc[:, ["code"]])

# 从本地数据库中获取名为兴业银行的股票指定日期的日线行情信息
# code = bs_utils.get_basic_info_from_db(result_column="code"
#                                        , query_column="code_name"
#                                        , query_string="兴业银行")
# code = code["code"].iloc[0]
# price_info = bs_utils.get_single_price_from_db(code,start_date="2010-03-04")
# print(price_info)

# 获取本地数据库中兴业银行股票的最后一个交易日的日期
# code = bs_utils.get_basic_info_from_db(result_column="code"
#                                        , query_column="code_name", query_string="兴业银行")
# code = code["code"].iloc[0]
# end_date = bs_utils.get_single_stock_end_date_from_db(code)

# 检测baostock服务器是否更新了数据
# bs_utils.is_data_updated_on_baostock_server()

# 更新当前的日线行情信息
bs_utils.update_stock_price_info_into_db()
