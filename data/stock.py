from jqdatasdk import *
import pandas as pd
import datetime
import os

# auth('18662705330', 'Zbw990520')
auth('18601710951', 'Goodluck5214')

# 设置行列不可忽略
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 1000)

# 全局变量
data_root = "/Users/bowenzhang/PycharmProjects/RFTrader/data/"


def init_db():
    """
    初始化股票数据库
    :return:
    """
    # 1. 获取所有股票代码
    stocks = get_stock_list()
    # 2. 存储到csv文件中
    for code in stocks:
        df = get_single_price(code, "daily")
        export_data(df, code, "price")
        print(code)


def get_stock_list():
    """
    获取所有A股列表
    :return: stock_list
    """
    stock_list = list(get_all_securities(['stock']).index)
    return stock_list


def get_single_price(code, frequency, start_date=None, end_date=None):
    """
    获取单个股票行情数据
    :param code: 股票代码
    :param frequency: 周期
    :param start_date: 起始时间
    :param end_date: 结束时间
    :return: 以dataframe形式封装的股票价格数据
    """
    # 如果start_date = None，默认为从上市日期开始
    if start_date is None:
        start_date = get_security_info(code).start_date
    if end_date is None:
        end_date = datetime.date.today()
    df = get_price(security=code, frequency=frequency, start_date=start_date,
                   end_date=end_date, panel="false")
    return df


def export_data(data, filename, type, mode=None):
    """
    导出股票相关数据
    :param data: 需要导出的数据
    :param filename: 导出数据存放的文件名
    :param type: 股票数据类型，可以是：price、finance
    :param mode: 写入模式选择：a代表追加，None代表默认w写入
    :return:
    """
    file_root = data_root + type + "/" + filename + ".csv"
    data.index.names = ['date']
    if mode == 'a':
        data.to_csv(file_root, mode=mode, header=False)
        # 删除重复值
        data = pd.read_csv(file_root)
        data = data.drop_duplicates(subset=["date"], index=False)  # 以日期列为准
        data.to_csv(file_root)  # 去重完后重新写入
    else:
        data.to_csv(file_root)
    print('已经成功存储至：', file_root)


def get_csv_data(code, type):
    file_root = data_root + type + "/" + code + ".csv"
    return pd.read_csv(file_root)


def transfer_price_freq(data, frequency):
    """
    将数据转换为指定周期
    :param data:
    :param frequency:
    :return:
    """
    df_trans = pd.DataFrame();
    df_trans['open'] = data['open'].resample(frequency).first()
    df_trans['close'] = data['close'].resample(frequency).first()
    df_trans['high'] = data['high'].resample(frequency).first()
    df_trans['low'] = data['low'].resample(frequency).first()

    return df_trans


def get_single_finance(code, date, statDate):
    """
    获取单个股票财务指标
    :param code:
    :param date:
    :param statDate:
    :return:
    """
    data = get_fundamentals(query(indicator).filter(indicator.code == code), date=date, statDate=statDate)
    return data


def get_single_valuation(code, date, statDate):
    """
    获取单个股票估值指标
    :param code:
    :param date:
    :param statDate:
    :return:
    """
    data = get_fundamentals(query(valuation).filter(valuation.code == code), date=date, statDate=statDate)
    return data


def calculate_change_pct(data):
    """
    涨跌幅=（当期收盘价-前期收盘价）/前期收盘价
    :param data: dataframe，带有收盘价
    :return: dataframe，带有涨跌幅
    """
    data['close_pct'] = (data['close'] - data['close'].shift(1)) \
                        / data['close'].shift(1)
    return data


def update_daily_price(type):
    stocks = get_stock_list()
    for stock_code in stocks:
        # 3.1 是否存在文件：不存在-重新获取，存在-3.2
        file_root = data_root + type + "/" + stock_code + ".csv"
        if os.path.exists(file_root):
            # 3.2 每日更新数据：获取增量数据 （code、start_date = 对应股票csv中最后一个日期、end_date = timedate.date.today()）
            start_date = pd.DataFrame(get_csv_data(stock_code, "price"))["date"].iloc[-1]
            end_date = datetime.date.today()
            df = get_single_price(stock_code, "daily", start_date, end_date)
            export_data(df, filename=stock_code, type="price", mode="a")
        else:
            # 3.4 追加到已有文件中
            df = get_single_price(stock_code, "daily")
            export_data(df, stock_code, "price")
