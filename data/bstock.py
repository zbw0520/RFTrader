import datetime
import os

import baostock as bs
import pandas
import pandas as pd

import glob

# 登录baostock
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 设置行列不可忽略
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 1000)

# 全局变量
# csv数据存储路径
data_root = "/Users/bowenzhang/PycharmProjects/RFTrader/data/"


def init_db():
    """
    初始化数据库
    :return:
    """
    # 1. 获取所有股票代码
    stocks = get_recent_stock_list()
    # 2. 存储到csv文件中
    for code in stocks:
        df = get_single_price(code, "daily")
        export_data(df, code, "price")
        print(code)


def convert_single_stock_name_2_code(stock_name):
    """
    将单个股票名称转化为股票代码
    :param stock_name: 股票名称
    :return: 股票代码
    """
    rs = bs.query_stock_basic(code_name=stock_name)
    df = convert_result_data_to_dataframe(rs)
    return df["code"].iloc[0]


def convert_stock_list_name_2_code(stocks_names):
    """
    将股票名称列表转换为股票代码列表
    :param stocks_names: 股票名称列表
    :return: 股票代码列表
    """
    code = []
    for stock_name in stocks_names:
        code.append(convert_single_stock_name_2_code(stock_name))
    return code


def get_recent_trade_dates(today=1):
    # 指定前i_days天
    i_days = 10
    # 获取前i_days天的datetime对象
    date = datetime.date.today() - datetime.timedelta(days=10)
    # 获取前i_days天内的交易日信息
    df = convert_result_data_to_dataframe(bs.query_trade_dates(str(date)))
    # 将日期列中的String转为datetime对象
    df["calendar_date"] = pd.to_datetime(df["calendar_date"])
    # 将交易日的日期选出
    df = df.loc[df["is_trading_day"] == "1"]
    # 如果df为空，报错，需要用户将i_days变量变大，实例：i_days = 20
    if df.empty:
        raise KeyError("No trading days in last 10 days!\nTry to increase value of i_days...")
    # 将df中的数据按照时间倒序排列，即最新的日期在第一个
    df = pd.DataFrame(df).sort_values(by="calendar_date", ascending=False)
    # 根据today是否真选择将今日或上一个交易日的日期取出，并删去时间的部分，并转换为字符串等待输出
    if today:
        latest_date = str(df["calendar_date"].iloc[0].date())
    else:
        latest_date = str(df["calendar_date"].iloc[1].date())
    return latest_date


def get_recent_stock_list(today=0):
    """
    获取最近一个交易日的所有A股列表
    :return: stock_list
    """
    rs = bs.query_all_stock(get_recent_trade_dates(today=today))
    stock_list = convert_result_data_to_dataframe(rs).loc[:, ["code"]]
    return stock_list["code"]


def get_stock_list_from_local():
    """
    从本地获取数据库中的所有股票列表
    :return: stock_list
    :rtype: pandas.DataFrame
    """
    # 初始化DataFrame对象用于存储股票代码信息
    column_name = "stock_code"
    stock_list = pd.DataFrame()
    # 获取file_root路径
    file_root = data_root
    # 获取路径下所有csv文件
    files = glob.glob(file_root + "/*.csv")
    for file in files:
        try:
            code_baostock = convert_stock_code_2baostock(file[:11])
            if not stock_list.isin([code_baostock]).any().stock_code:
                stock_list.loc[len(stock_list)] = code_baostock
        except Exception as e:
            print(f'Error occurred while processing {file} : {e}')
    return stock_list


def convert_result_data_to_dataframe(rs):
    """
    将bs.data.resultset.ResultData中的pandas.DataFrame数据提取并返回
    :param rs: 需要提取的bs.data.resultset.ResultData类型的数据
    :type rs: bs.data.resultset.ResultData
    :return: 转换完成的pandas.DataFrame数据
    :rtype: pandas.DataFrame
    """
    if type(rs) is bs.data.resultset.ResultData:
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        # 删去索引
        result = result.reset_index(drop=True)
        return result
    else:
        raise TypeError("rs must be type of bs.data.resultset.ResultData!")


def get_single_stock_basic(code):
    """
    获取股票基本信息：
        证券代码"code"、证券名称"code_name"、上市时间"ipoDate"、退市时间"outDate"、证券类型"type"、上市状态"status"
    证券类型"type"：
        1：股票，2：指数，3：其它，4：可转债，5：ETF
    上市状态"status"：
        1：上市，0：退市
    :param code: A股股票代码，sh或sz.+6位数字代码
    :type code: str
    :return: 股票基本信息
    :rtype: pandas.DataFrame
    """
    rs = bs.query_stock_basic(code=code)
    result = convert_result_data_to_dataframe(rs)
    return result


def get_single_start_date(code):
    """
    通过输入股票代码，返回股票上市时间[String]
    :param code: A股股票代码，sh或sz.+6位数字代码
    :type code: str
    :return: 股票上市时间
    :rtype: str
    """
    code = convert_stock_code_2baostock(code)
    data = get_single_stock_basic(code)
    if data.empty:
        return str(get_recent_trade_dates())
    start_date = pd.DataFrame(data)["ipoDate"].iloc[0]
    return str(start_date)


def get_single_start_date_from_local(code, type):
    """
    从本地数据获取单个股票的上市时间
    :param code: A股股票代码，sh或sz.+6位数字代码
    :return: 股票上市时间
    :rtype: str
    """
    stock_code_local = convert_stock_code_2local(code)
    rs = pd.DataFrame(get_csv_data(stock_code_local, "price"))
    start_date = str(rs.index[0])
    return start_date


def get_file_root(code, type):
    """
    根据输入的标的代码code以及数据种类type，返回对应的数据存储路径
    :param code: baostock类型的标的代码code，例如"sh.000001"、"sz.000001"
    :type code: str
    :param type: 数据种类，例如"price"，"fundamental"
    :type: str
    :return: 数据的绝对路径
    :rtype: str
    """
    stock_code_file = convert_stock_code_2local(code)
    file_root = data_root + type + "/" + stock_code_file + ".csv"


def get_single_end_date_from_local(code, type):
    """
    从本地数据获取单个股票的最后数据对应的日期
    :param code: A股股票代码，sh或sz.+6位数字代码
    :return: 股票上市时间
    :rtype: str
    """
    stock_code_local = convert_stock_code_2local(code)
    start_date = str(pd.DataFrame(get_csv_data(stock_code_local, "price")).index[-1])
    return start_date


def frequency_conversion(frequency):
    """
    将JoinQuant中的表示日线的频率格式"daily"转换为BaoStock中的"d"
    :param frequency: JoinQuant频率字符串"daily"
    :type frequency: str
    :return: Baostock中的"d"
    :rtype: str
    """
    if frequency == "daily":
        return "d"
    else:
        return frequency


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
        # 获取证券上市时间
        start_date = get_single_start_date(code)
    if end_date is None:
        end_date = str(datetime.date.today())
    rs = bs.query_history_k_data_plus(code, "date,open,close,high,low,volume,amount", start_date, end_date,
                                      frequency_conversion(frequency), adjustflag="2")
    df = convert_result_data_to_dataframe(rs)
    if df.empty is False:
        df = df.set_index("date")
    return df


def export_data(data, filename, type, mode=None):
    """
    导出股票相关数据到本地
    :param data: 需要导出的数据
    :param filename: 导出数据存放的文件名
    :param type: 股票数据类型，可以是：price、finance
    :param mode: 写入模式选择：a代表追加，None代表默认w写入
    :return:
    """
    file_root = data_root + type + "/" + filename + ".csv"
    if type == "price":
        data.index.names = ['date']
    elif type == "basic":
        data = data
    if mode == 'a':
        data.to_csv(file_root, mode=mode, header=False)
        # 删除重复值
        data = pd.read_csv(file_root)
        data = data.drop_duplicates(subset=["date"]).set_index("date")  # 以日期列为准
        data.to_csv(file_root)  # 去重完后重新写入
    else:
        data.to_csv(file_root)
    print('已经成功存储至：', file_root)


def get_csv_data(code, type, new_index=False):
    """
    根据标的编号：code、标的信息种类：type，获取本地csv数据，并根据new_index的值决定是否需要产生新的索引列，默认不生成新的索引。
    :param code: 标的编号，格式例如："sh.000001"、"sz.000001"、"000001.XSHE"、"000001.XSHG"
    :type code: str
    :param type: 标的信息种类，例如"price"、"basic"
    :type type: str
    :param new_index: 是否需要产生新的索引
    :type new_index: boolean
    :return: csv中的数据
    :rtype: pandas.DataFrame
    """
    code = convert_stock_code_2local(code)
    file_root = data_root + type + "/" + code + ".csv"
    if new_index is False:
        return pd.read_csv(file_root, index_col=0)
    elif new_index is True:
        return pd.read_csv(file_root)
    else:
        raise KeyError("new_index must be True or False!")


def get_single_stock_code_using_name(code_name):
    """
    通过股票名称查询股票数据
    :param code_name: 股票名称
    :type code_name: str
    :return: 股票代码
    :rtype: str
    """
    rs = bs.query_stock_basic(code_name=code_name)
    if rs.error_code == '0':
        df = convert_result_data_to_dataframe(rs)
        return df["code"].iloc[0]
    else:
        raise KeyError("股票名称错误，查询不到名称为：" + code_name + "的股票！")


def get_single_price_from_local(code, start_date=None, end_date=None):
    """
    从本地获取单个股票的价格数据，如果本地不存在该数据，则进行自动更新操作，再从本地获取。
    :param code: 股票代码，格式例如："sh.000001"、"sz.000001"、"000001.XSHE"、"000001.XSHG"
    :type code: str
    :param start_date: 开始日期，默认空则使用上市日期，例如"2022-01-01"
    :type start_date: str
    :param end_date: 结束日期，默认空使用今日日期，例如"2022-01-01"
    :type end_date: str
    :return: 返回股票价格信息
    """
    # 转换股票代码为本地格式、例如"000001.XSHE"、"000001.XSHG"
    code_local = convert_stock_code_2local(code)
    # 如果start_date == None，则使用本地数据起始时间（一般为上市时间）
    if start_date is None:
        # 获取本地的数据的起始时间字符串
        start_date = get_single_start_date_from_local(code, "price")
    # 如果end_date == None，则使用当天时间
    if end_date is None:
        # 获取当天时间字符串
        end_date = str(datetime.date.today())
    # 获取数据库中对应股票价格信息的路径
    file_root = data_root + "price" + "/" + code_local + ".csv"
    # 如果路径不存在
    if os.path.exists(file_root) is False:
        # 更新本地的股票日线数据
        update_single_data(code_local)
    # 从csv文件获取数据，并且设置索引列为date
    data = pd.read_csv(file_root, index_col="date")
    # 根据日期参数筛选数据
    data = data[(data.index > start_date) & (data.index < end_date)]
    return data


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


# def get_single_valuation(code, date, statDate):
#     """
#     获取单个股票估值指标
#     :param code:
#     :param date:
#     :param statDate:
#     :return:
#     """
#     data = get_fundamentals(query(valuation).filter(valuation.code == code), date=date, statDate=statDate)
#     return data


def calculate_change_pct(data):
    """
    涨跌幅=（当期收盘价-前期收盘价）/前期收盘价
    :param data: dataframe，带有收盘价
    :return: dataframe，带有涨跌幅
    """
    data['close_pct'] = (data['close'] - data['close'].shift(1)) \
                        / data['close'].shift(1)
    return data


def convert_stock_code(stock_code):
    """
    将股票代码进行转换，得到本地代码格式，例如"000001.XSHG"、"000001.XSHE"，以及BaoStock代码格式，例如"sh.000001"、"sz.000001"
    :param stock_code: 股票代码，例如"sh.000001"、"sz.000001"、"000001.XSHG"、"000001.XSHE"
    :return code_baostock: BaoStock代码格式，例如"sh.000001"、"sz.000001"
    :return code_local: 本地代码格式，例如"000001.XSHG"、"000001.XSHE"
    """
    code_local = convert_stock_code_2local(stock_code)
    code_baostock = convert_stock_code_2baostock(stock_code)
    return code_local, code_baostock


def convert_stock_code_2local(stock_code):
    """
    将BaoStock的股票代码格式转换为本地代码格式（同为JoinQuant代码格式）
    :param stock_code: BaoStock规定的股票代码格式，例如"sh.000001"、"sz.000001"
    :type stock_code: str
    :return: 本地代码格式（同为JoinQuant代码格式），例如"000001.XSHG"、"000001.XSHE"
    :rtype: str
    """
    if "sh" in stock_code:
        stock_code_file = stock_code[3:] + ".XSHG"
    elif "sz" in stock_code:
        stock_code_file = stock_code[3:] + ".XSHE"
    elif ("XSHE" in stock_code) or ("XSHG" in stock_code):
        stock_code_file = stock_code
    else:
        raise KeyError("Wrong code format!")
    return stock_code_file


def convert_stock_code_2baostock(stock_code):
    """
    将BaoStock的股票代码格式转换为本地代码格式（同为JoinQuant代码格式）
    :param stock_code: 本地代码格式（同为JoinQuant代码格式），例如"000001.XSHG"、"000001.XSHE"
    :type stock_code: str
    :return: BaoStock规定的股票代码格式，例如"sh.000001"、"sz.000001"
    :rtype: str
    """
    if "XSHG" in stock_code:
        stock_code_file = "sh." + stock_code[:6]
    elif "XSHE" in stock_code:
        stock_code_file = "sz." + stock_code[:6]
    elif ("sh" in stock_code) or ("sz" in stock_code):
        stock_code_file = stock_code
    else:
        raise KeyError("Wrong code format!")
    return stock_code_file


def update_daily_price():
    """
    调用update_single_daily_price()方法更新全部已有股票数据
    """
    stocks = get_recent_stock_list()
    if (pd.DataFrame(stocks).empty):
        print("今日数据服务器仍未更新，请待远端数据更新后再进行update操作！")
    else:
        for stock in stocks:
            if ("sh" in stock) or ("sz" in stock):
                update_single_data(stock, "price")
        print("更新股票price数据成功！")


def update_basic_info():
    # 获取今日或最近一个交易日的股票列表
    stocks = get_recent_stock_list(today=1)
    # 获取昨日或上一个交易日的股票列表
    stocks_pre = get_recent_stock_list(today=0)
    if pd.DataFrame(stocks).empty:
        print("今日数据服务器仍未更新，请待远端数据更新后再进行update操作！")
        for stock in stocks_pre:
            if ("sh" in stock) or ("sz" in stock):
                update_single_data(stock, "basic")
        print("更新股票basic数据成功！")
    else:
        for stock in stocks:
            if ("sh" in stock) or ("sz" in stock):
                update_single_data(stock, "basic")
        print("更新股票basic数据成功！")


def update_single_data(stock_code, type="price"):
    """
    更新指定股票代码的数据，如果本地存有一定量历史数据，则选择增量更新模式；否则，选择新建文件更新模式。
    所有数据均以csv格式存储。
    :param stock_code: 股票代码，示例"sh.000001"，"sz.000001"
    :type stock_code: str
    :param type: 股票信息，"price"表示价格信息（默认），"basic"表示基本信息
    :type type: str
    """
    # 获取本地股票代码格式，以及BaoStock代码格式
    stock_code_file, stock = convert_stock_code(stock_code)
    # 如果查询的是价格数据
    if type == "price":
        # 是否存在已有数据文件：不存在-重新获取，存在，则进行追加
        file_root = data_root + type + "/" + stock_code_file + ".csv"
        # 如果路径存在
        if os.path.exists(file_root):
            # 3.2 每日更新数据：获取增量数据 （code、start_date = 对应股票csv中最后一个日期、end_date = timedate.date.today()）
            start_date = str(pd.DataFrame(get_csv_data(stock_code_file, "price", True))["date"].iloc[-1])
            end_date = str(get_recent_trade_dates())
            if start_date != end_date:
                df = get_single_price(stock, "daily", start_date, end_date)
                export_data(df, filename=stock_code_file, type="price", mode="a")
                print("更新" + stock_code_file + "的" + type + "数据成功！")
            else:
                print(stock_code_file + "已最新，无需更新！")
        else:
            # 3.4 追加到已有文件中
            df = get_single_price(stock, "daily")
            export_data(df, stock_code_file, "price")
            print("新建" + stock_code_file + "的" + type + "数据成功！")
    # 股票基本数据采取一只股票一个csv的形式还是
    elif type == "basic":
        file_root = data_root + type + "/" + "basic" + ".csv"
        # 如果不存在该股票的基本信息文件路径
        if os.path.exists(file_root) is False:
            # 通过BaoStock获取股票基本数据
            rs = bs.query_stock_basic(stock)
            df = convert_result_data_to_dataframe(rs)
            # 新建文件夹，并写入新文件中
            export_data(df, filename=stock_code_file, type="basic")
            print("新建" + stock_code_file + "的" + type + "数据成功！")
        else:
            # 获取源文件内容
            df_local = get_csv_data(stock, type, False)
            rs = bs.query_stock_basic(stock)
            df = convert_result_data_to_dataframe(rs)
            if df_local == df:
                print(stock + "的" + type + "数据已经最新，无需更新！")
            else:
                export_data(df, filename=stock_code_file, type="basic")


if __name__ == "__main__":
    print(get_recent_stock_list())
