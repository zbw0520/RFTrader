import datetime
import os

import baostock as bs
import pandas
import pandas as pd

import sqlite3
from tqdm import tqdm
import time


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


def transfer_price_freq(data, frequency):
    """
    将数据转换为指定周期
    :param data:
    :param frequency:
    :return:
    """
    df_trans = pd.DataFrame()
    df_trans['open'] = data['open'].resample(frequency).first()
    df_trans['close'] = data['close'].resample(frequency).first()
    df_trans['high'] = data['high'].resample(frequency).first()
    df_trans['low'] = data['low'].resample(frequency).first()

    return df_trans


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
    elif ("sh" in stock_code) or ("sz" in stock_code) or ("bj" in stock_code):
        stock_code_file = stock_code
    else:
        raise KeyError("Wrong code format!")
    return stock_code_file


class bstock:
    database = None

    def __init__(self, database_path):
        bstock.database = database_path
        # 登录baostock
        lg = bs.login()
        # 显示登陆返回信息
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)

        # 设置行列不可忽略
        pd.set_option('display.max_rows', 100000)
        pd.set_option('display.max_columns', 1000)

    def init_db(self):
        """
        初始化数据库，生成对应名称的数据库，名称定义为database_name
        :return:
        """
        conn = sqlite3.connect(self.database)
        conn.close()

    def convert_single_stock_name_2_code(self, stock_name):
        """
        将单个股票名称转化为股票代码
        :param stock_name: 股票名称
        :return: 股票代码
        """
        rs = bs.query_stock_basic(code_name=stock_name)
        df = convert_result_data_to_dataframe(rs)
        return df["code"].iloc[0]

    def convert_stock_list_name_2_code(self, stocks_names):
        """
        将股票名称列表转换为股票代码列表
        :param stocks_names: 股票名称列表
        :return: 股票代码列表
        """
        code = []
        for stock_name in stocks_names:
            code.append(self.convert_single_stock_name_2_code(stock_name))
        return code

    def get_recent_trade_dates(self, today=1):
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

    def get_basic_info(self, today=1):
        """
        从baostock服务器上获取最近一个交易日的所有A股的基本信息，其中包含三个字段："code"、"tradeStatus"、"code_name"，
        将这些信息转换为dataframe的格式，并输出。
        重写支持数据库操作
        :param today: 选择当前交易日today=1；选择上一个交易日today=0。
        :return: data的dataframe
        """
        rs = bs.query_all_stock(self.get_recent_trade_dates(today=today))
        info = convert_result_data_to_dataframe(rs)
        if info.isnull:
            print("今日信息未更新，请将today=0再操作，或者等今日数据更新后再进行操作！")
        return info

    def get_df_into_db(self, table_name, df):
        conn = sqlite3.connect(self.database)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()

    def append_df_into_db(self, table_name, df):
        conn = sqlite3.connect(self.database)
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()

    def get_basic_info_into_db(self, today=1):
        """
        从baostock服务器上获取最近一个交易日的所有A股的基本信息，其中包含三个字段："code"、"tradeStatus"、"code_name"，
        将这些信息转换为dataframe的格式，并输出，之后存储到制定路径的数据库中。
        :param today: 选择当前交易日today=1；选择上一个交易日today=0
        :return: 无返回
        """
        info = self.get_basic_info(today=today)
        self.get_df_into_db('basic_info', df=info)

    def get_basic_info_from_db(self, result_column="*", query_column="*", query_string="*"):
        """
        从指定路径的本地数据库中获取基本信息其中包含三个字段："code"、"tradeStatus"、"code_name"，
        将这些信息转换为dataframe的格式，并输出。
        :param query_string: 需要进行检索的字符串，可以自定义，根据检索的列名选择不同的字符串
        :param query_column: 需要进行检索的列名，可选三个："code"、"tradeStatus"、"code_name"
        :param result_column: 需要查询的结果需要包括那些列，可选三个："code"、"tradeStatus"、"code_name"，如果"*"那么则是全部查询
        :return: dataframe格式的所有股票的基本信息
        """

        conn = sqlite3.connect(self.database)
        if query_string != "*":
            query = ("SELECT " + result_column + " FROM basic_info WHERE "
                     + query_column + " LIKE '" + query_string + "'")
        else:
            query = ("SELECT " + result_column + " FROM basic_info")
        info = pd.read_sql_query(query, conn)
        conn.close()
        return info

    def get_single_stock_fundamentals(self, code):
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

    def get_all_fundamentals_into_db(self, codes):
        """
        从baostock服务器上获取codes股票代码列表中的所有股票的基本面信息，并将其存入名为database的数据库中。
        :param codes: 需要获取基本面信息的股票代码列表
        :return: 无返回
        """
        all_fundamentals = []
        for i, code in tqdm(enumerate(codes['code']), desc="基本面信息获取进度", total=len(codes)):
            try:
                fundamentals = self.get_single_stock_fundamentals(code)
                all_fundamentals.append(fundamentals)
            except Exception as e:
                print(f"Error fetching data for {code}: {e}")

        # Concatenate all fundamentals data
        info = pd.concat(all_fundamentals, ignore_index=True, sort=False)
        # 将获取到的基本面信息DataFrame对象存入数据库中
        self.get_df_into_db("fundamentals", info)

    def get_single_stock_fundamentals_from_db(self, code):
        """
        从数据库中获取单个股票的基本面信息
        :param code: A股股票代码，sh或sz.+6位数字代码
        :return: 股票基本面的DataFrame数据info
        """
        code = convert_stock_code_2baostock(code)
        conn = sqlite3.connect(self.database)
        query = ("SELECT * FROM fundamentals WHERE code LIKE '" + code + "'")
        info = pd.read_sql_query(query, conn)
        conn.close()
        return info

    def get_single_stock_start_date_from_db(self, code):
        """
        从本地数据库中查找单个股票上市时间[String]
        :param code: A股股票代码，sh或sz.+6位数字代码
        :return: 股票上市时间
        """
        data = self.get_single_stock_fundamentals_from_db(code)
        return data["ipoDate"].iloc[0]

    def get_single_price(self, code, frequency='d', start_date=None, end_date=None):
        """
        获取单个股票的日线行情数据
        :param code: 股票代码
        :param frequency: 周期
        :param start_date: 起始时间
        :param end_date: 结束时间
        :return: 以dataframe形式封装的股票价格数据
        """
        # 如果start_date = None，默认为从上市日期开始
        code = convert_stock_code_2baostock(code)
        if start_date is None:
            # 获取证券上市时间
            start_date = self.get_single_stock_start_date_from_db(code)
        if end_date is None:
            end_date = str(datetime.date.today())
        rs = bs.query_history_k_data_plus(code, "date,open,close,high,low,volume,amount", start_date, end_date,
                                          frequency, adjustflag="2")
        df = convert_result_data_to_dataframe(rs)
        return df

    def get_all_price_into_db(self, codes):
        """
        从baostock服务器中获取所有在列表codes中的股票的日线行情信息，并存入本地的数据库
        :return: 无返回
        """
        for i, code in tqdm(enumerate(codes['code']), desc="日线行情获取进度", total=len(codes)):
            if "bj" in code:
                continue
            try:
                price_info = self.get_single_price(code)
                price_info["code"] = code
                self.append_df_into_db("price_info", price_info)
            except Exception as e:
                print(f"Error fetching data for {code}: {e}")

    def get_single_price_from_db(self, code, start_date=None, end_date=None):
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
        # 如果start_date == None，则使用本地数据起始时间（一般为上市时间）
        if start_date is None:
            # 获取本地的数据的起始时间字符串
            start_date = self.get_single_stock_start_date_from_db(code)
        # 如果end_date == None，则使用当天时间
        if end_date is None:
            # 获取当天时间字符串
            end_date = str(datetime.date.today())
        # 获取数据库中对应股票价格信息的路径
        conn = sqlite3.connect(self.database)
        query = ("SELECT * FROM price_info WHERE code LIKE '%" + code + "%'")
        data = pd.read_sql_query(query, conn)
        # 使用pandas自带功能抓取固定日期间的信息
        data["date"]=pd.to_datetime(data["date"])
        filtered_data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
        return filtered_data.reset_index(drop=True)

    def calculate_change_pct(data):
        """
        涨跌幅=（当期收盘价-前期收盘价）/前期收盘价
        :param data: dataframe，带有收盘价
        :return: dataframe，带有涨跌幅
        """
        data['close_pct'] = (data['close'] - data['close'].shift(1)) \
                            / data['close'].shift(1)
        return data

    def convert_stock_code(self, stock_code):
        """
        将股票代码进行转换，得到本地代码格式，例如"000001.XSHG"、"000001.XSHE"，以及BaoStock代码格式，例如"sh.000001"、"sz.000001"
        :param stock_code: 股票代码，例如"sh.000001"、"sz.000001"、"000001.XSHG"、"000001.XSHE"
        :return code_baostock: BaoStock代码格式，例如"sh.000001"、"sz.000001"
        :return code_local: 本地代码格式，例如"000001.XSHG"、"000001.XSHE"
        """
        code_local = convert_stock_code_2local(stock_code)
        code_baostock = convert_stock_code_2baostock(stock_code)
        return code_local, code_baostock

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
