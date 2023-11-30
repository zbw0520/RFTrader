# RFTrader

## 功能模块

### 行情记录（bstock）
从baostock[](http://baostock.com/baostock/index.php/首页)服务器上获取最新的基本信息、基本面信息、以及日线行情数据，存入本地数据库，有需要的时候从本地数据库中进行调用。

创建bstock对象，并指定本地数据库路径：
```python
# 引入该类文件
import data.bstock as bs_utils
# 你个人的数据库路径
database = "/User/admin/stock.db"
# 初始化
bs_utils = bs_utils.bstock(database)
```

从baostock服务器更新上一个交易日（today=0）股票的基本信息并存入本地数据库
```python
bs_utils.get_basic_info_into_db(today=0)
```

从本地sqlite数据库中获取存储的基本信息并存入本地数据库
```python
stock_list = bs_utils.get_basic_info_from_db()
```

从baostock服务器中获取基本面信息并存入本地数据库
```python
# 从本地的基本信息数据库中获取全部的股票基本信息
stock_list = bs_utils.get_basic_info_from_db()
# 从获取的基本信息中拿取股票代码列表
code_list = stock_list.loc[:, ["code"]]
# 根据获取的股票列表，从baostock服务器中获取基本面信息并存入本地数据库
bs_utils.get_all_fundamentals_into_db(code_list)
```

从本地数据库中获取存储的名为兴业银行的基本面信息
```python
code = bs_utils.get_basic_info_from_db(result_column="code"
                                       , query_column="code_name", query_string="兴业银行")
code_target = code["code"].iloc[0]
print(code_target)
print(bs_utils.get_single_stock_fundamentals_from_db(code_target))
```

从baostock服务器更新所有股票的日线行情信息并存入本地数据库
```python
stock_list = bs_utils.get_basic_info_from_db()
bs_utils.get_all_price_into_db(stock_list.loc[:, ["code"]])
```

从本地数据库中获取名为兴业银行的股票指定日期的日线行情信息
```python
code = bs_utils.get_basic_info_from_db(result_column="code"
                                       , query_column="code_name"
                                       , query_string="兴业银行")
code = code["code"].iloc[0]
price_info = bs_utils.get_single_price_from_db(code,start_date="2010-03-04")
```
#### 

### 策略开发

### 自动交易