# 这是一个示例 Python 脚本。
import datetime

import pandas as pd
# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。


import data.bstock as bs

code = "sz.000001"
# 更新所以股票数据库信息
print(bs.get_single_end_date_from_local(code,"price"))
print(bs.get_single_start_date_from_local(code,"price"))
