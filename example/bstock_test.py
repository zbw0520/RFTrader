"""
@author: Bowen Zhang
@software: pycharm
@file: bstock_test
@time: 2023/1/15 15:29
@desc:
"""

import data.bstock_utils as st

code = "sh.601398"

start_date = st.get_single_start_date(code)
print(start_date)
