import data.bstock_utils as bs_utils

# database directory
database = "/Users/bowenzhang/PycharmProjects/RFTrader/Astock_database.db"
# update info @ today(today=1) or @ yesterday(today=0)
today = 0
# instantiate bs_utils
bs_utils = bs_utils.Utils(database)
# update basic info
bs_utils.update_basic_info(today)
# update fundamentals
bs_utils.update_fundamentals(today)
# update price info
bs_utils.update_stock_price_info_into_db()