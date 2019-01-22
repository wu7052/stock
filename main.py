# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
# wx.info("this is new logger")

from functions import *
# update_ws_share_holder()


# 从上证、深证 网站更新 A 股基础信息
# update_sh_basic_info()
# update_sz_basic_info()

# 从 eastmoney 获得大宗交易数据
# 先检查数据库中大宗交易的最新日期，
# 截止时间 是今天
# update_whole_sales_data(force=False)
# update_ws_share_holder()


# 从sina获得实时的交易数据
# update_daily_data_from_sina()

# 调用mysql 存储过程获得 A 股市值
#get_list_a_total_amount()



# 从tushare 获取前一天的 交易数据
# update_daily_data_from_ts(period = -1)
