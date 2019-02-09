# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
# wx.info("this is new logger")

from functions import *

# 申银万国行业代码导入，一次性初始化即可
# update_sw_industry_code()


# 报表输出功能
# report_total_amount()
# report_days_vol(days=7, type="00")


"""
# 从上证、深证 网站更新 A 股基础信息
# 从申银万国 更新 行业分类
"""
# update_sh_basic_info()
# update_sz_basic_info()
# update_sw_industry_into_basic_info()


"""
# 从 eastmoney 获得大宗交易数据
# update_whole_sales_data 先检查ws_201901大宗交易流水的最新日期，截止时间 是今天； force = True 强制更新，删除旧数据
# update_ws_share_holder 更新 share_holder 表的汇总数据
"""
# update_whole_sales_data(force=False)
# update_ws_share_holder()


"""
# 从sina获得实时的交易数据
# 不指定日期，默认是当天
# 指定日期，sina 的交易数据更新到 指定日期
"""
# update_daily_data_from_sina()
# update_daily_data_from_sina(date = '20190201')


"""
# 从eastmoney 获得实时交易数据 + 增补信息（换手率、量比、振幅、市盈率、市净率）
# supplement = True 表示只添加 增补信息 到数据表， = False 表示所有信息添加到数据表
"""
update_daily_data_from_eastmoney(date= '20190202',supplement=False)


"""
# 调用mysql 存储过程获得 A 股市值
"""
#get_list_a_total_amount()


"""
# 从tushare 获取前一天的 交易数据
"""
# update_daily_data_from_ts(period = -1)


# ws_supplement()