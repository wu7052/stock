# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ma_kits, psy_kits
from report_package import ws_rp
# ma = ma_kits()
# psy = psy_kits()
# np_cprice = psy.get_cprice(stock_id= "600000")
# psy.calc(np_cprice)

import pandas as pd
from filter_package import filter_fix, filter_industry
from assess_package import back_trader
from functions import *

# f_industry = filter_industry( f_conf='filter_rules\\filter_001.conf', data_src='qfq', industry_name='计算机应用')
# df_ma_up_grp = f_industry.filter_ma_up()
# df_c_up_ma10_grp = f_industry.filter_c_up_ma10(df_grp = df_ma_up_grp)
# df_ws_grp = f_industry.filter_ws_record(df_grp=df_ma_up_grp, duration=-30)
# print(df_ws_grp)


# wx.info("============================[update_sh_basic_info_2]上证主板基础信息更新==========================================")
# update_sh_basic_info_2()
# wx.info("============================[update_sh_basic_info_kc]科创板基础信息更新==========================================")
# update_sh_basic_info_kc()
# analysis_dgj()
# analysis_hot_industry(duration = 10)
# 按板块股票数量 比例出图
# bt = back_trader(f_date='20190130', f_b_days=-100,  f_name='filter_rules\\filter_001.conf')
# bt.clear_bt_data()
# bt.get_qfq_data()


# update_sh_basic_info_kc()
# update_sh_basic_info_2()

# update_hot_industry(start_date= '20190101', end_date='20190917')
# analysis_hot_industry(duration = 5, level=2)

# wx.info("============================[filter_A 选股]==========================================")
# filter_A(data_src='qfq')
# wx.info("============================[update_sh_basic_info_2]上证主板基础信息更新==========================================")
# update_sh_basic_info_2()
# wx.info("============================[update_sh_basic_info_kc]科创板基础信息更新==========================================")
# update_sh_basic_info_kc()
# wx.info("============================[update_sz_basic_info]深证主板、中小板、创业板基础信息更新==========================================")
# update_sz_basic_info()
# wx.info("============================[update_sw_industry_into_basic_info]申万行业信息更新==========================================")
# update_sw_industry_into_basic_info()

# update_daily_data_from_ts(period = -1)
# wx.info("============================[update_last_day_qfq_data_from_ts]更新个股前复权数据==========================================")
# qfq_id_arr = update_last_day_qfq_data_from_ts(start= 0)

# update_daily_data_from_eastmoney(supplement=False)
# update_daily_data_from_eastmoney(date= '20190712',supplement=False)
# update_daily_data_from_ts(period = -240, type='qfq')
# qfq_id_arr = update_last_day_qfq_data_from_ts()
# update_ind_ma_single(id_arr=qfq_id_arr, data_src='qfq')

# update_daily_qfq_data_from_ts()
# from conf import conf_handler

# update_ind_ma_2(fresh=True)
# update_ind_psy(fresh = False)
#
# update_daily_data_from_ts(period = -10)
# update_daily_data_from_eastmoney(supplement=False)
#
# wx.info("============================[update_ind_ma_df]除权数据指标（增量更新）==========================================")
# update_ind_ma_df(fresh=True, data_src='cq')
# wx.info("============================[update_ind_ma_df]前复权数据指标（增量更新）==========================================")
# update_ind_ma_df(fresh=True, data_src='qfq')
# wx.info("============================[update_ind_ma_single]当日个股前复权数据指标（个股全部更新）==========================================")
# update_ind_ma_single(id_arr=['000156'], data_src='qfq')
# 常用功能，从 eastmoney 获得全部的 日交易数据，同时更新  'cq' \ 'qfq' 两类表
# wx.info("============================[update_daily_data_from_eastmoney]当日交易数据==========================================")
# update_daily_data_from_eastmoney(supplement=False)

# wx.info("============================[update_dd_by_date_from_ts] 更新指定日期的交易数据==========================================")
# update_dd_by_date_from_ts(q_date='20190313')

# wx.info("=========================[verify_trade_date]===============================")
# verify_trade_date(start_date= '20180610')
reporter = ws_rp()
analysis_single_stock(rp=reporter, id_arr = ['300368'])

# analysis_hot_industry(duration = 5)

# wx.info("============================[update_whole_sales_data_from_eastmoney]大宗交易数据==========================================")
# update_whole_sales_data_from_eastmoney(force=False)
#
#
# """####################################################################
# 从 eastmoney 获得 董高监 的交易数据（日期、股票、成交人、价格、数量、董高监、关系）
# """
# wx.info("============================[update_dgj_trading_data_from_eastmoney]董高监交易数据==========================================")
# update_dgj_trading_data_from_eastmoney(force= False)