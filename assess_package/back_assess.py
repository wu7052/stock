from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import datetime, time, date, timedelta
import time
import pandas as pd
import numpy as np

class back_trader:
    def __init__(self, trade_date=''):
        wx = lg.get_handle()
        try:
            if trade_date != '':
                self.date = trade_date
            else:
                wx.info("[back_trader] __init__ Failed , trade_date is Empty")
                raise RuntimeError('[back_trader] __init__ Failed , trade_date is Empty')
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            self.pe = self.h_conf.rd_opt('filter_fix', 'pe')
            self.total_amount = self.h_conf.rd_opt('filter_fix', 'total_amount')
            self.high_price = self.h_conf.rd_opt('filter_fix', 'high_price')
            self.days = self.h_conf.rd_opt('filter_fix', 'below_ma55_days')
            self.filter_growth_below_pct = self.h_conf.rd_opt('filter_fix', 'filter_growth_below_pct')
            self.filter_high_left_power_request = self.h_conf.rd_opt('filter_fix', 'filter_high_left_power_request')
            self.filter_high_right_power_request = self.h_conf.rd_opt('filter_fix', 'filter_high_right_power_request')
            self.filter_cur_left_power_request = self.h_conf.rd_opt('filter_fix', 'filter_cur_left_power_request')
            self.filter_golden_pct = self.h_conf.rd_opt('filter_fix', 'filter_golden_pct')
            self.filter_golden_pct_request = float(self.h_conf.rd_opt('filter_fix', 'filter_golden_pct_request'))

            self.daily_cq_t_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
            self.daily_cq_t_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
            self.daily_cq_t_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
            self.daily_cq_t_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')

            self.back_days = self.h_conf.rd_opt('back_assess', 'back_days')
            # self.daily_qfq_t_00 = self.h_conf.rd_opt('db', 'daily_table_qfq_00')
            # self.daily_qfq_t_30 = self.h_conf.rd_opt('db', 'daily_table_qfq_30')
            # self.daily_qfq_t_60 = self.h_conf.rd_opt('db', 'daily_table_qfq_60')
            # self.daily_qfq_t_002 = self.h_conf.rd_opt('db', 'daily_table_qfq_002')

            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)
            wx.info("[OBJ] back_trader : __init__ called")

        except Exception as e:
            raise e


    def get_qfq_data(self):
        tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_002, self.daily_cq_t_60]

        df_amount_grp = pd.DataFrame()
        for t_name in tname_arr:
            sql = "SELECT la.id as id, la.flow_shares * dd.close as tt_amount FROM stock.list_a as la " \
                  " left join " + t_name + " as dd on dd.id = la.id" \
                  " where dd.date = " + self.date + " and la.flow_shares * dd.close between 1 and 10000*" + self.total_amount
            if df_amount_grp.empty:
                df_amount_grp = self.db._exec_sql(sql=sql)
            else:
                df_amount_grp = df_amount_grp.append(self.db._exec_sql(sql=sql))
        df_amount_grp.reset_index(drop=True, inplace=True)
