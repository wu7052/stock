from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import datetime, time, date, timedelta
import time
import pandas as pd
import numpy as np
from filter_package import filter_fix


class back_trader:
    def __init__(self, f_date='', f_name=''):
        wx = lg.get_handle()
        try:
            if f_date != '':
                self.date = f_date
            else:
                wx.info("[back_trader] __init__ Failed, trade_date is Empty")
                raise RuntimeError('[back_trader] __init__ Failed, trade_date is Empty')

            if f_name != '':
                # self.f_name = f_name
                self.filter = filter_fix(f_conf=f_name)
            else:
                wx.info("[back_trader] __init__ Failed, Filter Rule is Empty")
                raise RuntimeError('[back_trader] __init__ Failed, Filter Rule is Empty')


            """
            self.f_conf = conf_handler(conf="filter_rules\\filter_001.conf")
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            self.pe = self.f_conf.rd_opt('filter_fix', 'pe')
            self.total_amount = self.f_conf.rd_opt('filter_fix', 'total_amount')
            self.high_price = self.f_conf.rd_opt('filter_fix', 'high_price')
            self.days = self.f_conf.rd_opt('filter_fix', 'below_ma55_days')
            self.filter_growth_below_pct = self.f_conf.rd_opt('filter_fix', 'filter_growth_below_pct')
            self.filter_high_left_power_request = self.f_conf.rd_opt('filter_fix', 'filter_high_left_power_request')
            self.filter_high_right_power_request = self.f_conf.rd_opt('filter_fix', 'filter_high_right_power_request')
            self.filter_cur_left_power_request = self.f_conf.rd_opt('filter_fix', 'filter_cur_left_power_request')
            self.filter_golden_pct = self.f_conf.rd_opt('filter_fix', 'filter_golden_pct')
            self.filter_golden_pct_request = float(self.f_conf.rd_opt('filter_fix', 'filter_golden_pct_request'))
            """
            self.daily_cq_t_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
            self.daily_cq_t_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
            self.daily_cq_t_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
            self.daily_cq_t_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')
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


    def _get_qfq_data(self):
        tname_dict = {"00":self.daily_cq_t_00, "30":self.daily_cq_t_30, "002":self.daily_cq_t_002,
                      "60":self.daily_cq_t_60, "68":self.daily_cq_t_68}
        dd_qfq_00_df, dd_qfq_30_df, dd_qfq_60_df, dd_qfq_002_df, dd_qfq_68_df = pd.DataFrame()
        dd_qfq_dict = {"00":dd_qfq_00_df, "30":dd_qfq_30_df, "002":dd_qfq_002_df,
                      "60":dd_qfq_60_df, "68":dd_qfq_68_df}
        for key in tname_dict.keys():
            sql = "select id, date, open, high, low, close, pre_close, chg, pct_chg, vol, amount from "\
                  +tname_dict[key]+" where date < "+ self.date
            dd_qfq_dict[key] = self.db._exec_sql(sql=sql)

