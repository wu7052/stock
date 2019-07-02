from db_package import db_ops
# from stock_package import  ex_web_data
from conf import conf_handler
import new_logger as lg
from datetime import datetime, time, date, timedelta
import time
import pandas as pd
import numpy as np


class filter_fix():

    def __init__(self):
        wx = lg.get_handle()
        try:
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            self.pe = self.h_conf.rd_opt('filter_fix', 'pe')
            self.total_amount = self.h_conf.rd_opt('filter_fix', 'total_amount')
            self.high_price = self.h_conf.rd_opt('filter_fix', 'high_price')
            self.days = self.h_conf.rd_opt('filter_fix', 'below_ma55_days')
            self.daily_t_00 = self.h_conf.rd_opt('db', 'daily_table_00')
            self.daily_t_30 = self.h_conf.rd_opt('db', 'daily_table_30')
            self.daily_t_60 = self.h_conf.rd_opt('db', 'daily_table_60')
            self.daily_t_002 = self.h_conf.rd_opt('db', 'daily_table_002')

            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)
            wx.info("[OBJ] filter_fix : __init__ called")

            sql = "SELECT date from "+self.daily_t_00+" order by date desc limit 1"
            df_date =self.db._exec_sql(sql=sql)
            self.date = df_date.iloc[0,0]
        except Exception as e:
            raise e

    def __del__(self):
        wx = lg.get_handle()
        wx.info("[OBJ] filter_fix : __del__ called")


    """
    函数说明：PE 小于 pe（50）
    """
    def filter_pe(self):
        sql = "SELECT id, pe from "+self.daily_t_00+" where pe between 1 and "+self.pe+" and date = "+self.date+" union " \
              "SELECT id, pe from "+self.daily_t_30+" where pe between 1 and "+self.pe+" and date = "+self.date+" union " \
              "SELECT id, pe from "+self.daily_t_60+" where pe between 1 and "+self.pe+" and date = "+self.date+" union " \
              "SELECT id, pe from "+self.daily_t_002+" where pe between 1 and "+self.pe+" and date = "+self.date
        df_pe_grp = self.db._exec_sql(sql=sql)
        return df_pe_grp


    """
    函数说明：流动股金额低于 total_amount(100亿）
    """
    def filter_tt_amount(self):
        tname_arr = [self.daily_t_00, self.daily_t_30, self.daily_t_002, self.daily_t_60]
        df_amount_grp = pd.DataFrame()
        for t_name in tname_arr:
            sql = "SELECT la.id as id, la.flow_shares * dd.close as tt_amount FROM stock.list_a as la " \
                  " left join "+t_name+" as dd on dd.id = la.id" \
                  " where dd.date = "+self.date+" and la.flow_shares * dd.close between 1 and 10000*"+self.total_amount
            if (df_amount_grp.empty):
                df_amount_grp = self.db._exec_sql(sql=sql)
            else:
                df_amount_grp = df_amount_grp.append(self.db._exec_sql(sql=sql))
        df_amount_grp.reset_index(drop=True, inplace=True)

        return df_amount_grp


    """
    函数说明：股价低于 high_price (50元) 的股票列表
    """
    def filter_high_price(self):
        tname_arr = [self.daily_t_00, self.daily_t_30, self.daily_t_002, self.daily_t_60]
        df_high_price_grp = pd.DataFrame()
        for t_name in tname_arr:
            sql = "SELECT id, high from "+ t_name +" where date =  "+self.date+" and high < "+self.high_price
            if (df_high_price_grp.empty):
                df_high_price_grp = self.db._exec_sql(sql=sql)
            else:
                df_high_price_grp = df_high_price_grp.append(self.db._exec_sql(sql=sql))
        df_high_price_grp.reset_index(drop=True, inplace=True)
        return df_high_price_grp


    """
    函数说明：最近收盘价 及 MA5 都低于 MA55  的股票列表
    """
    def filter_below_ma55(self):
        t_ma_00 = self.h_conf.rd_opt('db', 'ma_table_00')
        t_ma_30 = self.h_conf.rd_opt('db', 'ma_table_30')
        t_ma_60 = self.h_conf.rd_opt('db', 'ma_table_60')
        t_ma_002 = self.h_conf.rd_opt('db', 'ma_table_002')

        tname_arr = [[t_ma_00, self.daily_t_00], [t_ma_30, self.daily_t_30] ,
                     [t_ma_60, self.daily_t_60], [t_ma_002, self.daily_t_002]]
        df_blow_ma55_grp = pd.DataFrame()
        for t_name in tname_arr:
            # wx.info("{} - {}".format(t_name[0], t_name[1]))
            sql = "SELECT ma.id, dd.close, ma.ma_5, ma.ma_55 FROM "+t_name[0]+" as ma " \
                  " left join "+t_name[1]+" as dd  on dd.id = ma.id and dd.date = ma.date " \
                  " where dd.date = "+self.date+" and ma.ma_5 < ma.ma_55 and dd.close < ma.ma_55;"
            if (df_blow_ma55_grp.empty):
                df_blow_ma55_grp = self.db._exec_sql(sql=sql)
            else:
                df_blow_ma55_grp = df_blow_ma55_grp.append(self.db._exec_sql(sql=sql))
        df_blow_ma55_grp.reset_index(drop=True, inplace=True)

        return df_blow_ma55_grp


    """
    函数说明：近半年内，股票价格高点的日期，与之前1个月内的低点 价格差 30% 以上，并且有 6% 的单日涨幅存在
    """
    def filter_strength(self):
        start_date = (date.today() + timedelta(days=-180)).strftime('%Y%m%d')
        tname_arr = [self.daily_t_00, self.daily_t_30, self.daily_t_60, self.daily_t_002]
        df_strength = pd.DataFrame()
        for t_name in tname_arr:
            sql = "select id, date, high, close, 100*(close-pre_close)/pre_close as pct_chg from "+t_name+\
                  " where date >  "+start_date

            df_strength_tmp = self.db._exec_sql(sql= sql)
            df_strength_tmp.groupby('id').apply(lambda x: x[x.high == x.high.max()])


