from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import pandas as pd
import re

from datetime import datetime, time, date, timedelta
import time

class ma_kits(object):
    def __init__(self):
        # wx = lg.get_handle()
        self.h_conf = conf_handler(conf="stock_analyer.conf")
        ma_str = self.h_conf.rd_opt('ma', 'duration')
        self.ma_duration = ma_str.split(",")

        ema_str = self.h_conf.rd_opt('ema', 'duration')
        self.ema_duration = ema_str.split(",")
        # wx.info("ma_duration {}".format(self.ma_duration))

        host = self.h_conf.rd_opt('db', 'host')
        database = self.h_conf.rd_opt('db', 'database')
        user = self.h_conf.rd_opt('db', 'user')
        pwd = self.h_conf.rd_opt('db', 'pwd')
        self.db = db_ops(host=host, db=database, user=user, pwd=pwd)

    def calc(self, stock_id, fresh=False):
        wx = lg.get_handle()
        tname_00 = self.h_conf.rd_opt('db', 'daily_table_00')
        tname_30 = self.h_conf.rd_opt('db', 'daily_table_30')
        tname_60 = self.h_conf.rd_opt('db', 'daily_table_60')
        tname_002 = self.h_conf.rd_opt('db', 'daily_table_002')
        if re.match('002',stock_id) is not None:
            t_name = tname_002
        elif  re.match('00', stock_id) is not None :
            t_name = tname_00
        elif re.match('30', stock_id) is not None:
            t_name = tname_30
        elif  re.match('60', stock_id) is not None :
            t_name = tname_60
        else:
            wx.info ("[Class MA_kits: calc] failed to identify the Stock_id {}".format(stock_id))
            return None

        sql = "select id, date, close from " + t_name + " where id = " + stock_id + " order by date desc limit 240"
        df_ma = self.db._exec_sql(sql)
        df_ma.sort_values(by='date', ascending=True, inplace=True)

        # MA 5 10 20 60 移动均值
        for duration in self.ma_duration:
            # df_ma['MA_' + duration] = pd.rolling_mean(df_ma['close'], int(duration))
            df_ma['MA_' + duration] = df_ma['close'].rolling(int(duration)).mean()

        # EMA 12 26 指数移动均值
        for duration in self.ema_duration:
            df_ma['EMA_' + duration] = df_ma['close'].ewm(span=int(duration)).mean()

        # MACD 快线
        df_ma['DIF'] = df_ma['EMA_' + self.ema_duration[0]] - df_ma['EMA_' + self.ema_duration[1]]

        # MACD 慢线
        df_ma['DEA'] = df_ma['DIF'].ewm(span=9).mean()

        df_ma.drop(columns=['close'], inplace= True)
        df_ma.dropna(axis=0, how="any", inplace=True)
        # df_ma.fillna(0, inplace=True)
        if fresh == False:
            df_ma = df_ma.iloc[-1:]  # 选取DataFrame最后一行，返回的是DataFrame
        return df_ma
