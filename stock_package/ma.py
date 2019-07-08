from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
from datetime import datetime, time, date, timedelta
import pandas as pd
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

        self.bolling = self.h_conf.rd_opt('bolling', 'duration')

        self.tname_00 = self.h_conf.rd_opt('db', 'daily_table_00')
        self.tname_30 = self.h_conf.rd_opt('db', 'daily_table_30')
        self.tname_60 = self.h_conf.rd_opt('db', 'daily_table_60')
        self.tname_002 = self.h_conf.rd_opt('db', 'daily_table_002')

        host = self.h_conf.rd_opt('db', 'host')
        database = self.h_conf.rd_opt('db', 'database')
        user = self.h_conf.rd_opt('db', 'user')
        pwd = self.h_conf.rd_opt('db', 'pwd')
        self.db = db_ops(host=host, db=database, user=user, pwd=pwd)


    def calc_arr(self, stock_arr, fresh = False):
        wx = lg.get_handle()
        if stock_arr is None:
            wx.info("[Class ma_kits Calc_arr] stock_arr is Empty, Wrong & Return")
            return None

        if re.match('002', stock_arr[0][0]) is not None:
            t_name = self.tname_002
        elif re.match('00', stock_arr[0][0]) is not None:
            t_name = self.tname_00
        elif re.match('30', stock_arr[0][0]) is not None:
            t_name = self.tname_30
        elif re.match('60', stock_arr[0][0]) is not None:
            t_name = self.tname_60
        else:
            wx.info("[Class MA_kits: calc] failed to identify the Stock_id {}".format(stock_arr[0][0]))
            return None

        # today = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() + timedelta(days=-240)).strftime('%Y%m%d')  # 起始日期 为记录日期+1天

        sql = "select id, date, close from " + t_name + " where date >  " + start_date + " order by id"
        df_ma = self.db._exec_sql(sql)
        # df_ma.sort_values(by='date', ascending=True, inplace=True)
        # df_ma.fillna(0, inplace=True)
        # df_grouped = df_ma['close'].groupby(df_ma['id'])
        df_tmp = pd.DataFrame()
        for duration in self.ma_duration:
            df_tmp['MA_' + duration] = df_ma['close'].groupby(df_ma['id']).rolling(int(duration)).mean()
        # 整理 移动均值数据，合并DataFrame
        df_tmp.reset_index(drop=True, inplace=True)
        df_ma = pd.merge(df_ma, df_tmp, left_index=True, right_index=True, how='inner')

        # EMA 12 26 指数移动均值
        # df_grouped = df_ma['close'].groupby(df_ma['id'])
        df_tmp = pd.DataFrame()
        for duration in self.ema_duration:
            # df_tmp['EMA_' + duration] = df_grouped['close'].ewm(span=int(duration)).mean()
            df_tmp['EMA_' + duration] = df_ma['close'].groupby(df_ma['id']).apply(lambda x:x.ewm(span=int(duration)).mean())
        # 整理 指数移动均值数据，合并DataFrame
        df_ma = pd.merge(df_ma, df_tmp, left_index=True, right_index=True, how='inner')

        # MACD 快线
        df_ma['DIF'] = df_ma['EMA_' + self.ema_duration[0]] - df_ma['EMA_' + self.ema_duration[1]]

        # MACD 慢线
        df_tmp = pd.DataFrame()
        df_tmp['DEA'] = df_ma['DIF'].groupby(df_ma['id']).apply(lambda x:x.ewm(span=9).mean())
        df_ma = pd.merge(df_ma, df_tmp, left_index=True, right_index=True, how='inner')

        # Bolling 20 计算
        df_tmp = pd.DataFrame()
        df_ma['bolling_mid'] = df_ma['MA_' + self.bolling]
        df_tmp['tmp2'] = df_ma['close'].groupby(df_ma['id']).rolling(int(self.bolling)).std()
        df_tmp.reset_index(drop=True, inplace=True)
        df_ma = pd.merge(df_ma, df_tmp, left_index=True, right_index=True, how='inner')
        df_ma['bolling_top'] = df_ma['MA_' + self.bolling] + 2 * df_ma['tmp2']
        df_ma['bolling_bottom'] = df_ma['MA_' + self.bolling] - 2 * df_ma['tmp2']
        df_ma.drop(columns=['tmp2'], inplace=True)

        df_ma.drop(columns=['close'], inplace=True)
        df_ma.fillna(0, inplace=True)
        # df_ma.dropna(axis=0, how="any", inplace=True)

        if(fresh == False):
            df_ma[['date']] = df_ma[['date']].apply(pd.to_numeric)
            df_ret = df_ma.iloc[df_ma.groupby(['id']).apply(lambda x: x['date'].idxmax())]
            return  df_ret
        return df_ma


    def calc(self, stock_id, fresh=False):
        wx = lg.get_handle()
        # tname_00 = self.h_conf.rd_opt('db', 'daily_table_00')
        # tname_30 = self.h_conf.rd_opt('db', 'daily_table_30')
        # tname_60 = self.h_conf.rd_opt('db', 'daily_table_60')
        # tname_002 = self.h_conf.rd_opt('db', 'daily_table_002')
        if re.match('002',stock_id) is not None:
            t_name = self.tname_002
        elif  re.match('00', stock_id) is not None :
            t_name = self.tname_00
        elif re.match('30', stock_id) is not None:
            t_name = self.tname_30
        elif  re.match('60', stock_id) is not None :
            t_name = self.tname_60
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

        # Bolling 20 计算
        df_ma['bolling_mid'] = df_ma['MA_'+self.bolling]
        df_ma['tmp2'] = df_ma['close'].rolling(int(self.bolling)).std()
        df_ma['bolling_top'] = df_ma['MA_'+self.bolling]  + 2 * df_ma['tmp2']
        df_ma['bolling_bottom'] = df_ma['MA_'+self.bolling]  - 2 * df_ma['tmp2']
        df_ma.drop(columns=['tmp2'], inplace=True)

        df_ma.drop(columns=['close'], inplace= True)
        df_ma.dropna(axis=0, how="any", inplace=True)
        # df_ma.fillna(0, inplace=True)
        if fresh == False:
            df_ma = df_ma.iloc[-1:]  # 选取DataFrame最后一行，返回的是DataFrame
        return df_ma