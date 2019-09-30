from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import date, timedelta
import pandas as pd
from stock_package import ex_web_data

class filter_fix:

    # f_conf 指定策略文件路径
    # f_end_date 指定回测结束日期，默认是最近交易日
    # f_start_date 指定回测开始日期
    def __init__(self, f_conf='', f_start_date='',  f_end_date='', data_src='qfq'):
        wx = lg.get_handle()
        try:
            self.f_conf = conf_handler(conf=f_conf)
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            self.pe = self.f_conf.rd_opt('filter_fix', 'pe')
            self.total_amount = self.f_conf.rd_opt('filter_fix', 'total_amount')
            self.high_price = self.f_conf.rd_opt('filter_fix', 'high_price')
            self.days = self.f_conf.rd_opt('filter_fix', 'below_ma55_days')
            # self.filter_growth_below_pct = self.f_conf.rd_opt('filter_fix', 'filter_growth_below_pct')
            # self.filter_high_left_power_request = self.f_conf.rd_opt('filter_fix', 'filter_high_left_power_request')
            # self.filter_high_right_power_request = self.f_conf.rd_opt('filter_fix', 'filter_high_right_power_request')
            # self.filter_cur_left_power_request = self.f_conf.rd_opt('filter_fix', 'filter_cur_left_power_request')
            # self.filter_golden_pct = self.f_conf.rd_opt('filter_fix', 'filter_golden_pct')
            # self.filter_golden_pct_request = float(self.f_conf.rd_opt('filter_fix', 'filter_golden_pct_request'))

            self.daily_cq_t_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
            self.daily_cq_t_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
            self.daily_cq_t_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
            self.daily_cq_t_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')
            self.daily_cq_t_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')

            self.daily_qfq_t_00 = self.h_conf.rd_opt('db', 'daily_table_qfq_00')
            self.daily_qfq_t_30 = self.h_conf.rd_opt('db', 'daily_table_qfq_30')
            self.daily_qfq_t_60 = self.h_conf.rd_opt('db', 'daily_table_qfq_60')
            self.daily_qfq_t_68 = self.h_conf.rd_opt('db', 'daily_table_qfq_68')
            self.daily_qfq_t_002 = self.h_conf.rd_opt('db', 'daily_table_qfq_002')

            self.bt_daily_qfq_t_00 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_00')
            self.bt_daily_qfq_t_30 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_30')
            self.bt_daily_qfq_t_60 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_60')
            self.bt_daily_qfq_t_68 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_68')
            self.bt_daily_qfq_t_002 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_002')

            self.ma_cq_table_60 = self.h_conf.rd_opt('db','ma_cq_table_60')
            self.ma_cq_table_30 = self.h_conf.rd_opt('db','ma_cq_table_30')
            self.ma_cq_table_00 = self.h_conf.rd_opt('db','ma_cq_table_00')
            self.ma_cq_table_002 = self.h_conf.rd_opt('db','ma_cq_table_002')
            self.ma_cq_table_68 = self.h_conf.rd_opt('db','ma_cq_table_68')

            self.ma_qfq_table_60 = self.h_conf.rd_opt('db','ma_qfq_table_60')
            self.ma_qfq_table_30 = self.h_conf.rd_opt('db','ma_qfq_table_30')
            self.ma_qfq_table_00 = self.h_conf.rd_opt('db','ma_qfq_table_00')
            self.ma_qfq_table_002 = self.h_conf.rd_opt('db','ma_qfq_table_002')
            self.ma_qfq_table_68 = self.h_conf.rd_opt('db','ma_qfq_table_68')

            self.ma_bt_qfq_table_00 = self.h_conf.rd_opt('db','ma_bt_qfq_table_00')
            self.ma_bt_qfq_table_30= self.h_conf.rd_opt('db','ma_bt_qfq_table_30')
            self.ma_bt_qfq_table_60 = self.h_conf.rd_opt('db','ma_bt_qfq_table_30')
            self.ma_bt_qfq_table_68 = self.h_conf.rd_opt('db','ma_bt_qfq_table_30')
            self.ma_bt_qfq_table_002 = self.h_conf.rd_opt('db','ma_bt_qfq_table_30')

            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)
            wx.info("[OBJ] filter_fix : __init__ called")

            # 指定日期，用于回测，默认日期是当前最近交易日
            if data_src == 'cq' or data_src== 'qfq':
                sql = "SELECT date from " + self.daily_cq_t_00 + " order by date desc limit 1"
                df_date = self.db._exec_sql(sql=sql)
                self.f_end_date = df_date.iloc[0, 0]
                self.f_start_date = (date.today() + timedelta(days=-240)).strftime('%Y%m%d')
            elif data_src == 'bt_qfq':
                if f_end_date=='' or f_start_date=='':
                    wx.info("[Filter_fix]回测未设置起止日期区间[{}-{}]".format(f_start_date, f_end_date))
                    return
                self.f_end_date = f_end_date
                self.f_start_date = f_start_date

            self.data_src = data_src
        except Exception as e:
            raise e

    def __del__(self):
        wx = lg.get_handle()
        wx.info("[OBJ] filter_fix : __del__ called")

    """
    函数说明：PE 小于 pe（50）
    不使用 动态前复权数据
    """

    def filter_pe(self):
        sql = "SELECT id, pe from " + self.daily_cq_t_00 + " where pe between 1 and " + self.pe + " and date = " + self.f_end_date + " union " \
                "SELECT id, pe from " + self.daily_cq_t_30 + " where pe between 1 and " + self.pe + " and date = " + self.f_end_date + " union " \
                "SELECT id, pe from " + self.daily_cq_t_60 + " where pe between 1 and " + self.pe + " and date = " + self.f_end_date + " union " \
                "SELECT id, pe from " + self.daily_cq_t_002 + " where pe between 1 and " + self.pe + " and date = " + self.f_end_date
        df_pe_grp = self.db._exec_sql(sql=sql)
        return df_pe_grp

    """
    函数说明：流动股金额低于 total_amount, 
    不使用 动态前复权数据
    """

    def filter_tt_amount(self):
        tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_002, self.daily_cq_t_60, self.daily_cq_t_68]

        df_amount_grp = pd.DataFrame()
        for t_name in tname_arr:
            sql = "SELECT la.id as id, la.flow_shares * dd.close as tt_amount FROM stock.list_a as la " \
                  " left join " + t_name + " as dd on dd.id = la.id" \
                  " where dd.date = " + self.f_end_date + " and la.flow_shares * dd.close between 1 and 10000*" + self.total_amount
            if df_amount_grp.empty:
                df_amount_grp = self.db._exec_sql(sql=sql)
            else:
                df_amount_grp = df_amount_grp.append(self.db._exec_sql(sql=sql))
        df_amount_grp.reset_index(drop=True, inplace=True)

        return df_amount_grp

    """
    函数说明：股价低于 high_price (50元) 的股票列表
    """

    def filter_high_price(self):
        if self.data_src == 'qfq':
            tname_arr = [self.daily_qfq_t_00, self.daily_qfq_t_30, self.daily_qfq_t_002,
                         self.daily_qfq_t_60, self.daily_qfq_t_68]
        elif self.data_src == 'cq':
            tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_002,
                         self.daily_cq_t_60, self.daily_cq_t_68]
        elif self.data_src == 'bt_qfq':
            tname_arr = [self.bt_daily_qfq_t_00, self.bt_daily_qfq_t_30, self.bt_daily_qfq_t_002,
                         self.bt_daily_qfq_t_60, self.bt_daily_qfq_t_68]

        df_high_price_grp = pd.DataFrame()
        for t_name in tname_arr:
            sql = "SELECT id, high from " + t_name + " where date =  " + self.f_end_date + " and high < " + self.high_price
            if df_high_price_grp is None or  df_high_price_grp.empty:
                df_high_price_grp = self.db._exec_sql(sql=sql)
            else:
                df_high_price_grp = df_high_price_grp.append(self.db._exec_sql(sql=sql))

        if df_high_price_grp is None or df_high_price_grp.empty:
            return None
        else:
            df_high_price_grp.reset_index(drop=True, inplace=True)
            return df_high_price_grp

    """
    函数说明：最近收盘价 及 MA5 都低于 MA55  的股票列表
    """

    def filter_below_ma55(self,):
        if self.data_src == 'qfq':
            t_ma_00 = self.ma_qfq_table_00
            t_ma_30 = self.ma_qfq_table_30
            t_ma_60 = self.ma_qfq_table_60
            t_ma_68 = self.ma_qfq_table_68
            t_ma_002 = self.ma_qfq_table_002
            t_dd_00 = self.daily_qfq_t_00
            t_dd_30 = self.daily_qfq_t_30
            t_dd_60 = self.daily_qfq_t_60
            t_dd_68 = self.daily_qfq_t_68
            t_dd_002 = self.daily_qfq_t_002
        elif self.data_src == 'bt_qfq':
            t_ma_00 = self.ma_bt_qfq_table_00
            t_ma_30 = self.ma_bt_qfq_table_30
            t_ma_60 = self.ma_bt_qfq_table_60
            t_ma_68 = self.ma_bt_qfq_table_68
            t_ma_002 = self.ma_bt_qfq_table_002
            t_dd_00 = self.bt_daily_qfq_t_00
            t_dd_30 = self.bt_daily_qfq_t_30
            t_dd_60 = self.bt_daily_qfq_t_60
            t_dd_68 = self.bt_daily_qfq_t_68
            t_dd_002 = self.bt_daily_qfq_t_002
        elif self.data_src == 'cq':
            t_ma_00 = self.ma_cq_table_00
            t_ma_30 = self.ma_cq_table_30
            t_ma_60 = self.ma_cq_table_60
            t_ma_68 = self.ma_cq_table_68
            t_ma_002 = self.ma_cq_table_002
            t_dd_00 = self.daily_cq_t_00
            t_dd_30 = self.daily_cq_t_30
            t_dd_60 = self.daily_cq_t_60
            t_dd_68 = self.daily_cq_t_68
            t_dd_002 = self.daily_cq_t_002

        tname_arr = [[t_ma_00, t_dd_00], [t_ma_30, t_dd_30], [t_ma_60, t_dd_60],
                     [t_ma_002, t_dd_002], [t_ma_68, t_dd_68]]
        df_below_ma55_grp = pd.DataFrame()
        for t_name in tname_arr:
            # wx.info("{} - {}".format(t_name[0], t_name[1]))
            sql = "SELECT ma.id, dd.close, ma.ma_5, ma.ma_55 FROM " + t_name[0] + " as ma " \
                                                                                  " left join " + t_name[
                      1] + " as dd  on dd.id = ma.id and dd.date = ma.date " \
                           " where dd.date = " + self.f_end_date + " and ma.ma_5 < ma.ma_55 and dd.close < ma.ma_55;"
            if df_below_ma55_grp is None or df_below_ma55_grp.empty:
                df_below_ma55_grp = self.db._exec_sql(sql=sql)
            else:
                df_below_ma55_grp = df_below_ma55_grp.append(self.db._exec_sql(sql=sql))

        if df_below_ma55_grp is None or df_below_ma55_grp.empty:
            return None
        else:
            df_below_ma55_grp.reset_index(drop=True, inplace=True)

        return df_below_ma55_grp


    """
    # 函数说明：均线向上， Ma 5 > Ma 10 > Ma 20 > Ma 60
    """
    def filter_ma_up(self):

        wx = lg.get_handle()
        if self.data_src == 'cq':
            tname_arr = [self.ma_cq_table_60, self.ma_cq_table_30, self.ma_cq_table_002,
                         self.ma_cq_table_00, self.ma_cq_table_68]
        elif self.data_src == 'qfq':
            tname_arr = [self.ma_qfq_table_60, self.ma_qfq_table_30, self.ma_qfq_table_00,
                         self.ma_qfq_table_002, self.ma_qfq_table_68]
        elif self.data_src == 'bt_qfq':
            tname_arr = [self.ma_bt_qfq_table_00, self.ma_bt_qfq_table_30, self.ma_bt_qfq_table_60,
                         self.ma_bt_qfq_table_68, self.ma_bt_qfq_table_002]
        else:
            wx.info("[Filter_Fix][filter_ma_up] Data Source is not legal type{}".format(self.data_src))
        df_ma_grp = pd.DataFrame()
        for t_name in tname_arr:
            # 无论是 选股 还是回测 ， self.f_end_date 都是 对应的时间点
            sql = "SELECT id, date, ma_5, ma_10, ma_20, ma_60 from "+t_name+" where date = "+self.f_end_date
            # if self.data_src == 'cq' or self.data_src == 'qfq':
            #     sql = "SELECT ma_5, ma_10, ma_20, ma_60 from "+t_name+" where date = "+self.f_end_date
            # else:
            #     sql = "SELECT ma_5, ma_10, ma_20, ma_60 from "+t_name+\
            #           " where date between "+ self.f_start_date+ " and "+self.f_end_date

            if df_ma_grp.empty:
                df_ma_grp = self.db._exec_sql(sql=sql)
            else:
                df_ma_grp = df_ma_grp.append(self.db._exec_sql(sql=sql))

        df_ma_up_grp = df_ma_grp[(df_ma_grp['ma_5']>df_ma_grp['ma_10']) & (df_ma_grp['ma_10']>df_ma_grp['ma_20'])
                                 & (df_ma_grp['ma_20']>df_ma_grp['ma_60'])]
        return df_ma_up_grp

    """
    # 函数说明：当日涨幅超过7%的个股及行业, 录入数据表 dd_hot_industry
    """
    def filter_dd_pct_top(self):
        web_data = ex_web_data()
        wx = lg.get_handle()
        if self.data_src == 'cq':
            tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_60,
                         self.daily_cq_t_002, self.daily_cq_t_68]
        elif self.data_src == 'qfq':
            tname_arr = [self.daily_qfq_t_00, self.daily_qfq_t_30, self.daily_qfq_t_60,
                         self.daily_qfq_t_002, self.daily_qfq_t_68]
        elif self.data_src == 'bt_qfq':
            tname_arr = [self.bt_daily_qfq_t_00, self.bt_daily_qfq_t_30, self.bt_daily_qfq_t_60,
                         self.bt_daily_qfq_t_002, self.bt_daily_qfq_t_68]
        else:
            wx.info("[Filter_Fix][filter_dd_pct_top] Data Source is not legal type{}".format(self.data_src))
        df_pct_top_grp = pd.DataFrame()
        for t_name in tname_arr:
            # 无论是 选股 还是回测 ， self.f_end_date 都是 对应的时间点
            sql = "SELECT dd.id , dd.date ,la.name, la.sw_level_1, sw.industry_name, dd.pct_chg FROM "+t_name+\
                  " as dd left join list_a as la on la.id=dd.id " \
                  " left join sw_industry_code as sw on sw.industry_code =  la.sw_level_1" \
                  " where dd.date =  "+self.f_end_date+" and dd.pct_chg > 7"
                  # " where dd.date between 20190101 and 20190829 and dd.pct_chg > 7"

            if df_pct_top_grp.empty:
                df_pct_top_grp = self.db._exec_sql(sql=sql)
            else:
                df_pct_top_grp = df_pct_top_grp.append(self.db._exec_sql(sql=sql))


        if not df_pct_top_grp.empty:
            df_pct_top_grp.reset_index(drop=True,inplace=True)
            df_pct_top_grp.replace([None], ['None'], inplace=True)
            # web_data.db_load_into_hot_industry(df_hot_industry=df_pct_top_grp)

        return df_pct_top_grp
