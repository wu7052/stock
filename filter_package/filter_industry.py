from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import date, timedelta
import pandas as pd
from stock_package import ex_web_data

class filter_industry():

    # f_conf 指定策略文件路径
    # f_end_date 指定回测结束日期，默认是最近交易日
    # f_start_date 指定回测开始日期
    def __init__(self, f_conf='', f_start_date='',  f_end_date='', data_src='qfq', industry_name=''):
        wx = lg.get_handle()
        if industry_name == '':
            wx.info("[filter industry] 初始化：行业名称不能为空，退出")
            return None
        else:
            self.industry_name = industry_name
        try:
            self.f_conf = conf_handler(conf=f_conf)
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            # self.pe = self.f_conf.rd_opt('filter_fix', 'pe')
            # self.total_amount = self.f_conf.rd_opt('filter_fix', 'total_amount')
            # self.high_price = self.f_conf.rd_opt('filter_fix', 'high_price')
            # self.days = self.f_conf.rd_opt('filter_fix', 'below_ma55_days')

            self.dgj_table = self.h_conf.rd_opt('db', 'dgj_table')
            self.ws_table = self.h_conf.rd_opt('db', 'ws_table')
            self.repo_table = self.h_conf.rd_opt('db', 'repo_table')

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

            # 选出该行业的所有股票
            sql = "select la.id, la.name, la.sw_level_1, la.sw_level_2 " \
                  "from list_a as la, sw_industry_code  as sw where sw.industry_name  =\"%s\" and" \
                  " (sw.industry_code = la.sw_level_1 or sw.industry_code = la.sw_level_2)"%(self.industry_name)
            self.df_industry_id = self.db._exec_sql(sql=sql)
            self.industry_id_arr = self.df_industry_id.id.tolist()
            self.data_src = data_src
        except Exception as e:
            raise e

    def __del__(self):
        wx = lg.get_handle()
        wx.info("[OBJ] filter_fix : __del__ called")

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
        industry_id_arr_2_str = (",".join(self.industry_id_arr))
        for t_name in tname_arr:
            # 无论是 选股 还是回测 ， self.f_end_date 都是 对应的时间点
            sql = "SELECT id, date, ma_5, ma_10, ma_20, ma_60 from " + t_name + \
                  " where date = " + self.f_end_date +" and id in ("+industry_id_arr_2_str+")"
            # if self.data_src == 'cq' or self.data_src == 'qfq':
            #     sql = "SELECT ma_5, ma_10, ma_20, ma_60 from "+t_name+" where date = "+self.f_end_date
            # else:
            #     sql = "SELECT ma_5, ma_10, ma_20, ma_60 from "+t_name+\
            #           " where date between "+ self.f_start_date+ " and "+self.f_end_date

            if df_ma_grp.empty:
                df_ma_grp = self.db._exec_sql(sql=sql)
            else:
                df_ma_grp = df_ma_grp.append(self.db._exec_sql(sql=sql))

        df_ma_up_grp = df_ma_grp[(df_ma_grp['ma_5'] > df_ma_grp['ma_10']) & (df_ma_grp['ma_10'] > df_ma_grp['ma_20'])
                                 & (df_ma_grp['ma_20'] > df_ma_grp['ma_60'])]
        return df_ma_up_grp

    """
    #  函数说明：股票收盘价 > Ma10 均值，短线趋势未破
    #  需要进行筛选的股票DataFrame df_grp, 包含'id'列， 
    """

    def filter_c_up_ma10(self, df_grp = None):
        wx = lg.get_handle()
        if df_grp is None or df_grp.empty:
            wx.info("[Filter Industry][filter_c_up_Ma10] 输入参数为空 df_grp ")
        else:
            id_arr_2_str = (",".join(df_grp.id.tolist()))
        if self.data_src == 'cq':
            tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_60,
                         self.daily_cq_t_002, self.daily_cq_t_68]
        elif self.data_src == 'qfq':
            tname_arr = [self.daily_qfq_t_00, self.daily_qfq_t_30, self.daily_qfq_t_60,
                         self.daily_qfq_t_002, self.daily_qfq_t_68]
        elif self.data_src == 'bt_qfq':
            tname_arr = [self.bt_daily_qfq_t_00, self.bt_daily_qfq_t_30, self.bt_daily_qfq_t_60,
                         self.bt_daily_qfq_t_002, self.bt_daily_qfq_t_68]
        df_c_grp = pd.DataFrame()
        for t_name in tname_arr:
            # 无论是 选股 还是回测 ， self.f_end_date 都是 对应的时间点
            sql = "SELECT id, date, close from " + t_name + \
                  " where date = " + self.f_end_date +" and id in ("+id_arr_2_str+")"
            # if self.data_src == 'cq' or self.data_src == 'qfq':
            #     sql = "SELECT ma_5, ma_10, ma_20, ma_60 from "+t_name+" where date = "+self.f_end_date
            # else:
            #     sql = "SELECT ma_5, ma_10, ma_20, ma_60 from "+t_name+\
            #           " where date between "+ self.f_start_date+ " and "+self.f_end_date

            if df_c_grp.empty:
                df_c_grp = self.db._exec_sql(sql=sql)
            else:
                df_c_grp = df_c_grp.append(self.db._exec_sql(sql=sql))

        df_c_up_ma10 = pd.merge(df_grp, df_c_grp, how='inner', on=['id'])
        df_c_up_ma10 = df_c_up_ma10[(df_c_up_ma10['close'] > df_c_up_ma10['ma_10'])]
        if not df_c_up_ma10.empty:
            return df_c_up_ma10
        else:
            return None

    # def _ws_total_by_id_process(df):
    #     print(df)
    #     pass

    def filter_ws_record(self, df_grp=None, duration=-180):
        wx = lg.get_handle()
        if df_grp is None or df_grp.empty:
            wx.info("[Filter Industry][filter_ws_record] 输入参数为空 df_grp ")
        else:
            id_arr_2_str = (",".join(df_grp.id.tolist()))

        start_date_str = (date.today() + timedelta(days=duration)).strftime('%Y%m%d')
        sql = "SELECT id, date, disc, price, vol, amount, close_price, b_name, s_name from " +  self.ws_table + \
              " where date >= " + start_date_str +" and id in ("+id_arr_2_str+")"
        df_ws_record = self.db._exec_sql(sql=sql)

        if df_ws_record is None or df_ws_record.empty:
            wx.info("[Filter Industry][filter_ws_record] 大宗交易数据查询结果为空，退出")
            return None

        df_ws_grp_by_id = df_ws_record.groupby(['id'])
        dict_ws_by_id = {'id':[], 'ave_disc':[],'max_disc':[],'min_disc':[], 't_amount':[], 't_vol':[]}
        for count, df_ws_by_id in enumerate(df_ws_grp_by_id):
            dict_ws_by_id['id'].append(df_ws_by_id[0])
            dict_ws_by_id['t_amount'].append(df_ws_by_id[1]['amount'].sum())
            dict_ws_by_id['t_vol'].append(df_ws_by_id[1]['vol'].sum())

            df_ws_by_id[1]['amount_weight']=df_ws_by_id[1]['amount']/df_ws_by_id[1]['amount'].sum()
            df_ws_by_id[1]['disc_weight'] = df_ws_by_id[1]['disc']* df_ws_by_id[1]['amount_weight']
            disc = df_ws_by_id[1]['disc_weight'].sum()
            dict_ws_by_id['ave_disc'].append(disc)
            dict_ws_by_id['max_disc'].append(df_ws_by_id[1]['disc'].max())
            dict_ws_by_id['min_disc'].append(df_ws_by_id[1]['disc'].min())

        df_ws_by_id = pd.DataFrame(dict_ws_by_id)

        return df_ws_by_id