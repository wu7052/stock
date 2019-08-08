from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import datetime, time, date, timedelta
import pandas as pd
from filter_package import filter_fix
from stock_package import ts_data, ex_web_data, ma_kits, psy_kits
import re

class back_trader:
    def __init__(self, f_date='', f_days=-240, f_name=''):
        wx = lg.get_handle()
        try:
            if f_days >= 0 and f_date != '':
                self.f_begin_date = f_date
                f_begin_date = datetime.strptime(self.f_begin_date, "%Y%m%d")
                self.f_end_date = (f_begin_date + timedelta(days=f_days)).strftime('%Y%m%d')
            elif f_date != '':
                self.f_end_date = f_date
                f_end_date = datetime.strptime(self.f_end_date, "%Y%m%d")
                self.f_begin_date = (f_end_date + timedelta(days=f_days)).strftime('%Y%m%d')
            else:
                wx.info("[back_trader] __init__ Failed, f_date is Empty")
                raise RuntimeError('[back_trader] __init__ Failed, trade_date is Empty')

            self.h_conf = conf_handler(conf="stock_analyer.conf")

            self.daily_cq_t_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
            self.daily_cq_t_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
            self.daily_cq_t_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
            self.daily_cq_t_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')
            self.daily_cq_t_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')

            self.bt_daily_qfq_t_00 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_00')
            self.bt_daily_qfq_t_30 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_30')
            self.bt_daily_qfq_t_60 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_60')
            self.bt_daily_qfq_t_68 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_68')
            self.bt_daily_qfq_t_002 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_002')

            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)

            self.ts = ts_data()
            self.web_data = ex_web_data()
            # wx.info("[OBJ] back_trader : __init__ called")

        except Exception as e:
            raise e

    def clear_bt_data(self):
        wx = lg.get_handle()
        bt_tname_dict = {'00': self.bt_daily_qfq_t_00, '30': self.bt_daily_qfq_t_30, '002': self.bt_daily_qfq_t_002,
                         '60': self.bt_daily_qfq_t_60, '68': self.bt_daily_qfq_t_68}
        for key in bt_tname_dict.keys():
            sql = "delete from " + bt_tname_dict[key]
            self.db._exec_sql(sql=sql)
            wx.info("[back_trader] {} is cleared".format(bt_tname_dict[key]))
        wx.info("[back_trader] =========== back trader tables are completed cleared=============")

    """
    # 废弃函数，计算过程在 get_qfq_data 中完成
    """

    def _calc_qfq(self, id_df):
        id_df.fillna(0, inplace=True)
        cur_df = id_df[~id_df['d_factor'].isin([0])].copy()
        cur_df.reset_index(drop=True, inplace=True)
        if cur_df['d_factor'][0] < 1:
            cur_df['open'] *= cur_df['d_factor']
            cur_df['high'] *= cur_df['d_factor']
            cur_df['low'] *= cur_df['d_factor']
            cur_df['close'] *= cur_df['d_factor']
            # cur_df['pre_close'] *= cur_df['d_factor']
            return cur_df
        elif cur_df['d_factor'][0] == 1:
            # oth_df = id_df[id_df['d_factor'].isin([0])]
            id_df['d_factor'] = 1
            return id_df


    def _process_abnormal_qfq_data(self, id_df):
        wx = lg.get_handle()
        id_df.drop_duplicates(['id'], inplace=True)
        # id_df['id'] = id_df['id'].apply(lambda x: x[0:6])
        qfq_df = pd.DataFrame()
        for id in id_df['id']:
            qfq_tmp = self.ts.acquire_qfq_period(id=id, start_date=self.f_begin_date, end_date=self.f_end_date)
            if qfq_tmp is None or qfq_tmp.empty:
                wx.info("[back_trader][_process_abnormal_qfq_data] Failed to acquire {} qfq Data {} - {} ".
                        format(id, self.f_begin_date, self.f_end_date))
            else:
                qfq_df = qfq_df.append(qfq_tmp)
                wx.info("[back_trader][_process_abnormal_qfq_data] Acquired {} qfq Data {} - {} ".
                        format(id, self.f_begin_date, self.f_end_date))
        return qfq_df
        # qfq_df.sort_values('id', ascending=True, inplace=True)
        # ret_dd_qfq_dict[key].reset_index(drop=True, inplace=True)

    def get_qfq_data(self):
        wx = lg.get_handle()
        tname_dict = {'00': self.daily_cq_t_00, '30': self.daily_cq_t_30, '002': self.daily_cq_t_002,
                      '60': self.daily_cq_t_60, '68': self.daily_cq_t_68}
        dd_qfq_00_df = pd.DataFrame()
        dd_qfq_30_df = pd.DataFrame()
        dd_qfq_60_df = pd.DataFrame()
        dd_qfq_002_df = pd.DataFrame()
        dd_qfq_68_df = pd.DataFrame()
        dd_qfq_dict = {"00": dd_qfq_00_df, "30": dd_qfq_30_df, "002": dd_qfq_002_df,
                       "60": dd_qfq_60_df, "68": dd_qfq_68_df}
        for key in tname_dict.keys():
            sql = "select id, date, open, high, low, close, pre_close, chg, pct_chg, vol, amount from " \
                  + tname_dict[key] + " where date between " + self.f_begin_date + " and " + self.f_end_date
            dd_qfq_dict[key] = self.db._exec_sql(sql=sql)
            if dd_qfq_dict[key] is None:
                wx.info("[back_trader] 获得 {} 板块除权数据 0 条".format(key))
            else:
                wx.info("[back_trader] 获得 {} 板块除权数据 {} 条".format(key, len(dd_qfq_dict[key])))

        # 从除权数据记录 更新回测的 起、止时间点
        df_date = dd_qfq_dict['00'].sort_values('date', ascending=False)
        self.f_end_date = df_date.head(1).reset_index(drop=True).loc[0]['date']
        self.f_begin_date = df_date.tail(1).reset_index(drop=True).loc[0]['date']

        # 从tushare 获得复权因子
        end_datetime = datetime.strptime(self.f_end_date, '%Y%m%d')
        cur_datetime = datetime.strptime(self.f_begin_date, '%Y%m%d')

        end_factor_df = self.ts.acquire_factor(date=self.f_end_date)

        while end_factor_df.empty or end_factor_df is None:
            # end_datetime += timedelta(days=1)
            # self.f_end_date = end_datetime.strftime('%Y%m%d')
            wx.info("获取终点日期的 复权因子失败，等待10秒，再次尝试...")
            time.sleep(10)
            end_factor_df = self.ts.acquire_factor(date=self.f_end_date)

        wx.info("[get_qfq_data] 回测数据日期区间 [{}] -- [{}]".format(self.f_begin_date, self.f_end_date))

        factor_abnormal_df = pd.DataFrame()

        ret_dd_qfq_00_df = pd.DataFrame()
        ret_dd_qfq_30_df = pd.DataFrame()
        ret_dd_qfq_60_df = pd.DataFrame()
        ret_dd_qfq_002_df = pd.DataFrame()
        ret_dd_qfq_68_df = pd.DataFrame()
        ret_dd_qfq_dict = {"00": ret_dd_qfq_00_df, "30": ret_dd_qfq_30_df, "002": ret_dd_qfq_002_df,
                           "60": ret_dd_qfq_60_df, "68": ret_dd_qfq_68_df}

        while cur_datetime <= end_datetime:
            cur_factor_df = self.ts.acquire_factor(date=cur_datetime.strftime('%Y%m%d'))
            if cur_factor_df.empty or cur_factor_df is None:
                wx.info("[get_qfq_data] {} factor Empty, End Date {}".
                        format(cur_datetime.strftime('%Y%m%d'), self.f_end_date))
                cur_datetime += timedelta(days=1)
                continue
            else:
                factor_tmp = pd.merge(cur_factor_df, end_factor_df, on='ts_code', how='left')
                factor_tmp.rename(
                    columns={'ts_code': 'id', 'trade_date_x': 'date', 'adj_factor_x': 'cur_factor',
                             'trade_date_y': 'end_date', 'adj_factor_y': 'end_factor'}, inplace=True)

                # 左链接，当天的所有复权因子保留，期末的复权因子为空，则设置0
                factor_tmp.fillna(0, inplace=True)
                # 期末复权因子为空的股票，计入 异常清单
                factor_abnormal_df = factor_abnormal_df.append(factor_tmp[(factor_tmp['end_factor'] == 0)])
                # 删除期末复权因子为空的记录
                # factor_tmp.dropna(axis=0, how="any", inplace=True)
                factor_tmp = factor_tmp[~(factor_tmp['end_factor'].isin([0]))]

                # cur_factor_zero = factor_tmp.loc[factor_tmp[(factor_tmp['adj_factor_x'] == 0 )].index,:]
                # end_factor_zero = factor_tmp.loc[factor_tmp[(factor_tmp['adj_factor_y'] == 0 )].index,:]
                # 当天的复权因子
                factor_tmp['d_factor'] = factor_tmp['cur_factor'] / factor_tmp['end_factor']
                factor_tmp['id'] = factor_tmp['id'].apply(lambda x: x[0:6])
                cur_date_str = cur_datetime.strftime('%Y%m%d')

                for key in dd_qfq_dict.keys():
                    if dd_qfq_dict[key] is None:
                        wx.info("[back trader] get_qfq_data 处理 {} 板块 -- 数据 0 条 --- 跳过".format(key))
                        continue
                    cur_qfq_df = dd_qfq_dict[key][(dd_qfq_dict[key]['date'].isin([cur_date_str]))]
                    if cur_qfq_df.empty:
                        wx.info("[back trader] get_qfq_data 处理 {} 板块 -- 数据 0 条 --- 日期{}--- 跳过".
                                format(key, cur_date_str))
                        continue

                    # dd_qfq_dict[key] = pd.merge(dd_qfq_dict[key], factor_tmp, on=['id', 'date'], how='left')
                    cur_qfq_df = pd.merge(cur_qfq_df, factor_tmp, on=['id', 'date'], how='left')
                    cur_qfq_df['open'] *= cur_qfq_df['d_factor']
                    cur_qfq_df['high'] *= cur_qfq_df['d_factor']
                    cur_qfq_df['low'] *= cur_qfq_df['d_factor']
                    cur_qfq_df['close'] *= cur_qfq_df['d_factor']
                    ret_dd_qfq_dict[key] = ret_dd_qfq_dict[key].append(cur_qfq_df)

                    wx.info("[back trader] get_qfq_data 处理 {} 板块 -- 数据 {} 条 --- 日期{}".
                            format(key, cur_qfq_df.shape[0], cur_date_str))

                    """
                    dd_qfq_group_id = dd_qfq_dict[key].groupby(['id'])
                    for count, df_each_stock in enumerate(dd_qfq_group_id):
                        df_each_stock[1].sort_values(by='date', ascending=True, inplace=True)
                        if df_each_stock[1].head(1)['date'].values[0] == cur_date_str:
                            wx.info("[back trader] get_qfq_date 处理 ：{} -- {}".format(df_each_stock[0], cur_date_str))
                            dd_qfq_df = dd_qfq_df.append(self._calc_qfq(df_each_stock[1].copy()))
                        else:
                            wx.info("[back trader] get_qfq_date 处理 ：{} -- {} 无数据跳过".format(df_each_stock[0], cur_date_str))
                            continue
                    """

                cur_datetime += timedelta(days=1)
                wx.info("[back trader]======================= get_qfq_data 开始处理{}的数据 ======================= "
                        .format(cur_datetime.strftime('%Y%m%d')))

        for key in ret_dd_qfq_dict.keys():
            if ret_dd_qfq_dict[key] is None or ret_dd_qfq_dict[key].empty:
                continue
            else:
                ret_dd_qfq_dict[key].sort_values('id', ascending=True, inplace=True)
                ret_dd_qfq_dict[key].reset_index(drop=True, inplace=True)
                ret_dd_qfq_dict[key].drop(['cur_factor', 'end_date', 'end_factor', 'd_factor'], axis=1, inplace=True)
                wx.info("[back trader][db_load_into_daily_data]=================={}=================".format(key))
                self.web_data.db_load_into_daily_data(dd_df=ret_dd_qfq_dict[key], pre_id=key, mode='basic',
                                                      type='bt_qfq')

        qfq_abnormal_df = self._process_abnormal_qfq_data(id_df=factor_abnormal_df)
        if qfq_abnormal_df is None or qfq_abnormal_df.empty:
            wx.info("[back trader][abnormal stock qfq data] is None, 全部前复权数据处理完毕 ")
        else:
            qfq_abnormal_df.rename(
                columns={'ts_code': 'id', 'trade_date': 'date', 'change': 'chg'}, inplace=True)
            qfq_abnormal_df['id'] = qfq_abnormal_df['id'].apply(lambda x: x[0:6])
            qfq_abnormal_df.reset_index(drop=True, inplace=True)
            regx_prefix = {'^002':'002','^00[0,1,3-9]':'00','^60':'60','^30':'30','^68':'68'}
            for key in regx_prefix.keys():
                qfq_abnormal_df_tmp = qfq_abnormal_df.loc[qfq_abnormal_df['id'].str.contains(key) ]
                self.web_data.db_load_into_daily_data(dd_df=qfq_abnormal_df_tmp, pre_id=regx_prefix[key], mode='basic',
                                                    type='bt_qfq')
            wx.info("[back trader][异常数据处理完毕], 全部前复权数据处理完毕 ")