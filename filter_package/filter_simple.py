from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import  date, timedelta
import pandas as pd


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
            self.filter_growth_below_pct = self.f_conf.rd_opt('filter_fix', 'filter_growth_below_pct')
            self.filter_high_left_power_request = self.f_conf.rd_opt('filter_fix', 'filter_high_left_power_request')
            self.filter_high_right_power_request = self.f_conf.rd_opt('filter_fix', 'filter_high_right_power_request')
            self.filter_cur_left_power_request = self.f_conf.rd_opt('filter_fix', 'filter_cur_left_power_request')
            self.filter_golden_pct = self.f_conf.rd_opt('filter_fix', 'filter_golden_pct')
            self.filter_golden_pct_request = float(self.f_conf.rd_opt('filter_fix', 'filter_golden_pct_request'))

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
                self.f_start_date = '20190101' #(date.today() + timedelta(days=-240)).strftime('%Y%m%d')
            elif data_src == 'bt_qfq':
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
        # if self.data_src == 'qfq':
        #     tname_arr = [self.daily_qfq_t_00, self.daily_qfq_t_30, self.daily_qfq_t_002, self.daily_qfq_t_60]
        # elif self.data_src == 'cq':
        #     tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_002, self.daily_cq_t_60]
        # elif self.data_src == 'bt_qfq':
        #     tname_arr = [self.bt_daily_qfq_t_00, self.bt_daily_qfq_t_30, self.bt_daily_qfq_t_002, self.bt_daily_qfq_t_60]

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
            if (df_high_price_grp.empty):
                df_high_price_grp = self.db._exec_sql(sql=sql)
            else:
                df_high_price_grp = df_high_price_grp.append(self.db._exec_sql(sql=sql))
        df_high_price_grp.reset_index(drop=True, inplace=True)
        return df_high_price_grp

    """
    函数说明：最近收盘价 及 MA5 都低于 MA55  的股票列表
    """

    def filter_below_ma55(self,):
        if self.data_src == 'qfq':
            t_ma_00 = self.h_conf.rd_opt('db', 'ma_qfq_table_00')
            t_ma_30 = self.h_conf.rd_opt('db', 'ma_qfq_table_30')
            t_ma_60 = self.h_conf.rd_opt('db', 'ma_qfq_table_60')
            t_ma_68 = self.h_conf.rd_opt('db', 'ma_qfq_table_68')
            t_ma_002 = self.h_conf.rd_opt('db', 'ma_qfq_table_002')
            t_dd_00 = self.daily_qfq_t_00
            t_dd_30 = self.daily_qfq_t_30
            t_dd_60 = self.daily_qfq_t_60
            t_dd_68 = self.daily_qfq_t_68
            t_dd_002 = self.daily_qfq_t_002
        elif self.data_src == 'bt_qfq':
            t_ma_00 = self.h_conf.rd_opt('db', 'ma_bt_qfq_table_00')
            t_ma_30 = self.h_conf.rd_opt('db', 'ma_bt_qfq_table_30')
            t_ma_60 = self.h_conf.rd_opt('db', 'ma_bt_qfq_table_60')
            t_ma_68 = self.h_conf.rd_opt('db', 'ma_bt_qfq_table_68')
            t_ma_002 = self.h_conf.rd_opt('db', 'ma_bt_qfq_table_002')
            t_dd_00 = self.bt_daily_qfq_t_00
            t_dd_30 = self.bt_daily_qfq_t_30
            t_dd_60 = self.bt_daily_qfq_t_60
            t_dd_68 = self.bt_daily_qfq_t_68
            t_dd_002 = self.bt_daily_qfq_t_002
        elif self.data_src == 'cq':
            t_ma_00 = self.h_conf.rd_opt('db', 'ma_cq_table_00')
            t_ma_30 = self.h_conf.rd_opt('db', 'ma_cq_table_30')
            t_ma_60 = self.h_conf.rd_opt('db', 'ma_cq_table_60')
            t_ma_68 = self.h_conf.rd_opt('db', 'ma_cq_table_68')
            t_ma_002 = self.h_conf.rd_opt('db', 'ma_cq_table_002')
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

    def _acquire_sides(self, id, x, df_high, dir="left"):
        # 取出 目标 ID , index 会带入，所以[0,0]不对
        # id = x.ix[0,0]

        # 取出 目标ID 的 最高价日期
        df_high_date = df_high.ix[df_high['id'] == id, ['date']]
        high_date = df_high_date.values[0][0]

        # 记录 高点 左侧、右侧的 index
        if dir == "left":
            side_index = x[x['date'] > high_date].index
        elif dir == "right":
            side_index = x[x['date'] < high_date].index
        # 根据 index 删除左侧、右侧的数据
        x.drop(index=side_index, inplace=True)

        return x

    """
    函数说明：1）判断左侧 涨幅是否小于 filter_growth_below_pct 要求
              2）判断右侧是否创新低，低于左侧低点 3% 有效  
    """

    def _filter_LR(self, df_side_left=None, df_side_right=None):
        wx = lg.get_handle()
        if df_side_left is None:
            wx.info("[Filter Fix] filter_Left_Side DataFrame is Empty")
            return None

        if df_side_right is None:
            wx.info("[Filter Fix] filter_Right_Side DataFrame is Empty")
            return None

        # 计算 总体涨幅，并判断 是否小于 conf最小涨幅的要求
        high = df_side_left['high'].max()
        low_left = df_side_left['low'].min()
        if (high / low_left - 1) <= float(self.filter_growth_below_pct):
            return 0  # 不满足最小涨幅的要求

        # 判断 右侧是否创新低，条件低于 左侧低点 3%
        low_right = df_side_right['low'].min()
        if low_right  < low_left * 0.97:
            return -1 # 右侧创新低，不满足要求

        return 1  # 选中，满足要求

    """
    函数说明：处理 单个股票 左侧 或 右侧的数据，根据规则过滤后，返回 数组[left_power，cur_left_power]
    """

    def _filter_strength(self, df_side_left=None, df_side_right=None):
        wx = lg.get_handle()
        if df_side_left is None:
            wx.info("[Filter Fix] filter_Left_Side DataFrame is Empty")
            return None

        if df_side_right is None:
            wx.info("[Filter Fix] filter_Right_Side DataFrame is Empty")
            return None

        # 按日期排序，由小到大

        df_side_right = df_side_right.sort_values('date', ascending=False).copy()

        # 涨幅 变整数，便于统计数量
        df_side_left['pct_chg_int'] = df_side_left['pct_chg'].astype(int).copy()
        df_side_left.loc[df_side_left[(df_side_left['pct_chg'] >= 11)].index, ['pct_chg_int']] = 10
        df_side_left.loc[df_side_left[(df_side_left['pct_chg'] <= -11)].index, ['pct_chg_int']] = -10

        df_side_right['pct_chg_int'] = df_side_right['pct_chg'].astype(int).copy()
        df_side_right.loc[df_side_right[(df_side_right['pct_chg'] >= 11)].index, ['pct_chg_int']] = 10
        df_side_right.loc[df_side_right[(df_side_right['pct_chg'] <= -11)].index, ['pct_chg_int']] = -10

        # 读取 高点左侧 涨幅的权重表
        high_left_power_conf = dict(self.f_conf.rd_sec(sec='filter_high_left_power_table'))
        zero_power_table = dict(zip(range(-10, 11), [0] * 21))  # 从 -10% 到 10% 全部权重为0
        high_left_power = 0
        for key in high_left_power_conf.keys():
            power_item = high_left_power_conf[key].split("#")
            k = [int(x) for x in power_item[0].split(",")]
            v = [int(x) for x in power_item[1].split(",")]
            real_power_table = zero_power_table.copy()
            real_power_table.update(dict(zip(k, v)))  # 从配置文件读取权重，赋值
            high_left_power_conf[key] = real_power_table.copy()

            # Key 是天数，作为统计周期，获得这个周期内的交易数据
            df_lastest_side = df_side_left.head(int(key))
            pct_count = dict(df_lastest_side['pct_chg_int'].value_counts())

            # 获得这个周期内的 high_left_power 得分
            for key in pct_count.keys():
                high_left_power += pct_count[key] * real_power_table[key]

        # 读取 最新交易日 左侧 涨幅的权重表
        cur_left_power_conf = dict(self.f_conf.rd_sec(sec='filter_cur_left_power_table'))
        zero_power_table = dict(zip(range(-10, 11), [0] * 21))  # 从 -10% 到 10% 全部权重为0
        cur_left_power = 0
        for key in cur_left_power_conf.keys():
            power_item = cur_left_power_conf[key].split("#")
            k = [int(x) for x in power_item[0].split(",")]
            v = [int(x) for x in power_item[1].split(",")]
            real_power_table = zero_power_table.copy()
            real_power_table.update(dict(zip(k, v)))  # 从配置文件读取权重，赋值
            cur_left_power_conf[key] = real_power_table.copy()

            # Key 是天数，作为统计周期，获得这个周期内的交易数据
            df_lastest_side = df_side_right.head(int(key))
            pct_count = dict(df_lastest_side['pct_chg_int'].value_counts())

            # 获得这个周期内的 left_power 得分
            for key in pct_count.keys():
                cur_left_power += pct_count[key] * real_power_table[key]

        # 读取 高点右侧 涨幅的权重表
        high_right_power_conf = dict(self.f_conf.rd_sec(sec='filter_high_right_power_table'))
        zero_power_table = dict(zip(range(-10, 11), [0] * 21))  # 从 -10% 到 10% 全部权重为0
        high_right_power = 0
        for key in high_right_power_conf.keys():
            power_item = high_right_power_conf[key].split("#")
            k = [int(x) for x in power_item[0].split(",")]
            v = [int(x) for x in power_item[1].split(",")]
            real_power_table = zero_power_table.copy()
            real_power_table.update(dict(zip(k, v)))  # 从配置文件读取权重，赋值
            high_right_power_conf[key] = real_power_table.copy()

            # Key 是天数，作为统计周期，获得这个周期内的交易数据
            df_lastest_side = df_side_right.tail(int(key))
            pct_count = dict(df_lastest_side['pct_chg_int'].value_counts())

            # 获得这个周期内的 left_power 得分
            for key in pct_count.keys():
                high_right_power += pct_count[key] * real_power_table[key]


        ret_arr_power = [high_left_power, high_right_power, cur_left_power]
        return ret_arr_power
        # df_high = df_side_left.sort_values('high', ascending=False).groupby('id', as_index=False).first()
        # df_low = df_side_left.sort_values('low', ascending=True).groupby('id', as_index=False).first()

    """
    函数说明： 1）股票右侧数据，计算最高、最低点的之间 的黄金分割 价格
               2）对比最近的收盘价
    """

    def _filter_golden_price(self, df_right_side=None):
        golden_pct = [float(x) for x in self.filter_golden_pct.split("#")]

        # 右侧最高、最低价格，计算黄金分割线
        high = df_right_side['high'].max()
        low_right = df_right_side['low'].min()
        diff = high - low_right

        # close = df_right_side.loc[df_right_side[(df_right_side['date'] == self.f_end_date)].index, 'close']
        close = df_right_side.tail(1)['close'].values[0]
        min_golden_pct = 0
        for pct in golden_pct:
            gold_price = high- diff*pct
            if gold_price > close:
                min_golden_pct = pct
            else:
                break
        return  min_golden_pct

    """
    函数说明： 1）近半年内，所有A股股票价格高点的日期，切分出 高点左侧 、右侧的数据记录
               2）判断左侧 涨势，高点 到 低点的 涨幅是否超过 30%
               3）判断左侧 力度，高点左侧 5天、10天、20天、30天内 是否有超过 6% 的单日涨幅
    """

    def filter_side(self):
        wx = lg.get_handle()

        # if self.data_src == 'bt_qfq':
        #     start_date = self.f_start_date
        # elif self.data_src == 'cq' or self.data_src == 'qfq':
        #     start_date = (date.today() + timedelta(days=-180)).strftime('%Y%m%d')
        # else:
        #     wx.info("[Filter_fix] [filter_side] data src [{}], Err. ".format(self.data_src))
        #     return

        if self.data_src == 'qfq':
            tname_arr = [self.daily_qfq_t_002, self.daily_qfq_t_68, self.daily_qfq_t_00,
                         self.daily_qfq_t_30, self.daily_qfq_t_60]
        elif self.data_src == 'bt_qfq':
            tname_arr = [self.bt_daily_qfq_t_00, self.bt_daily_qfq_t_30, self.bt_daily_qfq_t_60,
                         self.bt_daily_qfq_t_002, self.bt_daily_qfq_t_68]
        elif self.data_src == 'cq':
            tname_arr = [self.daily_cq_t_00, self.daily_cq_t_30, self.daily_cq_t_60,
                         self.daily_cq_t_002,  self.daily_cq_t_68]
        arr_filter_side = []
        for t_name in tname_arr:
            sql = "select id, date, high, low, close, 100*(close-pre_close)/pre_close as pct_chg from " + t_name + \
                  " where date >  " + self.f_start_date

            # sql = "select id, date, high, low, close, 100*(close-pre_close)/pre_close as pct_chg from "+ self.daily_cq_t_30 +" where id = 300576 and date >  " + start_date

            df_all_tmp = self.db._exec_sql(sql=sql)
            if df_all_tmp is None or df_all_tmp.empty:
                wx.info("[filter_side] {} 数据表未检出任何记录，继续处理...".format(t_name))
                continue

            df_all = df_all_tmp[-df_all_tmp.high.isin([0])]
            df_high = df_all.sort_values('high', ascending=False).groupby('id', as_index=False).first()
            wx.info("[Filter] Completed Located the Highest Price Point in {}".format(t_name))
            # df_left_side = df_all.groupby(['id']).apply(lambda x: self._find_left(x, df_high))

            df_groupby_id = df_all.groupby(['id'])
            for count, df_each_stock in enumerate(df_groupby_id):

                # 获取最高点 左侧 \ 右侧 的数据记录
                df_tmp_left = self._acquire_sides(id=df_each_stock[0], x=df_each_stock[1].copy(), df_high=df_high,
                                                  dir="left")
                df_tmp_right = self._acquire_sides(id=df_each_stock[0], x=df_each_stock[1].copy(), df_high=df_high,
                                                   dir="right")

                # 判断 左侧涨幅 是否满足 filter_growth_below_pct 要求
                # 判断 右侧是否 创新低，条件低于左侧 3%
                ret_LR = self._filter_LR(df_side_left=df_tmp_left, df_side_right=df_tmp_right)
                if ret_LR == 1:
                    wx.info("[Filter LR] {} / {} Filter {} LR ... [LR FOUND!!!] ".format(count + 1, len(df_groupby_id),
                                                                                          df_each_stock[0]))
                elif ret_LR == 0:  # 左侧涨幅不达标
                    wx.info("[Filter LR] {} / {} Filter {} Left Raise Low... [LR PASS] ".format(count + 1,  len(df_groupby_id),
                                                                                                 df_each_stock[0]))
                    continue
                elif ret_LR == -1: # 右侧创新低
                    wx.info("[Filter LR] {} / {} Filter {} Right New Lowest ... [LR PASS] ".format(count + 1, len(df_groupby_id),
                                                                                      df_each_stock[0]))
                    continue

                # 计算该股票 收盘价 接近的最小黄金分割比例
                min_golden_pct = self._filter_golden_price(df_right_side=df_tmp_right)
                if min_golden_pct > self.filter_golden_pct_request:
                    wx.info("[Filter Golden] {} / {} Filter {}  ........ [Golden PCT FOUND!!!] ".format(count + 1,
                                                                                                len(df_groupby_id),
                                                                                                df_each_stock[0]))
                else:
                    wx.info("[Filter Golden] {} / {} Filter {}  ........ [Golden PCT PASS] ".format(count + 1,
                                                                                                len(df_groupby_id),
                                                                                                df_each_stock[0]))
                    continue

                # 根据规则过滤 高点左侧\右侧 、
                arr_power = self._filter_strength(df_side_left=df_tmp_left, df_side_right=df_tmp_right)

                # wx.info("{}:{}".format(df_each_stock[0],arr_power))
                # 配置文件中所有周期的 Power 得分累计
                if arr_power[0] < int(self.filter_high_left_power_request):
                    wx.info("[Filter Power] {} / {} Filter {} High Left Side ... [Power PASS] ".format(count + 1,
                                                                                                len(df_groupby_id),
                                                                                                df_each_stock[0]))
                    continue
                elif arr_power[1] < int(self.filter_high_right_power_request):
                    wx.info("[Filter Power] {} / {} Filter {} High Right Side ... [Power PASS] ".format(count + 1,
                                                                                                len(df_groupby_id),
                                                                                                df_each_stock[0]))
                    continue
                elif arr_power[2] < int(self.filter_cur_left_power_request):
                    wx.info(
                        "[Filter Power] {} / {} Filter {} Cur Left Side ... [Power PASS] ".format(count + 1,
                                                                                            len(df_groupby_id),
                                                                                            df_each_stock[0]))
                    continue
                else:
                    wx.info("[Filter Power] {} / {} Filter {} Strength ........ [Power FOUND!] ".format(count + 1,
                                                                                                len(df_groupby_id),
                                                                                                df_each_stock[0]))
                arr_power.insert(0, df_each_stock[0])

                arr_power.append(min_golden_pct)
                arr_filter_side.append(arr_power)

        df_filter_side = pd.DataFrame(arr_filter_side, columns=['股票代码', '高点左侧得分', '高点右侧得分','今日左侧得分', '收盘价低于(高低点黄金比例)'])
        df_filter_side['得分合计'] = df_filter_side['高点左侧得分'] + df_filter_side['高点右侧得分'] + df_filter_side['今日左侧得分']
        order = ['股票代码', '得分合计', '高点左侧得分', '高点右侧得分','今日左侧得分', '收盘价低于(高低点黄金比例)']
        df_filter_side = df_filter_side[order]
        df_filter_side = df_filter_side.sort_values('得分合计', ascending=False)
        df_filter_side.reset_index(drop=True, inplace=True)
        wx.info("[Filter Fix] Completed Filter the Left & Right Side ")

        return df_filter_side
