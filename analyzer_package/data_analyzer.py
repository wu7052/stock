from conf import conf_handler
import new_logger as lg
from db_package import db_ops
from datetime import date, timedelta
import datetime
import pandas as pd
import re
from painter_package import mp_painter

class analyzer():
    def __init__(self):
        try:
            wx = lg.get_handle()
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)
            self.dd_hot_industry = self.h_conf.rd_opt('db', 'dd_hot_industry')
            # wx.info("[OBJ] analyzer __init__() called")

            self.dd_cq_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
            self.dd_cq_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
            self.dd_cq_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
            self.dd_cq_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')
            self.dd_cq_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')

        except Exception as e:
            raise e

    def __del__(self):
        wx = lg.get_handle()
        self.db.cursor.close()
        self.db.handle.close()
        wx.info("[OBJ] analyzer __del__() called")

    def ana_single_stock(self, s_id=None, start_date=None):
        query_dict = dict()

        # 最近收盘价
        if re.match(r'^002', s_id):
            sql = "select date as `日期`, close as `收盘价` from "+self.dd_cq_002+" where id = " + s_id + " order by date desc limit 1"
        elif re.match(r'^00', s_id):
            sql = "select date as `日期`, close as `收盘价` from "+self.dd_cq_00+" where id = " + s_id + " order by date desc limit 1"
        elif re.match(r'^60', s_id):
            sql = "select date as `日期`, close as `收盘价` from "+self.dd_cq_60+" where id = " + s_id + " order by date desc limit 1"
        elif re.match(r'^30', s_id):
            sql = "select date as `日期`, close as `收盘价` from "+self.dd_cq_30+" where id = " + s_id + " order by date desc limit 1"
        query_dict['最新收盘价'] = sql

        # 董高监 买入 数量 、金额 和 均价
        sql = "select round(sum(vol)/10000,0) as `总买入（万股）` , round(sum(amount)/10000,0) as `总金额（万元）`, " \
              "round(sum(amount)/sum(vol),2) as `买入均价（元）` from dgj_201901 " \
              " where id = " + s_id + " and date > " + start_date + " and vol >= 0"
        query_dict['董高监买入'] = sql

        # 董高监 卖出 数量 、金额  和 均价
        sql = "select round(sum(vol)/10000,0) as `总卖出（万股）` , round(sum(amount)/10000,0) as `总金额（万元）`, " \
              "round(sum(amount)/sum(vol),2) as `卖出均价（元）` from dgj_201901 " \
              " where id = " + s_id + " and date > " + start_date + " and vol < 0"
        query_dict['董高监卖出'] = sql

        # 董高监 净买卖 的数量 、金额  和 均价
        sql = "select round(sum(vol)/10000,0) as `总量（万股）` , round(sum(amount)/10000,0) as `总金额（万元）`, " \
              "round(sum(amount)/sum(vol),2) as `均价（元）` from dgj_201901 " \
              " where id = " + s_id + " and date > " + start_date
        query_dict['董高监增减持'] = sql

        # 大宗交易 溢价成交 的 平均溢价率 、数量、金额 和 均价
        sql = "select round(sum(disc)/count(*)*100,2) as `平均溢价率`, round(sum(vol),2)  as `溢价成交量（万股）`, " \
              "round(sum(amount),2) as `溢价成交金额（万元）`, round(sum(amount)/sum(vol),2) as `溢价成交均价（元）` from ws_201901 " \
              " where id = " + s_id + " and date > " + start_date + " and disc >= 0"
        query_dict['大宗溢价交易'] = sql

        # 大宗交易 折价10%以内成交 的 平均折价率 、数量、金额 和 均价
        sql = "select round(sum(disc)/count(*)*100,2) as `10%以内平均折价率`, round(sum(vol),2) as `10%以内折价成交量（万股）`, " \
              "round(sum(amount),2) as `10%以内折价成交金额（万元）`, round(sum(amount)/sum(vol),2) as `10%以内折价成交均价（元）` from ws_201901 " \
              " where id = " + s_id + " and date > " + start_date + " and disc between -0.1 and 0"
        query_dict['大宗折价交易10%以内'] = sql

        # 大宗交易 折价10%以上成交 的 平均折价率 、数量、金额 和 均价
        sql = "select round(sum(disc)/count(*)*100,2) as `10%以上平均折价率`, round(sum(vol),2) as `10%以上折价成交量（万股）`, " \
              "round(sum(amount),2) as `10%以上折价成交金额（万元）`, round(sum(amount)/sum(vol),2) as `10%以上折价成交均价（元）` from ws_201901 " \
              " where id = " + s_id + " and date > " + start_date + " and disc < -0.1"
        query_dict['大宗折价交易10%以上'] = sql

        # 股票回购 已实施的 平均价格、 数量 和 金额
        sql = "select  round(sum(buy_in_amount)/10000,0) as `回购金额（万元）`, " \
              " round(sum(buy_in_vol)/10000,0) as `回购数量（万股）`, " \
              " round(sum(buy_in_amount)/sum(buy_in_vol),2) as `回购均价（元）` from repo_201901 " \
              " where progress in (004,006) and id = "+ s_id +" and notice_date >"+ start_date
        query_dict['股票回购']=sql

        for key in query_dict.keys():
            query_dict[key] = self.db._exec_sql(query_dict[key])

        query_dict['title'] =s_id + "分析报告"

        return query_dict

    def ana_dgj_trading(self, ass_type=None, start_date=None, vol=10000, ass_weight=0):
        wx = lg.get_handle()
        today = (date.today()).strftime('%Y%m%d')

        sql = "select dgj.id, la.name, '" + today + "'as date, '" + ass_type + "' as ass_type, '" \
              + ass_weight + "' as ass_weight, round(sum(dgj.vol)/10000,0) as ass_comment" \
                             " from dgj_201901 as dgj left join list_a  as la on la.id =  dgj.id " \
                             " where date > " + start_date + " group by dgj.id having sum(dgj.vol)" + vol
        df_dgj_buy = self.db._exec_sql(sql)
        df_dgj = df_dgj_buy
        """
        sql = "select dgj.id, la.name, '"+ today +"' as date, '"+ass_type+"' as ass_type, '-"\
              +ass_weight+"' as ass_weight, round(sum(dgj.vol)/10000,0) as ass_comment " \
              " from dgj_201901 as dgj left join list_a  as la on la.id =  dgj.id " \
              " where date > " + start_date + " group by dgj.id having sum(dgj.vol)<-"+vol
        df_dgj_sell = self.db._exec_sql(sql)
        df_dgj = pd.concat([df_dgj_buy, df_dgj_sell])
        """
        self.db_refresh_fruit(type=ass_type, df_ana=df_dgj)
        if df_dgj is None or df_dgj.empty:
            wx.info("[ana_ws] Ass_Type {} is updated 0 rows".format(ass_type))
        else:
            wx.info("[ana_dgj_trading] Ass_Type {} is updated {} rows".format(ass_type, len(df_dgj)))

    def ana_repo(self, ass_type=None, start_date=None, vol=None, ass_weight=0):
        wx = lg.get_handle()
        today = (date.today()).strftime('%Y%m%d')

        sql = "SELECT id, name, '" + today + "'as date, '" + ass_type + "' as ass_type, '" \
              + ass_weight + "' as ass_weight, round(sum(buy_in_amount)/10000,0) as ass_comment" \
                             " FROM stock.repo_201901 where notice_date>" + start_date + \
              " group by id having sum(buy_in_amount) " + vol
        df_repo = self.db._exec_sql(sql)
        self.db_refresh_fruit(type=ass_type, df_ana=df_repo)

        if df_repo is None or df_repo.empty:
            wx.info("[ana_ws] Ass_Type {} is updated 0 rows".format(ass_type))
        else:
            wx.info("[ana_repo] Ass_Type {} is updated {} rows".format(ass_type, len(df_repo)))

    def ana_ws(self, ass_type=None, start_date=None, ass_weight=None, ass_disc=None, ass_amount=None):
        wx = lg.get_handle()
        today = (date.today()).strftime('%Y%m%d')

        if ass_type.find('-') == -1:
            disc_flag = '>=0'
        else:
            disc_flag = '<0'

        sql = "select ws.id,la.name, '" + today + "'as date, '" + ass_type + "' as ass_type, '" \
              + ass_weight + "' as ass_weight, round(sum(ws.amount),0) as ass_comment " \
                             "from ws_201901 as ws left join list_a as la on la.id = ws.id " \
                             "where ws.date > " + start_date + " and ws.disc " + disc_flag + " " \
                                                                                             "group by ws.id having sum(ws.amount)" + ass_amount + " and sum(ws.disc)/count(ws.disc) " + ass_disc
        df_ws = self.db._exec_sql(sql)
        self.db_refresh_fruit(type=ass_type, df_ana=df_ws)
        if df_ws is None or df_ws.empty:
            wx.info("[ana_ws] Ass_Type {} is updated 0 rows".format(ass_type))
        else:
            wx.info("[ana_ws] Ass_Type {} is updated {} rows".format(ass_type, len(df_ws)))

    def db_refresh_fruit(self, type=None, df_ana=None):
        wx = lg.get_handle()
        sql = "delete from fruit where ass_type='" + type + "'"
        iCount = self.db.cursor.execute(sql)
        self.db.handle.commit()
        wx.info("[db_refresh_fruit] Refresh Ass_Type {}, {} rows of date removed ".format(type, iCount))

        if df_ana is None:
            wx.info("[db_refresh_fruit] Return: Analysis Data Frame or type is Empty,")
            return -1
        ana_array = df_ana.values.tolist()
        i = 0
        while i < len(ana_array):
            ana_array[i] = tuple(ana_array[i])
            i += 1
        sql = "REPLACE INTO fruit SET id=%s, name=%s, date=%s, " \
              "ass_type=%s, ass_weight=%s, ass_comment=%s"
        self.db.cursor.executemany(sql, ana_array)
        self.db.handle.commit()

    def _draw_hot_industry_para_pillar(self, df_industry_pillar = None, title=''):
        industry_arr = list(set(df_industry_pillar.industry_name.tolist()))
        date_arr = list(set(df_industry_pillar.date.tolist()))

        # 生成 全部日期、全部行业 的 qty=0 的 DataFrame
        zero_df_industry_pillar = pd.DataFrame()
        for date in date_arr:
            df_tmp = pd.DataFrame({'date': [date]*len(industry_arr),
                                   'industry_name':industry_arr,
                                   'qty_y':[0]*len(industry_arr)})
            zero_df_industry_pillar = zero_df_industry_pillar.append(df_tmp)

        df_industry_pillar = pd.merge(df_industry_pillar,zero_df_industry_pillar, how='outer', on=['date','industry_name'])
        df_industry_pillar.sort_values(by=['date','industry_name'], ascending=True, inplace=True)
        # 为了明显，把 0 设置为 -1
        df_industry_pillar.fillna(-1, inplace=True)
        df_industry_pillar.drop('qty_y', axis=1, inplace=True)
        # df_industry_pillar.loc[df_industry_pillar['qty'] == 0, 'qty'] = -1
        # 开始画柱状图，以日期为 X 主坐标， X副坐标为 行业
        painter = mp_painter()
        painter.para_pillar_draw(x_label=industry_arr,
                                 x_sub_label=date_arr,
                                 df= df_industry_pillar,
                                 title=title)
        # painter.para_pillar_draw(x_label=df_industry_pillar.industry_name.tolist() ,
        #                          x_sub_label=df_industry_pillar.date.tolist(),
        #                          df= df_industry_pillar)

    def ana_hot_industry(self, duration = 3, level = 1):
        end_date = datetime.date.today().strftime('%Y%m%d')
        start_date = (datetime.date.today() + datetime.timedelta(days=duration*-1)).strftime('%Y%m%d')

        if level ==1:
            # 按日期统计 dd_hot_industry 表的前10名
            sql = "SELECT date, industry_code, industry_name , count(industry_name) as qty FROM stock.dd_hot_industry " \
                  " where date between "+start_date+" and "+end_date+" group by date, industry_name order by date desc"
            df_hot_industry = self.db._exec_sql(sql=sql)

            # 统计各个板块的股票数量
            sql1 = 'select sw_level_1 as industry_code, count(id) as total_qty  from list_a  ' \
                  ' where sw_level_1 is not NULL group by sw_level_1'
            df_industry_qty = self.db._exec_sql(sql=sql1)
        elif level == 2:
            # 按日期统计 dd_hot_industry 表的前10名
            sql = "SELECT date, industry_code_2 as industry_code, industry_name_2 as industry_name , count(industry_name_2) as qty FROM stock.dd_hot_industry " \
                  " where date between " + start_date + " and " + end_date + " group by date, industry_name_2 order by date desc"
            df_hot_industry = self.db._exec_sql(sql=sql)

            # 统计各个板块的股票数量
            sql1 = 'select sw_level_2 as industry_code, count(id) as total_qty  from list_a  ' \
                   ' where sw_level_2 is not NULL group by sw_level_2'
            df_industry_qty = self.db._exec_sql(sql=sql1)


        # 生成按股票数量/该板块总数 的柱状图的 dataframe
        dd_percent_hot_industry = pd.merge(df_hot_industry, df_industry_qty, how='left', on=['industry_code'])
        dd_percent_hot_industry['qty'] = dd_percent_hot_industry['qty'] / dd_percent_hot_industry['total_qty']*100
        # 选出每天的前 N 名的板块，然后再重新查询 这些板块 在过去 X 天的表现
        dd_percent_hot_industry_name = dd_percent_hot_industry.groupby(['date']).apply(
            lambda x: x.sort_values(by=['qty'], ascending=False).head(5))

        # 股票名称 --> 数组 --> 字符串，用来过滤 dataframe
        percent_hot_industry_name_arr = list(set(dd_percent_hot_industry_name.industry_name.tolist()))
        # percent_hot_industry_name_str = (",".join(percent_hot_industry_name_arr))
        # dd_percent_hot_industry_1 = dd_percent_hot_industry[dd_percent_hot_industry['industry_name'].str.contains(percent_hot_industry_name_str)]
        # 从dd_percent_hot_industry筛选对应板块的股票
        dd_percent_hot_industry_1 = pd.DataFrame()
        for industry_name in percent_hot_industry_name_arr:
            df_tmp = dd_percent_hot_industry.loc[dd_percent_hot_industry['industry_name'] == industry_name, ]
            if dd_percent_hot_industry_1.empty:
                dd_percent_hot_industry_1 = df_tmp
            else:
                dd_percent_hot_industry_1 = dd_percent_hot_industry_1.append(df_tmp)

        dd_percent_hot_industry_1.reset_index(drop=True, inplace=True)
        dd_percent_hot_industry_1.drop('total_qty', axis=1, inplace=True)
        dd_percent_hot_industry_1.sort_values(by=['date','industry_name'], ascending=True, inplace=True)
        dd_percent_hot_industry_1.reset_index(drop=True, inplace=True)
        self._draw_hot_industry_para_pillar(df_industry_pillar = dd_percent_hot_industry_1, title='个股涨幅榜（按Level_'+str(level)+'_行业内占比）')

        # 生成按股票数量统计 柱状图的 dataframe
        dd_grp_by_date = df_hot_industry.groupby(['date']).apply(
            lambda x: x.sort_values(by=['qty'], ascending=False).head(5))
        # 股票名称 --> 数组 --> 字符串，用来过滤 dataframe
        dd_grp_by_date_name_arr = list(set(dd_grp_by_date.industry_name.tolist()))
        # percent_hot_industry_name_str = (",".join(percent_hot_industry_name_arr))
        # dd_percent_hot_industry_1 = dd_percent_hot_industry[dd_percent_hot_industry['industry_name'].str.contains(percent_hot_industry_name_str)]
        # 从df_hot_industry筛选对应板块的股票
        dd_grp_by_date_1 = pd.DataFrame()
        for industry_name in dd_grp_by_date_name_arr:
            df_tmp = df_hot_industry.loc[df_hot_industry['industry_name'] == industry_name, ]
            if dd_grp_by_date_1.empty:
                dd_grp_by_date_1 = df_tmp
            else:
                dd_grp_by_date_1 = dd_grp_by_date_1.append(df_tmp)

        dd_grp_by_date_1.reset_index(drop=True, inplace=True)
        dd_grp_by_date_1.sort_values(by=['date','industry_name'], ascending=True, inplace=True)
        self._draw_hot_industry_para_pillar(df_industry_pillar = dd_grp_by_date_1, title='个股涨幅榜（按Level_'+str(level)+'_行业内数量）')

        dd_final_hot = pd.merge(dd_grp_by_date_1, dd_percent_hot_industry_1 , how='inner', on=['date','industry_name','industry_code'])
        dd_final_hot.sort_values(by=['date','industry_name'], ascending=True, inplace=True)
        dd_final_hot.drop(['qty_y'], axis=1, inplace=True)
        dd_final_hot.rename(columns={'qty_x': 'qty'}, inplace=True)
        dd_final_hot.reset_index(drop=True, inplace=True)
        self._draw_hot_industry_para_pillar(df_industry_pillar=dd_final_hot,title='个股涨幅榜（按Level_'+str(level)+'_占比、数量综合）')
