from db_package import db_ops

import urllib3
import requests
import chardet
# import logging
import os
from urllib import parse
import new_logger as lg
from datetime import datetime, time, date, timedelta
import pandas as pd
import numpy as np
import json
from jsonpath import jsonpath
import re


class ex_web_data(object):

    def __init__(self):
        wx = lg.get_handle()
        # log_dir = os.path.abspath('.')
        # self.logger = myLogger(log_dir)
        try:
            self.db = db_ops(host='127.0.0.1', db='stock', user='wx', pwd='5171013')
            wx.info("ex_web_data : __init__() called")
        except Exception as e:
            raise e

    def __del__(self):
        # self.logger.wt.info("{} __del__ called".format(self))
        wx = lg.get_handle()
        self.db.cursor.close()
        self.db.handle.close()
        wx.info("ex_web_data :{}: __del__() called".format(self))

    def url_encode(self, str):
        return parse.quote(str)

    def daily_data_table_name(self):
        table_name = date.today().strftime('%Y%m')
        return table_name

    def db_fetch_stock_id(self, pre_id):
        if pre_id == '00%':
            sql = "select id from list_a where id like '" + pre_id + "' and id not like '002%'"
        else:
            sql = "select id from list_a where id like '" + pre_id + "'"
        id = self.db.cursor.execute(sql)
        id_array = self.db.cursor.fetchmany(id)
        return id_array

    def db_load_into_daily_data(self, dd_df=None, t_name=None):
        wx = lg.get_handle()
        if dd_df is None or t_name is None:
            wx.info("Err: Daily Data Frame or Table Name is Empty,")
            return -1
        dd_array = dd_df.values.tolist()
        i = 0
        while i < len(dd_array):
            dd_array[i] = tuple(dd_array[i])
            i += 1
        sql = "REPLACE INTO " + t_name + " SET id=%s, date=%s, open=%s, high=%s, low=%s, " \
                                         "close=%s, pre_close=%s, chg=%s,  pct_chg=%s,vol=%s, amount=%s"
        self.db.cursor.executemany(sql, dd_array)
        self.db.handle.commit()
        # wx.info(dd_array)

    def db_call_procedure(self, p_name, *args):
        wx = lg.get_handle()
        # wx.info("[{}] parameter ==> values {},{},{},{},{},{},{}".format(p_name, args[0], args[1], args[2],
        #                                                                 args[3], args[4],args[5], args[6]))
        self.db.cursor.callproc(p_name, (args[0], args[1], args[2], args[3], args[4], args[5], args[6]))
        self.db.cursor.execute(
            "select @_" + p_name + "_0, @_" + p_name + "_1, @_" + p_name + "_2, @_" + p_name + "_3, @_" + p_name + "_4, @_" + p_name + "_5, @_" + p_name + "_6")
        result = self.db.cursor.fetchall()
        wx.info(result)

    def db_load_into_list_a_2(self, basic_info_df):
        wx = lg.get_handle()
        if (basic_info_df is None):
            wx.info("Err: basic info dataframe is Empty,")
            return -1
        basic_info_array = basic_info_df.values.tolist()
        i = 0
        while i < len(basic_info_array):
            basic_info_array[i] = tuple(basic_info_array[i])
            i += 1
        # wx.info(basic_info_array)
        sql = "REPLACE INTO stock.list_a SET id=%s, name=%s, total_shares=%s, flow_shares=%s, list_date=%s, " \
              "full_name=%s, industry=%s, industry_code=%s"
        self.db.cursor.executemany(sql, basic_info_array)
        self.db.handle.commit()

    """
    db_load_into_list_a() 已经废弃，目前使用新函数 db_load_into_list_a_2() 代替
    """

    def db_load_into_list_a(self, basic_info_df):
        wx = lg.get_handle()
        for basic_info in basic_info_df.get_values():
            sql = "select * from list_a where id ='" + basic_info[0] + "'"
            # sql =  'select count(*) from list_a where id = \'%s\''%basic_info[0]
            iCount = self.db.cursor.execute(sql)  # 返回值，受影响的行数， 不需要 fetchall 来读取了
            if iCount == 0:
                sql = "insert into list_a (id, name, total_shares, flow_shares, list_date, full_name, industry, industry_code) " \
                      "values (%s, %s, %s ,%s, %s, %s, %s, %s)"
                # self.logger.wt.info( "Insert id={0}, name={1}, t_shares={2}, f_shares={3}, date={4}, f_name={5},
                # industry={6}, industry_code={7}". format(basic_info[0], basic_info[1], basic_info[2], basic_info[
                # 3], basic_info[4], basic_info[5], basic_info[6], basic_info[7]))
                wx.info("Insert id={0}, name={1}".format(basic_info[0], basic_info[1]))
                self.db.cursor.execute(sql, (
                    basic_info[0], basic_info[1], float(basic_info[2]), float(basic_info[3]), basic_info[4],
                    basic_info[5], basic_info[6], basic_info[7]))
                self.db.handle.commit()
            elif iCount == 1:
                wx.info("Existed\t[{0}==>{1}]".format(basic_info[0], basic_info[1]))
            else:
                wx.info("iCount == %d , what happended ???" % iCount)

    def get_json_str(self, url, web_flag=None):
        wx = lg.get_handle()
        if web_flag == 'sz_basic':
            header = {
                'User-Agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
                'Connection': 'keep-alive'
            }
        elif web_flag == 'sh_basic':
            header = {
                'Cookie': 'yfx_c_g_u_id_10000042=_ck18012900250116338392357618947; VISITED_MENU=%5B%228528%22%5D; yfx_f_l_v_t_10000042=f_t_1517156701630__r_t_1517314287296__v_t_1517320502571__r_c_2',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
                'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
            }
        elif web_flag == 'eastmoney':
            header = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Cookie': 'st_pvi=71738581877645; st_sp=2018-11-22%2011%3A40%3A40; qgqp_b_id=8db9365e6c143170016c773cee144103; em_hq_fls=js; HAList=a-sz-000333-%u7F8E%u7684%u96C6%u56E2%2Ca-sz-300059-%u4E1C%u65B9%u8D22%u5BCC; st_si=74062085443937; st_asi=delete; st_sn=27; st_psi=20190113183705692-113300301007-4079839165',
                'Host': 'dcfm.eastmoney.com',
                'Upgrade-Insecure-Requests': 1,
                'User-Agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
            }

        requests.packages.urllib3.disable_warnings()
        http = urllib3.PoolManager()
        try:
            raw_data = http.request('GET', url, headers=header)
        except Exception as e:
            raise e
        finally:
            if raw_data.status >= 300:
                wx.info("Web response failed : {}".format(url))
                return None

        # 获得html源码,utf-8解码
        str_type = chardet.detect(raw_data.data)
        # unicode = raw_data.data.decode(str_type['encoding'])
        unicode = lg.str_decode(raw_data.data, str_type['encoding'])
        return unicode

    def sina_daily_data_json_parse(self, json_str=None, date='20100101'):
        # self.logger.wt.info("start to parse BASIC INFO ...\n")
        if json_str is not None:
            json_obj = json.loads(json_str)

            company_code = jsonpath(json_obj, '$..code')  # 公司/A股代码
            open = jsonpath(json_obj, '$..open')
            high = jsonpath(json_obj, '$..high')
            low = jsonpath(json_obj, '$..low')
            close = jsonpath(json_obj, '$..trade')
            pre_close = jsonpath(json_obj, '$..settlement')
            chg = jsonpath(json_obj, '$..pricechange')
            pct_chg = jsonpath(json_obj, '$..changepercent')
            v = jsonpath(json_obj, '$..volume')  # 成交量，单位“股”，需要换算成“手”
            vol = [float(tmp) / 100 for tmp in v]  # 换算成 “手” 成交量
            am = jsonpath(json_obj, '$..amount')  # 成交金额， 单位“元”， 需要换算成 “千”
            amount = [float(tmp) / 1000 for tmp in am]  # 换算成 “千” 成交金额
            cur_date = list(date for _ in range(0, len(company_code)))
            daily_data = [company_code, cur_date, open, high, low, close, pre_close, chg, pct_chg, vol, amount]
            df = pd.DataFrame(daily_data)
            df1 = df.T
            df1.rename(
                columns={0: 'ID', 1: 'Date', 2: 'Open', 3: 'High', 4: 'Low', 5: 'Close', 6: 'Pre_close', 7: 'Chg',
                         8: 'Pct_chg', 9: 'Vol', 10: 'Amount'}, inplace=True)
            # col_name = df1.columns.tolist()
            # col_name.insert(1, 'Date')
            # df1.reindex(columns=col_name)
            # df1['Date'] = date
            return df1
        else:
            # self.logger.wt.info("json string is Null , exit ...\n")
            return None

    def east_ws_json_parse(self, json_str=None):
        if json_str is not None:
            json_obj = json.loads(json_str)
        self.page_count = json_obj['pages']
        if len(json_obj['data']) == 0:
            return None
        dt = jsonpath(json_obj, '$..TDATE')
        date = [re.sub(r'-', '', tmp[0:10]) for tmp in dt]
        id = jsonpath(json_obj, '$..SECUCODE')
        disc = jsonpath(json_obj, '$..Zyl')
        price = jsonpath(json_obj, '$..PRICE')
        vol = jsonpath(json_obj, '$..TVOL')
        v_t = jsonpath(json_obj, '$..Cjeltszb')
        vol_tf = [float(tmp) * 100 for tmp in v_t]  # 换算成百分比，交易量占流动股的百分比
        amount = jsonpath(json_obj, '$..TVAL')
        b_code = jsonpath(json_obj, '$..BUYERCODE')
        s_code = jsonpath(json_obj, '$..SALESCODE')
        close_price = jsonpath(json_obj, '$..CPRICE')
        pct_chg = jsonpath(json_obj, '$..RCHANGE')
        b_name = jsonpath(json_obj, '$..BUYERNAME')
        s_name = jsonpath(json_obj, '$..SALESNAME')
        ws_data = [date, id, disc, price, vol, vol_tf, amount, b_code, s_code, close_price, pct_chg, b_name, s_name]
        df = pd.DataFrame(ws_data)
        df1 = df.T
        df1.rename(columns={0: 'Date', 1: 'ID', 2: 'Disc', 3: 'Price', 4: 'Vol', 5: 'Vol_tf', 6: 'Amount', 7: 'B_code',
                            8: 'S_code', 9: 'Close_price', 10: 'Pct_chg', 11: 'B_name', 12: 'S_name'}, inplace=True)

        # irow = 0
        # while irow < len(df1['Date'].values.tolist()):
        #     date_str = df1['Date'][irow]
        #     date_str = date_str[:10]
        #     date_str = re.sub('-', '', date_str)
        #     df1['Date'][irow] = date_str
        #     irow += 1

        return df1

    def db_load_into_ws(self, ws_df=None):
        wx = lg.get_handle()
        if ws_df is None:
            wx.info("[db_load_into_ws]Err: ws flow dataframe is Empty,")
            return -1
        ws_array = ws_df.values.tolist()
        i = 0
        while i < len(ws_array):
            ws_array[i] = tuple(ws_array[i])
            i += 1
        sql = "REPLACE INTO stock.ws_201901 SET date=%s, id=%s, disc=%s, price=%s, vol=%s, vol_tf=%s, " \
              "amount=%s, b_code=%s, s_code=%s, close_price=%s, pct_chg=%s, b_name=%s, s_name=%s"
        self.db.cursor.executemany(sql, ws_array)
        self.db.handle.commit()

    def db_load_into_ws_share_holder(self, df_share_holder = None):
        wx = lg.get_handle()
        if df_share_holder is None:
            wx.info("[db_load_into_ws_share_holder]Err: ws share holder dataframe is Empty,")
            return -1
        ws_sh_array = df_share_holder.values.tolist()
        i = 0
        while i < len(ws_sh_array):
            ws_sh_array[i] = tuple(ws_sh_array[i])
            i += 1

        sql = "REPLACE INTO STOCK.ws_share_holder SET id=%s, h_code=%s, b_vol=%s, b_price=%s, b_vol_tf=%s, s_vol=%s, " \
              "s_price=%s, s_vol_tf=%s"
        self.db.cursor.executemany(sql, ws_sh_array)
        self.db.handle.commit()


    def whole_sales_stock_id(self):
        sql = "select distinct id from ws_201901"

        iCount = self.db.cursor.execute(sql)  # 返回值
        if iCount > 0:
            arr_id = self.db.cursor.fetchall()
            return arr_id
        else:
            return None

    def whole_sales_start_date(self):
        # sql = "select date from stock.ws_201901  where str_to_date(date,'%Y%m%d') " \
        #       "between str_to_date("+ start +",'%Y%m%d') and str_to_date(" + end+",'%Y%m%d'); "
        sql = "select date from ws_201901 as w order by w.date desc limit 1"

        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        if iCount == 1:
            result = self.db.cursor.fetchone()
            record_date = datetime.strptime(result[0], "%Y%m%d")  # 日期字符串 '20190111' ,转换成 20190111 日期类型
            start_date = (record_date + timedelta(days=1)).strftime('%Y-%m-%d')  # 起始日期 为记录日期+1天
            return start_date
        else:
            return None

    def whole_sales_remove_expired_data(self):
        expire_date = (date.today() + timedelta(days=-550)).strftime('%Y%m%d')

        # select * from ws_201901 where str_to_date(date, '%Y%m%d') < str_to_date('20170906', '%Y%m%d');
        # sql = "select date from stock.ws_201901  where str_to_date(date,'%Y%m%d') " \
        #       "between str_to_date("+ start +",'%Y%m%d') and str_to_date(" + end+",'%Y%m%d'); "
        sql = "delete from ws_201901 where str_to_date(date,'%Y%m%d') < str_to_date("+expire_date+", '%Y%m%d' )"
        # wx.info("[whole_sales_remove_expired_data]: {}".format(sql))
        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        return iCount

    def whole_sales_analysis(self, s_id=None):
        wx = lg.get_handle()
        if s_id is None:
            return -1

        # 查询该股票所有的大宗交易流水，输出成 Dataframe 格式
        sql = "select b_code, b_name, s_code, s_name, vol, vol_tf, price, amount from ws_201901 where id = %s order by date asc"
        self.db.cursor.execute(sql, (s_id))
        self.db.handle.commit()
        ws_flow = self.db.cursor.fetchall()
        columnDes = self.db.cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        df_ws_flow = pd.DataFrame([list(i) for i in ws_flow], columns=columnNames)

        # 买方信息归集整理成 Dataframe
        buyer_vol = df_ws_flow['vol'].groupby(df_ws_flow['b_code']).sum()
        buyer_vol_tf = df_ws_flow['vol_tf'].groupby(df_ws_flow['b_code']).sum()
        buyer_amount = df_ws_flow['amount'].groupby(df_ws_flow['b_code']).sum()
        buyer_price = buyer_amount / buyer_vol

        df_buyer = pd.concat([buyer_vol, buyer_price, buyer_vol_tf], axis=1)
        # df_buyer['id'] = s_id
        df_buyer['s_vol'] = 0
        df_buyer['s_price'] = 0
        df_buyer['s_vol_tf'] = 0
        df_buyer = df_buyer.reset_index()
        df_buyer.columns = ['h_code', 'b_vol', 'b_price', 'b_vol_tf',  's_vol', 's_price', 's_vol_tf']
        df_buyer = df_buyer.reindex(
            columns=['h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf'])

        # 卖方信息归集整理成 Dataframe
        seller_vol = df_ws_flow['vol'].groupby(df_ws_flow['s_code']).sum()
        seller_vol_tf = df_ws_flow['vol_tf'].groupby(df_ws_flow['s_code']).sum()
        seller_amount = df_ws_flow['amount'].groupby(df_ws_flow['s_code']).sum()
        seller_price = seller_amount / seller_vol
        df_seller = pd.concat([seller_vol, seller_price, seller_vol_tf], axis=1)
        # df_seller['id'] = s_id
        df_seller['b_vol'] = 0
        df_seller['b_price'] = 0
        df_seller['b_vol_tf'] = 0
        df_seller = df_seller.reset_index()
        df_seller.columns = ['h_code', 's_vol', 's_price', 's_vol_tf',  'b_vol', 'b_price', 'b_vol_tf']
        df_seller = df_seller.reindex(
            columns=['h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf'])

        # 买方、卖方 Dataframe 合并成一个 Dataframe
        df_share_holder = pd.concat([df_buyer, df_seller], axis=0, join='outer')
        df_share_holder = df_share_holder.groupby('h_code').sum()  #  合并 h_code相同 的 买家 、卖家 数据
        df_share_holder = df_share_holder.reset_index() # 重新整理成一个 Dataframe
        df_share_holder['id'] = s_id
        df_share_holder = df_share_holder.reindex(
            columns=['id','h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf'])

        return df_share_holder
"""
        sql = "select distinct b_code from ws_201901 where id = %s order by date asc"
        self.db.cursor.execute(sql, (s_id))
        self.db.handle.commit()
        arr_buyer = self.db.cursor.fetchall()
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        df_buyer = pd.DataFrame([list(i) for i in arr_buyer], columns=columnNames)

        # wx.info("[whole_sales_analysis] Stock {} record {}",format(s_id, arr_buyer))

        sql = "select distinct s_code from ws_201901 where id = %s order by date asc"
        self.db.cursor.execute(sql, (s_id))
        self.db.handle.commit()
        arr_seller = self.db.cursor.fetchall()
        # wx.info("[whole_sales_analysis] Stock {} record {}", format(s_id, arr_seller))
"""

# wx.info("[whole_sales_analysis] Stock {} record {}",format(s_id, ws_flow))
