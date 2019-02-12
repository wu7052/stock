# from logger_package import myLogger
# from filePackage import MyFile
from db_package import db_ops
from stock_package import ts_data, sz_web_data, sh_web_data, ex_web_data
from report_package import ws_rp
import sys
import os
import pandas as pd
from datetime import datetime, date, timedelta
import time
import new_logger as lg
import re


# 计时器 装饰器
def wx_timer(func):
    def wrapper(*args, **kwargs):
        wx = lg.get_handle()
        start_time = time.time()
        func(*args, **kwargs)
        time_used = time.time() - start_time
        # print("{} used {} seconds".format(func.__name__, time_used))
        wx.info("{} used {:.2f} seconds".format(func.__name__, time_used))

    return wrapper  # 这个语句 不属于 wrapper(), 而是 wx_timer 的返回值. 对应 func 后面这个()调用


"""
# 计时器 装饰器
def wx_timer(func):
    def wrapper():
        wx = lg.get_handle()
        start_time = time.time()
        func()
        time_used = time.time() - start_time
         # print("{} used {} seconds".format(func.__name__, time_used))
        wx.info("{} used {} seconds".format(func.__name__, time_used))
    return wrapper
"""


@wx_timer
def update_sz_basic_info():
    wx = lg.get_handle()
    sz_data = sz_web_data()
    try:
        # 深市 所有股票的基本信息获取
        # sz_data = sz_web_data()
        page_counter = 1
        while True:
            sz_basic_list_url = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1110&" \
                                "TABKEY=tab1&PAGENO=" + str(page_counter) + "&random=0.6886288319449341"
            json_str = sz_data.get_json_str(url=sz_basic_list_url, web_flag='sz_basic')
            pos = json_str.find('"error":null')  # 定位截取Json字符串的位置
            json_str = json_str[1:pos - 1] + '}'
            # wx.info(json_str)
            sz_basic_info_df = sz_data.basic_info_json_parse(json_str)
            wx.info("Total Page:{}---{}\n========================================"
                    .format(sz_data.total_page, page_counter))

            sz_data.db_load_into_list_a_2(sz_basic_info_df)
            # sz_data.db_load_into_list_a(sz_basic_info_df)
            page_counter += 1
            if page_counter > int(sz_data.total_page[0]):
                break
            else:
                continue
    except Exception as e:
        wx.info("[update_sz_basic_info]---{}".format(e))
    finally:
        pass


@wx_timer
def update_sh_basic_info():
    wx = lg.get_handle()
    sh_data = sh_web_data()
    try:
        # 沪市 所有股票的所属行业 DataFrame
        # sh_data = sh_web_data()
        sh_data.industry_df_build()  # 沪市股票 所属的行业类型、公司全称
        # wx.info("Return from [industry_df_build]\n{}".format(sh_data.industry_df))

        # 从Web获取沪市 所有股票的基本信息
        # 从Json读取 股票代码、名称、总股份、流动股份、上市日期
        page_counter = 1
        while True:
            sh_basic_list_url = 'http://query.sse.com.cn/security/stock/getStockListData2.do?&' \
                                'jsonCallBack=jsonpCallback99887&isPagination=true&stockCode=&csrcCode=&areaName=&' \
                                'stockType=1&pageHelp.cacheSize=1&pageHelp.beginPage=' \
                                + str(page_counter) + '&pageHelp.pageSize=25&pageHelp.pageNo=' + str(page_counter) + \
                                '&pageHelp.endPage=' + str(page_counter) + '1&_=1517320503161' + str(page_counter)
            json_str = sh_data.get_json_str(url=sh_basic_list_url, web_flag='sh_basic')
            json_str = '{"content":' + json_str[19:-1] + '}'
            sh_basic_info_df = sh_data.basic_info_json_parse(json_str)
            # wx.info(basic_info_df)

            # 基本信息 与 公司全名、所属行业、行业代码 等补充信息进行合并
            sh_basic_info_df = pd.merge(sh_basic_info_df, sh_data.industry_df, how='left', left_on='ID', right_on='ID')
            wx.info("Total Page:{}---{}\n========================================"
                    .format(sh_data.total_page, page_counter))

            sh_data.db_load_into_list_a_2(sh_basic_info_df)
            # sh_data.db_load_into_list_a(sh_basic_info_df)

            page_counter += 1
            if page_counter > int(sh_data.total_page[0]):
                break
            else:
                continue
    except Exception as e:
        wx.info("[update_sh_basic_info]---{}".format(e))
    finally:
        pass


def _df_sw_code_level(code):
    str_code = str(code)
    if re.match(r'\d{2}0{4}$', str_code):  # level ONE
        return 1
    elif re.match(r'\d{3}[1-9]0{2}$', str_code):  # level 2
        return 2
    elif re.match(r'\d{5}[1-9]$', str_code):  # level 3
        return 3
    elif re.match(r'\d{4}[1-9]0$', str_code):  # level 3
        return 3
    else:
        return 0


# 一次性调用，初始化数据表
@wx_timer
def update_sw_industry_code():
    # wx = lg.get_handle()
    web_data = ex_web_data()
    df_sw_code = pd.read_excel("D:\JetBrains\stock_analyzer\sw_industry_code.xlsx")
    df_sw_code['level'] = df_sw_code.apply(lambda x: _df_sw_code_level(x.industry_code), axis=1)
    web_data.db_load_into_sw_industry(df_sw_code)
    # df_sw_code.to_excel("D:\JetBrains\stock_analyzer\sw_industry_code_level.xlsx", encoding='utf-8', index=True, header=True)


@wx_timer
def update_sw_industry_into_basic_info():
    wx = lg.get_handle()
    web_data = ex_web_data()
    sw_industry_arr = web_data.db_get_sw_industry_code(level=2)
    code_counter = 1
    for code in sw_industry_arr:
        page_counter = 1
        while True:
            sina_industry_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/" \
                                "Market_Center.getHQNodeData?page=" + str(page_counter) + "&num=80&sort=symbol&asc=1&" \
                                                                                          "node=sw2_" + str(
                code[0]) + "&symbol=&_s_r_a=setlen"
            stock_id_json = web_data.get_json_str(url=sina_industry_url, web_flag='sh_basic')
            time.sleep(1)
            if stock_id_json == 'null':
                # wx.info("SW Industry Code{} page {} Null".format(code[0], page_counter))
                break
            else:
                wx.info("SW Industry {}:{}  Code:{}  Page:{} loaded into basic info table".
                        format(code_counter, len(sw_industry_arr), code[0], page_counter))

            # key字段 加引号，整理字符串
            stock_id_json = re.sub(r'([a-z|A-Z]+)(?=:)', r'"\1"', stock_id_json)
            stock_id_arr = web_data.sina_industry_json_parse(stock_id_json)
            web_data.db_update_sw_industry_into_basic_info(code=code[0], id_arr=stock_id_arr)
            page_counter += 1
        code_counter += 1


@wx_timer
def update_daily_data_from_sina(date=None):  # date 把数据更新到指定日期，默认是当天
    wx = lg.get_handle()
    # sz_data = sz_web_data()
    # sh_data = sh_web_data()
    web_data = ex_web_data()
    page_src = (('zxqy', 'stock.code_002_201901', '中小板'), ('cyb', 'stock.code_30_201901', '创业板'),
                ('sz_a', 'stock.code_00_201901', '深证 主板'), ('sh_a', 'stock.code_60_201901', '上证 主板'))

    try:
        for src in page_src:
            # 上证 主板 、深证 主板 、中小板、 创业板
            page_counter = 1
            loop_flag = True
            while loop_flag:
                wx.info("===" * 20)
                wx.info("[update_daily_data_from_sina] downloading {} Page {} ".format(src[2], page_counter))
                sina_daily_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/" \
                                 "Market_Center.getHQNodeData?page=" + str(page_counter) + "&num=80&sort=symbol&" \
                                 "asc=1&node=" + src[0] + "&symbol=&_s_r_a=page"

                daily_str = web_data.get_json_str(url=sina_daily_url, web_flag='sh_basic')
                # daily_str = daily_str[1:-1]
                found = re.match(r'.*symbol', daily_str)
                if found is None:
                    wx.info("[update_daily_data_from_sina] didn't acquire the data from page {}".format(page_counter))
                    break
                else:
                    page_counter += 1

                # 深证A股 的字符串包含 主板、中小板、创业板
                # 'sz_a' 标志截断 中、创
                if src[0] == 'sz_a':
                    trunc_pos = daily_str.find(',{symbol:"sz002')
                    if trunc_pos >= 0:
                        daily_str = daily_str[:trunc_pos] + ']'
                        loop_flag = False  # 完成本次后，退出循环

                # key字段 加引号，整理字符串
                jstr = re.sub(r'([a-z|A-Z]+)(?=:)', r'"\1"', daily_str)

                # 按股票拆分 成list， 这里把整个页面的股票数据统一处理成 dataframe，不做拆分了
                # d_arr = re.findall('{\S+?}',jstr)
                if date is None:
                    today = datetime.now().strftime('%Y%m%d')
                else:
                    today = date

                # 深证 A 股页面 包含了 主板、创业、中小， 所以处理 深证主板的时候，要把 创业、中小 的股票信息去掉
                daily_data_frame = web_data.sina_daily_data_json_parse(json_str=jstr, date=today)
                web_data.db_load_into_daily_data(dd_df=daily_data_frame, t_name=src[1])

    except Exception as e:
        wx.info("Err [update_daily_data_from_sina]: {}".format(e))


@wx_timer
def update_daily_data_from_ts(period=-1):
    wx = lg.get_handle()
    ts = ts_data()
    sz_data = sz_web_data()
    sh_data = sh_web_data()

    try:
        # 中小板
        id_array_002 = sz_data.db_fetch_stock_id(pre_id='002%')
        for id in id_array_002:
            ts_code = id[0] + '.SZ'
            dd_df = ts.acquire_daily_data(ts_code, period)
            dd_df['ts_code'] = id[0]
            # wx.info(dd_df)
            # wx.info("{} daily data loading into DB...".format(ts_code))
            sz_data.db_load_into_daily_data(dd_df=dd_df, t_name='stock.code_002_201901')

        # 上证 主板
        id_array_60 = sh_data.db_fetch_stock_id(pre_id='60%')
        for id in id_array_60:
            ts_code = id[0] + '.SH'
            dd_df = ts.acquire_daily_data(ts_code, period)
            dd_df['ts_code'] = id[0]
            # wx.info(dd_df)
            # wx.info("{} daily data loading into DB...".format(ts_code))
            sh_data.db_load_into_daily_data(dd_df=dd_df, t_name='stock.code_60_201901')

        # 深证 主板， 在 db_fetch_stcok_id() 中已做处理，剔除了 中小板的 002
        id_array_00 = sz_data.db_fetch_stock_id(pre_id='00%')
        for id in id_array_00:
            ts_code = id[0] + '.SZ'
            dd_df = ts.acquire_daily_data(ts_code, period)
            dd_df['ts_code'] = id[0]
            # wx.info(dd_df)
            # wx.info("{} daily data loading into DB...".format(ts_code))
            sz_data.db_load_into_daily_data(dd_df=dd_df, t_name='stock.code_00_201901')

        # 创业板
        id_array_30 = sz_data.db_fetch_stock_id(pre_id='300%')
        for id in id_array_30:
            ts_code = id[0] + '.SZ'
            dd_df = ts.acquire_daily_data(ts_code, period)
            dd_df['ts_code'] = id[0]
            # wx.info(dd_df)
            # wx.info("{} daily data loading into DB...".format(ts_code))
            sz_data.db_load_into_daily_data(dd_df=dd_df, t_name='stock.code_30_201901')
    except Exception as e:
        wx.info("Err:[update_daily_data_from_ts]---{}".format(e))
    finally:
        pass


@wx_timer
def update_daily_data_from_eastmoney(date=None, supplement=True):
    wx = lg.get_handle()
    web_data = ex_web_data()
    page_src = (('C._SZAME', 'stock.code_00_201901', '深证 主板'), ('C.13', 'stock.code_002_201901', '中小板'),
                ('C.80', 'stock.code_30_201901', '创业板'), ('C.2', 'stock.code_60_201901', '上证 主板'))

    try:
        for src in page_src:
            page_count = 1
            items_page = 100
            loop_page = True
            while loop_page:
                east_daily_url = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=" \
                                 "jQuery1124048605539191859704_1549549980465&type=CT&" \
                                 "token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&" \
                                 "js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&" \
                                 "cmd=" + src[0] + "&st=(Code)&sr=1&p=" + str(page_count) + "&ps=" + str(items_page) + \
                                 "&_=1549549980528"

                east_daily_str = web_data.get_json_str(url=east_daily_url, web_flag='eastmoney')

                # 把字符串 拆分成 交易数据",,,,,",",,,,,",",,,,",",,,,,"  和 记录数量 两个部分
                east_daily_str = re.search(r'(?:data\:\[)(.*)(?:\]\D+)(\d+)(?:.*)', east_daily_str)
                daily_data = east_daily_str.group(1)  # 获得交易数据
                total_item = int(east_daily_str.group(2))  # 获得股票总数量，用来计算 页数
                total_page = int((total_item + items_page - 1) / items_page)  # 总页数，向上取整
                wx.info("[update daily data from eastmoney] {}-- page {}/ {}".format(src[2], page_count, total_page))
                page_count += 1
                if page_count > total_page:
                    loop_page = False

                # 把交易数据 进一步拆分成 每支股票交易数据一条字符串 的数组
                east_daily_data = re.findall(r'(?:\")(.*?)(?:\")', daily_data)
                page_arr = list()
                for daily_str in east_daily_data:
                    daily_arr = daily_str.split(',')
                    daily_arr.pop(0)  # 去掉 无意义的 第一个字段
                    if src[0] == 'C._SZAME' and re.match(r'^002', daily_arr[0]) is not None:  # 深圳主板发现 中小板
                        loop_page = False
                        break
                    else:
                        page_arr.append(daily_arr)
                        # wx.info("{}".format(daily_arr[0]))
                page_full_df = pd.DataFrame(page_arr, columns=['id', 'name', 'close', 'chg', 'pct_chg', 'vol', 'amount',
                                                               'pct_up_down',
                                                               'high', 'low', 'open', 'pre_close', 'unknown1', 'qrr',
                                                               'tor', 'pe', 'pb',
                                                               'total_amount', 'total_flow_amount', 'unknown4',
                                                               'unknown5',
                                                               'unknown6', "unknown7", "date", "unknown8"])
                if date is None:
                    page_full_df['date'] = datetime.now().strftime('%Y%m%d')
                else:
                    page_full_df['date'] = date

                if supplement:  # 只采集增补信息
                    page_db_df = page_full_df.loc[:, ['id', 'date', 'qrr', 'tor', 'pct_up_down', 'pe', 'pb']]
                else:  # 采集全部数据
                    page_db_df = page_full_df.loc[:, ['id', 'date', 'open', 'high', 'low', 'close', 'pre_close', 'chg',
                                                      'pct_chg', 'vol', 'amount', 'qrr', 'tor', 'pct_up_down', 'pe',
                                                      'pb']]
                    page_db_df['open'] = page_db_df['open'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['high'] = page_db_df['high'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['low'] = page_db_df['low'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['close'] = page_db_df['close'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pre_close'] = page_db_df['pre_close'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['chg'] = page_db_df['chg'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pct_chg'] = page_db_df['pct_chg'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['vol'] = page_db_df['vol'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['amount'] = page_db_df['amount'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['qrr'] = page_db_df['qrr'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['tor'] = page_db_df['tor'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pct_up_down'] = page_db_df['pct_up_down'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pe'] = page_db_df['pe'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pb'] = page_db_df['pb'].apply(lambda x: '0' if str(x) == '-' else x)

                    page_db_df['amount'] = pd.to_numeric(page_db_df['amount'])
                    page_db_df['amount'] = page_db_df['amount'] / 1000
                    web_data.db_load_into_daily_data(dd_df=page_db_df, t_name=src[1], mode='full')

    except Exception as e:
        wx.info("Err [update_daily_data_from_eastmoney]: {}".format(e))


@wx_timer
def update_dgj_trading_data(force=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    # today = datetime.now().strftime('%Y-%m-%d')
    # select * from ws_201901 where  str_to_date(date, '%Y%m%d') < str_to_date('20180701', '%Y%m%d');

    # 强制刷新 dgj 表，删除所有历史数据，重新导入
    if force:  # == True:
        rows = web_data.dgj_trading_data_remove()
        start_date = (date.today() + timedelta(days=-550)).strftime('%Y-%m-%d')
        wx.info("[update_dgj_trading_data] Force to refresh DGJ table, {} rows REMOVED, ".format(rows))
        wx.info("[update_dgj_trading_data] Collect history data in last 550 days!!!")
    else:
        # 删除过期数据，超过 550天 的数据
        del_rows = web_data.dgj_remove_expired_data()
        wx.info("[update_dgj_trading_data] {} Rows of Expired data Removed ".format(del_rows))

        # 保持 dgj 表的数据，获得 dgj 交易记录的最新日期
        start_date = web_data.dgj_start_date()
        if start_date is None:
            wx.info("[update_dgj_trading_data] Checking lastest date is None !!!")
            return -1
        else:
            wx.info("[update_dgj_trading_data] lastest date is {}".format(start_date))

    start_timestamp = time.mktime(time.strptime(start_date, '%Y-%m-%d'))

    page_counter = 1
    loop_page = True
    while loop_page:
        dgj_eastmoney_url = "http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=GG&sty=GGMX&" \
                            "p="+str(page_counter)+"&ps=100&js=var%20pfXDviDd={pages:(pc),data:[(x)]}&rt=51663059"
        dgj_str = web_data.get_json_str(url=dgj_eastmoney_url, web_flag='eastmoney')
        dgj_str = re.sub(r'.*(?={pages)', r'', dgj_str)  # 去掉 {pages 之前的字符串

        dgj_str = re.search(r'(?:{pages:)(\d+)(?:\D+\:\[)(.*)(?:\]\})',dgj_str)

        total_page = int(dgj_str.group(1)) # 获得总页数
        dgj_trading_str = dgj_str.group(2)  # 获得交易数据

        dgj_trading_data = re.findall(r'(?:\")(.*?)(?:\")', dgj_trading_str)
        page_arr = list()
        for data_str in dgj_trading_data:
            data_arr = data_str.split(',')
            trade_timestamp = time.mktime(time.strptime(data_arr[5], '%Y-%m-%d'))
            trade_date = data_arr[5]
            data_arr[5]= re.sub(r'-','',data_arr[5]) # 日期格式调整，去掉 -
            # wx.info("name:{} / {} / {}".format(data_arr[1],data_arr[3],data_arr[12],data_arr[14] ))
            data_arr[1] = data_arr[1][:20] # dgj_name
            data_arr[3] = data_arr[3][:20] # trader_name
            data_arr[12] = data_arr[12][:20] # trading_type
            data_arr[14] = data_arr[14][:40] # dgj_pos

            if trade_timestamp - start_timestamp > 0:
                page_arr.append(data_arr)
            else:
                loop_page = False
                break

        if len(page_arr) > 0 :
            page_full_df = pd.DataFrame(page_arr, columns=['pct_chg', 'dgj_name', 'id', 'trader_name', 'unknown2', 'date',
                                                           'vol', 'in_hands', 'price', 's_name', 'relation', 's_abb_name',
                                                           'trading_type', 'amount', 'dgj_pos', 'unknown3'])
            # page_df.rename(
            #     columns={0: 'unknown1', 1: 'dgj_name', 2: 'id', 3: 'trader_name', 4: 'unknown2', 5: 'date', 6: 'vol',
            #              7: 'in_hands', 8: 'price', 9: 's_name', 10: 'relation', 11: 's_abb_name', 12: 'trade_type',
            #             13: 'amount', 14: 'dgj_pos', 15: 'unknown3' }, inplace=True)
            page_db_df = page_full_df.loc[:, ['date', 'id', 'dgj_name', 'dgj_pos', 'trader_name', 'relation', 'vol',
                                              'price', 'amount', 'pct_chg', 'trading_type', 'in_hands']]

            web_data.db_load_into_dgj_trade(dd_df=page_db_df)
            wx.info("[update_dgj_trading_data] page {} / {}, date {} : {}".format(page_counter, total_page, trade_date, start_date))
        else:
            wx.info("[update_dgj_trading_data] page {} without updating data/ {}, date {} : {}".format(page_counter, total_page, trade_date, start_date))
        page_counter += 1
        if page_counter > total_page:
            loop_page = False


@wx_timer
def update_whole_sales_data(force=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    today = datetime.now().strftime('%Y-%m-%d')

    # select * from ws_201901 where  str_to_date(date, '%Y%m%d') < str_to_date('20180701', '%Y%m%d');
    page_counter = 1

    # 强制刷新 ws 表，删除所有历史数据，重新导入
    if force:  # == True:
        rows = web_data.whole_sales_data_remove()
        start_date = (date.today() + timedelta(days=-550)).strftime('%Y-%m-%d')
        wx.info("[update_whole_sales_date] Force to refresh  WS data {} rows REMOVED, ".format(rows))
        wx.info("[update_whole_sales_date] Collect history data last 550 days!!!")
    else:
        # 删除过期数据，超过 550天 的数据
        del_rows = web_data.whole_sales_remove_expired_data()
        wx.info("[update_whole_sales_data] {} Rows of Expired data Removed ".format(del_rows))

        # 保持 ws表的数据，从最新日期+1 到 今天 ，获取web最新数据
        start_date = web_data.whole_sales_start_date()
        if start_date is None:
            wx.info("[update_whole_sales_data] Checking lastest date None")
            return -1

    while True:
        ws_eastmoney_url = "http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=DZJYXQ&" \
                           "token=70f12f2f4f091e459a279469fe49eca5&cmd=&st=TDATE&sr=-1&" \
                           "p=" + str(page_counter) + "&ps=50&js=var%20doXCfrVg=%7Bpages:(tp),data:(x)%7D&" \
                                                      "filter=(Stype='EQA')(TDATE%3E=%5E" + start_date +\
                           "%5E%20and%20TDATE%3C=%5E" + today +  "%5E)&rt=51576724"
        # wx.info(ws_eastmoney_url)
        daily_str = web_data.get_json_str(url=ws_eastmoney_url, web_flag='eastmoney')
        daily_str = re.sub(r'.*(?={pages)', r'', daily_str)  # 去掉 {pages 之前的字符串
        daily_str = re.sub(r'(pages)(.*)(data)', r'"\1"\2"\3"', daily_str)  # 给 pages data 加引号，变为合法的 json 串
        ws_df = web_data.east_ws_json_parse(daily_str)  # 组装 Dataframe，准备导入数据库

        if ws_df is None:
            wx.info("[update_whole_sales_data] Total Page {}, updated data NULL".format(web_data.page_count))
            break
        else:
            wx.info("[Eastmoney_ws_data]Total Page:{}---{}, Start date: {}\n========================================"
                    .format(web_data.page_count, page_counter, start_date))
            web_data.db_load_into_ws(ws_df=ws_df)

        if page_counter >= web_data.page_count:
            wx.info("Page : {} is the final page , End ".format(page_counter))
            break
        page_counter += 1


# 分析统计WS表中所有的交易记录，更新一遍 ws_share_holder 表
@wx_timer
def update_ws_share_holder():
    wx = lg.get_handle()
    web_data = ex_web_data()
    arr_id = web_data.whole_sales_stock_id()  # arr_id = ((id),(id),(id)....)
    wx.info("Total {} stocks records in Whole Sales tables".format(len(arr_id)))
    iCounter = -1
    for stock_id in arr_id:
        iCounter += 1
        df_share_holder = web_data.whole_sales_analysis(s_id=stock_id[0])
        if df_share_holder.empty:
            wx.info("{} failed to gather Share Holders' Information".format(stock_id[0]))
            continue
        else:
            web_data.db_load_into_ws_share_holder(df_share_holder=df_share_holder)
            wx.info("{}/{} : {} Loaded Share Holders' Information".format(iCounter, len(arr_id), stock_id[0]))
    wx.info("update_ws_share_holder loaded completed ! ")


@wx_timer
def ws_supplement():
    wx = lg.get_handle()
    web_data = ex_web_data()
    wx.info("Start to supplement more information ")
    # 调用 mysql 存储过程，在 ws表中 补充 stock_name , h_name, buy_date, sell_date
    if web_data.whole_sales_supplement_info():
        wx.info("update_ws_share_holder succeed !")
    else:
        wx.info("update_ws_share_holder failed !")


@wx_timer
def report_total_amount():
    # wx = lg.get_handle()
    rp = ws_rp()
    rp.calc_total_amount()


@wx_timer
def report_days_vol(days=0, type=None):
    # wx = lg.get_handle()
    rp = ws_rp()
    df_days_vol = rp.calc_days_vol(days, type)
    return df_days_vol



"""
# Logger 测试代码

logger = myLogger('.')
logger.wt.info("from logger [wuxiang]")
logger.wt.info('It works!')  # 记录该文件的运行状态
logger.wt.debug('debug message')
logger.wt.warning('warning message')
logger.wt.error('error message')
logger.wt.critical('critical message')
"""

"""
# MySQL 测试代码
try:
    db = db_ops(host='127.0.0.1', db='stock', user='wx', pwd='5171013')
    sql = "SELECT ID FROM LIST_A "
    db.cursor.execute(sql)
    db.handle.commit()
    result = db.cursor.fetchall()
    for _ in result:
        print(_[0])

    db.cursor.close()
    db.handle.close()
except Exception as e:
    print("Err occured {}".format(e))
"""

"""
# MyFile 测试代码
#a = file_class.MyFile("./filePackage/test.txt")
a = MyFile("./filePackage/test.txt")
a.printFilePath()
a.testWriteFile()
a.testReadFile()
"""
