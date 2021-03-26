from stock_package import ts_data, sz_web_data, sh_web_data, ex_web_data, ma_kits, psy_kits
from analyzer_package import analyzer
import pandas as pd
from datetime import datetime, date, timedelta
import time
import new_logger as lg
import re
from conf import conf_handler
from filter_package import filter_fix, filter_curve
from report_package import ws_rp
from db_package import db_ops
from itertools import chain


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


# 计时器 装饰器
def wx_timer_ret(func):
    def wrapper(*args, **kwargs):
        wx = lg.get_handle()
        start_time = time.time()
        ret = func(*args, **kwargs)
        time_used = time.time() - start_time
        # print("{} used {} seconds".format(func.__name__, time_used))
        wx.info("{} used {:.2f} seconds".format(func.__name__, time_used))
        return ret
    return wrapper  # 这个语句 不属于 wrapper(), 而是 wx_timer 的返回值. 对应 func 后面这个()调用


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

            sz_basic_info_df = sz_data.basic_info_json_parse(json_str)
            wx.info("[update_sz_basic_info] Total Page:{}---{}\n========================================"
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


"""
# 废弃函数，上海证券交易所网站 URL 已变更，需使用 update_sh_basic_info_2() 函数
# 
"""


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


"""
# 上证主板A股数据
"""


@wx_timer
def update_sh_basic_info_2():
    wx = lg.get_handle()
    sh_data = sh_web_data()
    industry_dict = {'A': u'农、林、牧、渔业', 'B': u'采矿业', 'C': u'制造业',
                     'D': u'电力、热力、燃气及水生产和供应业', 'E': u'建筑业',
                     'F': u'批发和零售业', 'G': u'交通运输、仓储和邮政业',
                     'H': u'住宿和餐饮业', 'I': u'信息传输、软件和信息技术服务业',
                     'J': u'金融业', 'K': u'房地产业', 'L': u'租赁和商务服务业',
                     'M': u'科学研究和技术服务业', 'N': u'水利、环境和公共设施管理业',
                     'O': u'居民服务、修理和其他服务业', 'P': u'教育',
                     'Q': u'卫生和社会工作', 'R': u'文化、体育和娱乐业', 'S': '综合'}

    try:
        # 从Web获取沪市 所有股票的基本信息
        # 从Json读取 股票代码、名称、总股份、流动股份、上市日期
        for key in industry_dict.keys():
            page_counter = 1
            while True:
                sh_basic_list_url = 'http://query.sse.com.cn/security/stock/getStockListData.do?&' \
                                    'jsonCallBack=jsonpCallback99887&isPagination=true&stockCode=&' \
                                    'csrcCode=' + key + '&areaName=&stockType=1&pageHelp.cacheSize=1&' \
                                                        'pageHelp.beginPage=' + str(page_counter) + \
                                    '&pageHelp.pageSize=100&pageHelp.pageNo=' + str(page_counter) + '&_=1556972800071'

                json_str = sh_data.get_json_str(url=sh_basic_list_url, web_flag='sh_basic')
                json_str = '{"content":' + json_str[19:-1] + '}'
                sh_basic_info_df = sh_data.basic_info_json_parse(json_str)

                wx.info("Industry:{}---Total Page:{}---{}\n========================================"
                        .format(industry_dict[key], sh_data.total_page, page_counter))
                sh_basic_info_df['industry'] = industry_dict[key]
                sh_basic_info_df['industry_code'] = key

                sh_data.db_load_into_list_a_2(sh_basic_info_df)

                page_counter += 1
                if page_counter > int(sh_data.total_page[0]):
                    sh_data.total_page.clear()
                    break
                else:
                    continue
    except Exception as e:
        wx.info("[update_sh_basic_info]---{}".format(e))
    finally:
        pass

"""
# 上证科创板数据
"""

@wx_timer
def update_sh_basic_info_kc():
    wx = lg.get_handle()
    sh_data = sh_web_data()
    industry_dict = {'KA': u'新能源',
                     'KB': u'新一代信息技术',
                     'KC': u'其他',
                     'KD': u'生物医药',
                     'KE': u'新材料',
                     'KF': u'高端装备',
                     'KG': u'高端装备'}

    try:
        # 从Web获取沪市 所有股票的基本信息
        # 从Json读取 股票代码、名称、总股份、流动股份、上市日期
        for key in industry_dict.keys():
            page_counter = 1
            while True:
                sh_basic_list_url = 'http://query.sse.com.cn/security/stock/getStockListData.do?&' \
                                    'jsonCallBack=jsonpCallback99887&isPagination=true&stockCode=&' \
                                    'csrcCode=' + sh_data.url_encode(industry_dict[key]) + '&areaName=&stockType=8&pageHelp.cacheSize=1&' \
                                                        'pageHelp.beginPage=' + str(page_counter) + \
                                    '&pageHelp.pageSize=100&pageHelp.pageNo=' + str(page_counter) + '&_=1556972800071'

                json_str = sh_data.get_json_str(url=sh_basic_list_url, web_flag='sh_basic')
                json_str = '{"content":' + json_str[19:-1] + '}'
                sh_basic_info_df = sh_data.basic_info_json_parse(json_str)

                wx.info("Industry:{}---Total Page:{}---{}\n========================================"
                        .format(industry_dict[key], sh_data.total_page, page_counter))
                sh_basic_info_df['industry'] = industry_dict[key]
                sh_basic_info_df['industry_code'] = key

                sh_data.db_load_into_list_a_2(sh_basic_info_df)

                page_counter += 1
                if page_counter > int(sh_data.total_page[0]):
                    sh_data.total_page.clear()
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
def update_sw_industry_into_basic_info(start_from = None, start_code = None):
    wx = lg.get_handle()
    web_data = ex_web_data()
    sw_industry_tuple = web_data.db_get_sw_industry_code(level=2)
    sw_industry_arr = list(chain.from_iterable(sw_industry_tuple))

    if start_code is not None and start_code in sw_industry_arr:
        start_from = sw_industry_arr.index(start_code)
    if start_from is not None:
        sw_industry_arr = sw_industry_arr[start_from-1:]
    code_counter = 1
    for code in sw_industry_arr:
        page_counter = 1
        while True:
            sina_industry_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/" \
                                "Market_Center.getHQNodeData?page=" + str(page_counter) + "&num=80&sort=symbol&asc=1&" \
                                                                                          "node=sw2_" + str(
                code) + "&symbol=&_s_r_a=setlen"
            wx.info("====== SW Industry query Code : {} , Page : {}====".format(code, page_counter))
            stock_id_json = web_data.get_json_str(url=sina_industry_url, web_flag='sh_basic')
            time.sleep(1)

            while stock_id_json is None:
                wx.info("SW Industry [{}:{}]  Code:{} Failed to access --> [{}], retry after 3 seconds".
                        format(code_counter, len(sw_industry_arr), code, sina_industry_url))
                time.sleep(10)
                stock_id_json = web_data.get_json_str(url=sina_industry_url, web_flag='sh_basic')

            if stock_id_json == 'null' or stock_id_json == '[]':
                # wx.info("SW Industry {}:{}  Code:{} Failed to access --> [{}], retry after 3 seconds".
                #         format(code_counter, len(sw_industry_arr), code, sina_industry_url))
                # time.sleep(10)
                # stock_id_json = web_data.get_json_str(url=sina_industry_url, web_flag='sh_basic')
                wx.info("SW Industry [{}:{}] Code:{} Page {} Empty, Move to Next Code ".
                        format(code_counter, len(sw_industry_arr), code,page_counter))
                break
            else:
                wx.info("SW Industry [{}:{}]  Code:{}  Page:{} loaded into basic info table".
                        format(code_counter, len(sw_industry_arr), code, page_counter))

            # key字段 加引号，整理字符串
            stock_id_json = re.sub(r'([a-z|A-Z]+)(?=:)', r'"\1"', stock_id_json)
            stock_id_arr = web_data.sina_industry_json_parse(stock_id_json)
            web_data.db_update_sw_industry_into_basic_info(code=code, id_arr=stock_id_arr)
            page_counter += 1
        code_counter += 1


@wx_timer
def update_daily_data_from_sina(date=None):  # date 把数据更新到指定日期，默认是当天
    wx = lg.get_handle()
    # sz_data = sz_web_data()
    # sh_data = sh_web_data()
    web_data = ex_web_data()
    page_src = (('zxqy', '002%', '中小板'), ('cyb', '30%', '创业板'),
                ('sz_a', '00%', '深证 主板'), ('sh_a', '60%', '上证 主板'))

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
                                                                                           "asc=1&node=" + src[
                                     0] + "&symbol=&_s_r_a=page"

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
                web_data.db_load_into_daily_data(dd_df=daily_data_frame, pre_id=src[1], mode='basic', type='cq')
                web_data.db_load_into_daily_data(dd_df=daily_data_frame, pre_id=src[1], mode='basic', type='qfq')

    except Exception as e:
        wx.info("Err [update_daily_data_from_sina]: {}".format(e))

"""
# 按日期，一次性获得当日所有股票数据，从Tushare
"""
def update_dd_by_date_from_ts(q_date=''):
    wx = lg.get_handle()
    ts = ts_data()
    web_data = ex_web_data()
    name_arr = (('^002', '002%', '中小板'), ('^60', '60%', '上证 主板'), ('^00[0,1,3-9]', '00%', '深证 主板'),
                ('^30', '30%', '创业板'), ('^68', '68%', '科创板'))
    if q_date == '':
        wx.info("[update_dd_by_date_from_ts] 日期为空，退出" )
        return
    try:
        dd_df = ts.acquire_daily_data_by_date(q_date=q_date)
        while dd_df is None:
            wx.info("[update_dd_by_date_from_ts]从Tushare获取 {} 数据失败, 休眠10秒后重试 ...".format(q_date))
            time.sleep(10)
            dd_df = ts.acquire_daily_data_by_date(q_date=q_date)
    except Exception as e:
        wx.info("Err:[update_dd_by_date_from_ts]---{}".format(e))
    dd_df['ts_code'] = dd_df['ts_code'].apply(lambda x: x[0:6])

    # 除权数据 导入数据表
    for name in name_arr:
        # 根据板块拆分 dataframe ，导入数据表
        df_tmp = dd_df[dd_df['ts_code'].str.contains(name[0])]
        # df_00 = dd_df[dd_df['ts_code'].str.contains("^00[0,1,3-9]")]
        # df_002 = dd_df[dd_df['ts_code'].str.contains("^002")]
        # df_60 = dd_df[dd_df['ts_code'].str.contains("^60")]
        # df_68 = dd_df[dd_df['ts_code'].str.contains("^68")]
        web_data.db_load_into_daily_data(dd_df=df_tmp, pre_id=name[1], mode='basic', type='cq')
    wx.info("[update_dd_by_date_from_ts] 除权数据已导入数据表，开始处理 前复权 数据")

    # 前复权数据处理，从tushare 获得 复权因子
    end_datetime_str = (date.today()).strftime('%Y%m%d')
    end_datetime = datetime.strptime(end_datetime_str, '%Y%m%d')
    cur_datetime_str = q_date
    end_factor_df = ts.acquire_factor(date=end_datetime_str)
    while end_factor_df is None:
        wx.info("[update_dd_by_date_from_ts]获取最近日期的 复权因子失败，等待10秒，再次尝试...")
        time.sleep(10)
        end_factor_df = ts.acquire_factor(date=end_datetime_str)

    while end_factor_df.empty:
        end_datetime += timedelta(days=-1)
        end_datetime_str = end_datetime.strftime('%Y%m%d')
        wx.info("[update_dd_by_date_from_ts]获取 最近日期的 复权因子为空，向前一日获取{}".format(end_datetime_str))
        end_factor_df = ts.acquire_factor(date=end_datetime_str)

    cur_factor_df = ts.acquire_factor(date=cur_datetime_str)
    while cur_factor_df is None:
        wx.info("[update_dd_by_date_from_ts]获取{}的 复权因子失败，等待10秒，再次尝试...".format(cur_datetime_str))
        time.sleep(10)
        cur_factor_df = ts.acquire_factor(date=cur_datetime_str)

    # 左链接，合并 cur_factor_df / end_factor_df 两张表
    factor_tmp = pd.merge(cur_factor_df, end_factor_df, on='ts_code', how='left')
    factor_tmp.rename(
        columns={'ts_code': 'id', 'trade_date_x': 'date', 'adj_factor_x': 'cur_factor',
                 'trade_date_y': 'end_date', 'adj_factor_y': 'end_factor'}, inplace=True)

    # cur_factor_df 所有复权因子保留，期末的复权因子为空，则设置0
    factor_tmp.fillna(0, inplace=True)
    # 期末复权因子为空的股票，计入 异常清单
    factor_abnormal_df = pd.DataFrame()
    factor_abnormal_df = factor_abnormal_df.append(factor_tmp[(factor_tmp['end_factor'] == 0)])

    # 删除期末复权因子为空的记录
    factor_tmp = factor_tmp[~(factor_tmp['end_factor'].isin([0]))]

    # 复权因子相除
    factor_tmp['d_factor'] = factor_tmp['cur_factor'] / factor_tmp['end_factor']
    factor_tmp['id'] = factor_tmp['id'].apply(lambda x: x[0:6])

    dd_df.rename(
        columns={'ts_code': 'id', 'trade_date': 'date'}, inplace=True)

    dd_df = pd.merge(dd_df, factor_tmp, on=['id', 'date'], how='left')
    dd_df['open'] *= dd_df['d_factor']
    dd_df['high'] *= dd_df['d_factor']
    dd_df['low'] *= dd_df['d_factor']
    dd_df['close'] *= dd_df['d_factor']
    dd_df.drop(['cur_factor', 'end_date', 'end_factor', 'd_factor'], axis=1, inplace=True)
    dd_df.fillna(0, inplace=True)

    # 前复权数据 导入数据表
    for name in name_arr:
        # 根据板块拆分 dataframe ，导入数据表
        df_tmp = dd_df[dd_df['id'].str.contains(name[0])]
        # df_00 = dd_df[dd_df['ts_code'].str.contains("^00[0,1,3-9]")]
        # df_002 = dd_df[dd_df['ts_code'].str.contains("^002")]
        # df_60 = dd_df[dd_df['ts_code'].str.contains("^60")]
        # df_68 = dd_df[dd_df['ts_code'].str.contains("^68")]
        web_data.db_load_into_daily_data(dd_df=df_tmp, pre_id=name[1], mode='basic', type='qfq')
    wx.info("[update_dd_by_date_from_ts] 前复权数据已导入数据表，开始处理 异常 数据")


"""
# 按股票代码，逐个获取数据从Tushare
"""
@wx_timer
def update_dd_by_id_from_ts(period=-1, type='cq'):
    wx = lg.get_handle()
    ts = ts_data()
    web_data = ex_web_data()

    # sz_data = sz_web_data()
    # sh_data = sh_web_data()
    name_arr = (('002%', '.SZ', '中小板'), ('60%', '.SH', '上证 主板'), ('00%', '.SZ', '深证 主板'),
                ('30%', '.SZ', '创业板'),('68%', '.SH', '科创板'))
    try:
        for name in name_arr:
            id_array = web_data.db_fetch_stock_id(pre_id=name[0])
            counter = 1
            for id in id_array:
                ts_code = id[0] + name[1]
                dd_df = ts.acquire_daily_data(code=ts_code, period=period, type=type)
                while dd_df is None:
                    wx.info("[update_dd_by_id_from_ts]...Failed {}, sleep 10 sec, retry ...".format(ts_code))
                    time.sleep(10)
                    dd_df = ts.acquire_daily_data(code=ts_code, period=period, type=type)
                dd_df['ts_code'] = id[0]
                web_data.db_load_into_daily_data(dd_df=dd_df, pre_id=name[0], mode='basic', type='cq')
                web_data.db_load_into_daily_data(dd_df=dd_df, pre_id=name[0], mode='basic', type='qfq')
                wx.info("[update_dd_by_id_from_ts] {}/{} completed".format(counter, len(id_array)))
                counter += 1
    except Exception as e:
        wx.info("Err:[update_dd_by_id_from_ts]---{}".format(e))
    finally:
        pass


# @wx_timer  用计时器 无法return 数据出来
# start = 0 表示从今天开始， -1 表示从昨天开始（补数据）
@wx_timer_ret
def update_last_day_qfq_data_from_ts(start=0):
    wx = lg.get_handle()
    ts = ts_data()
    web_data = ex_web_data()
    for d1 in range(start, -30, -1):
        today = (date.today() + timedelta(days=d1)).strftime('%Y%m%d')
        df1 = ts.acquire_factor(date=today)
        if df1.empty == False:
            break

    for d2 in range(d1 - 1, -30, -1):
        last_day = (date.today() + timedelta(days=d2)).strftime('%Y%m%d')
        df2 = ts.acquire_factor(date=last_day)
        if df2.empty == False:
            break

    df3 = pd.merge(df1, df2, how='outer', on='ts_code', copy=False)
    df3.fillna(0, inplace=True)
    df3['adj_factor'] = df3['adj_factor_x'] - df3['adj_factor_y']

    qfq_id_df = df3.loc[df3["adj_factor"] != 0, ].copy()
    qfq_id_df = qfq_id_df.loc[qfq_id_df["adj_factor_x"] != 0, ].copy()
    qfq_id_df = qfq_id_df.loc[qfq_id_df["adj_factor_y"] != 0, ].copy()
    qfq_id_df.sort_values(by='ts_code', ascending=True, inplace=True)

    # 获得今日 需要前复权 的股票代码
    qfq_id_arr = qfq_id_df['ts_code'].values
    qfq_df_00 = pd.DataFrame()
    qfq_df_002 = pd.DataFrame()
    qfq_df_30 = pd.DataFrame()
    qfq_df_60 = pd.DataFrame()
    qfq_df_68 = pd.DataFrame()
    i_counter = 1
    for ts_code in qfq_id_arr:
        wx.info("[update_last_day_qfq_data_from_ts] Acquiring qfq data {}/{} from tushare".format(i_counter,
                                                                                                  len(qfq_id_arr)))
        qfq_dd_df = ts.acquire_daily_data(code=ts_code, period=-240, type='qfq')
        while qfq_dd_df is None:
            wx.info(
                "[update_last_day_qfq_data_from_ts] Failed to acquire qfq data {} from Tushare, retry in 10 seconds...".format(
                    ts_code))
            time.sleep(10)
            qfq_dd_df = ts.acquire_daily_data(code=ts_code, period=-240, type='qfq')
        i_counter += 1
        if re.match('002', ts_code) is not None:
            qfq_df_002 = qfq_df_002.append(qfq_dd_df, ignore_index=True)
        elif re.match('00', ts_code) is not None:
            qfq_df_00 = qfq_df_00.append(qfq_dd_df, ignore_index=True)
        elif re.match('30', ts_code) is not None:
            qfq_df_30 = qfq_df_30.append(qfq_dd_df, ignore_index=True)
        elif re.match('60', ts_code) is not None:
            qfq_df_60 = qfq_df_60.append(qfq_dd_df, ignore_index=True)
        elif re.match('68', ts_code) is not None:
            qfq_df_68 = qfq_df_68.append(qfq_dd_df, ignore_index=True)
        else:
            wx.info("[update_daily_qfq_data_from_ts] stock_type does NOT match ('002','00','30','60','68')")

    if qfq_df_002.empty == False:
        qfq_df_002['ts_code'] = qfq_df_002['ts_code'].apply(lambda x: x[0:6])
        qfq_df_002.rename(columns={'ts_code': 'id', 'trade_date': 'date', 'change': 'chg'}, inplace=True)
        web_data.db_load_into_daily_data(dd_df=qfq_df_002, pre_id='002%', mode='basic', type='qfq')
    if qfq_df_00.empty == False:
        qfq_df_00['ts_code'] = qfq_df_00['ts_code'].map(lambda x: x[0:6])
        qfq_df_00.rename(columns={'ts_code': 'id', 'trade_date': 'date', 'change': 'chg'}, inplace=True)
        web_data.db_load_into_daily_data(dd_df=qfq_df_00, pre_id='00%', mode='basic', type='qfq')
    if qfq_df_30.empty == False:
        qfq_df_30['ts_code'] = qfq_df_30['ts_code'].map(lambda x: x[0:6])
        qfq_df_30.rename(columns={'ts_code': 'id', 'trade_date': 'date', 'change': 'chg'}, inplace=True)
        web_data.db_load_into_daily_data(dd_df=qfq_df_30, pre_id='30%', mode='basic', type='qfq')
    if qfq_df_60.empty == False:
        qfq_df_60['ts_code'] = qfq_df_60['ts_code'].map(lambda x: x[0:6])
        qfq_df_60.rename(columns={'ts_code': 'id', 'trade_date': 'date', 'change': 'chg'}, inplace=True)
        web_data.db_load_into_daily_data(dd_df=qfq_df_60, pre_id='60%', mode='basic', type='qfq')
    if qfq_df_68.empty == False:
        qfq_df_68['ts_code'] = qfq_df_68['ts_code'].map(lambda x: x[0:6])
        qfq_df_68.rename(columns={'ts_code': 'id', 'trade_date': 'date', 'change': 'chg'}, inplace=True)
        web_data.db_load_into_daily_data(dd_df=qfq_df_68, pre_id='68%', mode='basic', type='qfq')
    return qfq_id_arr


"""

科创板
http://38.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112409671694568323113_1563855120231&pn=1&pz=50&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f12&fs=m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1563855120236
"""
"""
# 从东财新接口获得 68 的日交易数据
"""


@wx_timer
def update_daily_data_from_eastmoney_2(date=None, supplement=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    page_src = (('60%', 'm:1+t:2', '上证 主板'), ('00%', 'm:0+t:6', '深证 主板'),('002%', 'm:0+t:13', '中小板'),
                ('30%', 'm:0+t:80', '创业板'), ('68%', 'm:1+t:23', '科创板'))

    try:
        for src in page_src:
            # page_count = 1
            # items_page = 500
            # loop_page = True
            # while loop_page:

            # np=2 设置后，一次性取出该版块的所有数据
            east_daily_url = "http://14.push2.eastmoney.com/api/qt/clist/get?" \
                             "cb=jQuery112401309587827468357_1605972394764&pn=1&pz=5000&po=1&np=2&" \
                             "ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&" \
                             "fs="+src[1]+"&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20," \
                                    "f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1605972394765"

            east_daily_str = web_data.get_json_str(url=east_daily_url, web_flag='eastmoney')
            east_daily_str = re.search(r'(?:jQuery\d+\_\d+\()(.*)', east_daily_str).group(1)[:-2]
            dd_cq_df = web_data.east_dd_data_json_parse(date = date , json_str=east_daily_str)
            dd_qfq_df = dd_cq_df.loc[:, ['id', 'date', 'open', 'high', 'low', 'close', 'pre_close',
                                        'chg', 'pct_chg', 'vol', 'amount']]
            web_data.db_load_into_daily_data(dd_df=dd_cq_df, pre_id=src[0], mode='full', type='cq')
            web_data.db_load_into_daily_data(dd_df=dd_qfq_df, pre_id=src[0], mode='basic', type='qfq')


    except Exception as e:
        wx.info("Err [update_daily_data_from_eastmoney]: {}".format(e))

"""
                # 把字符串 拆分成 交易数据",,,,,",",,,,,",",,,,",",,,,,"  和 记录数量 两个部分
                east_daily_str = 
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

                    page_db_df_qfq = page_db_df.loc[:, ['id', 'date', 'open', 'high', 'low', 'close', 'pre_close',
                                                        'chg', 'pct_chg', 'vol', 'amount']]
                    web_data.db_load_into_daily_data(dd_df=page_db_df, pre_id=src[1], mode='full', type='cq')
                    web_data.db_load_into_daily_data(dd_df=page_db_df_qfq, pre_id=src[1], mode='basic', type='qfq')
"""

"""
# 从东财老接口获得 68/60/30/00/002 的日交易数据
"""
@wx_timer
def update_daily_data_from_eastmoney(date=None, supplement=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    page_src = (('C.23', '68%', '科创板'), ('C.13', '002%', '中小板'),
                ('C.2', '60%', '上证 主板'), ('C._SZAME', '00%', '深证 主板'),
                ('C.80', '30%', '创业板'))

    if date is None:
        date = datetime.now().strftime('%Y%m%d')

    try:
        for src in page_src:
            page_count = 1
            items_page = 500
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

                    page_db_df_qfq = page_db_df.loc[:, ['id', 'date', 'open', 'high', 'low', 'close', 'pre_close',
                                                        'chg', 'pct_chg', 'vol', 'amount']]
                    web_data.db_load_into_daily_data(dd_df=page_db_df, pre_id=src[1], mode='full', type='cq')
                    web_data.db_load_into_daily_data(dd_df=page_db_df_qfq, pre_id=src[1], mode='basic', type='qfq')

    except Exception as e:
        wx.info("Err [update_daily_data_from_eastmoney]: {}".format(e))


@wx_timer
def update_dgj_trading_data_from_eastmoney(force=False):
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
        # 保持 dgj 历史数据
        # 删除过期数据，超过 550天 的数据
        del_rows = web_data.dgj_remove_expired_data()
        wx.info("[update_dgj_trading_data] {} Rows of Expired data Removed ".format(del_rows))

        # 删除最近一天的数据,因为最近一天的数据可能还有新增，把开始时间设为 前一天
        start_date = web_data.get_start_date(src='dgj')
        if start_date is None:
            wx.info("[update_dgj_trading_data] Checking lastest date is None !!!")
            return -1
        else:
            wx.info("[update_dgj_trading_data] lastest date is {}".format(start_date))

    start_timestamp = time.mktime(time.strptime(start_date, '%Y-%m-%d'))

    page_counter = 1
    loop_page = True
    icounter = 0
    while loop_page:
        dgj_eastmoney_url = "http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=GG&sty=GGMX&" \
                            "p=" + str(page_counter) + "&ps=100&js=var%20pfXDviDd={pages:(pc),data:[(x)]}&rt=51663059"
        dgj_str = web_data.get_json_str(url=dgj_eastmoney_url, web_flag='eastmoney')
        dgj_str = re.sub(r'.*(?={pages)', r'', dgj_str)  # 去掉 {pages 之前的字符串

        # 把字符串 拆分成 total_page 数字 和 交易记录 两部分
        dgj_str = re.search(r'(?:{pages:)(\d+)(?:\D+\:\[)(.*)(?:\]\})', dgj_str)

        total_page = int(dgj_str.group(1))  # 获得总页数
        dgj_trading_str = dgj_str.group(2)  # 获得交易数据

        dgj_trading_data = re.findall(r'(?:\")(.*?)(?:\")', dgj_trading_str)
        page_arr = list()
        # dd_counter = 1
        trade_date = ''
        for data_str in dgj_trading_data:
            data_arr = data_str.split(',')
            trade_timestamp = time.mktime(time.strptime(data_arr[5], '%Y-%m-%d'))
            # if trade_date == data_arr[5]:
            #     dd_counter += 1
            # else:
            #     dd_counter =1
            trade_date = data_arr[5]
            # wx.info("{}:{}:{}".format(trade_date, dd_counter, data_arr[2]))

            data_arr[5] = re.sub(r'-', '', data_arr[5])  # 日期格式调整，去掉 -
            # wx.info("name:{} / {} / {}".format(data_arr[1],data_arr[3],data_arr[12],data_arr[14] ))
            data_arr[1] = data_arr[1][:20]  # dgj_name
            data_arr[3] = data_arr[3][:20]  # trader_name
            data_arr[12] = data_arr[12][:20]  # trading_type
            data_arr[14] = data_arr[14][:40]  # dgj_pos

            if trade_timestamp - start_timestamp > 0:
                page_arr.append(data_arr)
            else:
                loop_page = False
                break

        if len(page_arr) > 0:
            page_full_df = pd.DataFrame(page_arr,
                                        columns=['pct_chg', 'dgj_name', 'id', 'trader_name', 'unknown2', 'date',
                                                 'vol', 'in_hands', 'price', 's_name', 'relation', 's_abb_name',
                                                 'trading_type', 'amount', 'dgj_pos', 'unknown3'])
            page_db_df = page_full_df.loc[:, ['date', 'id', 'dgj_name', 'dgj_pos', 'trader_name', 'relation', 'vol',
                                              'price', 'amount', 'pct_chg', 'trading_type', 'in_hands']]

            web_data.db_load_into_dgj_trade(dd_df=page_db_df)
            icounter += len(page_db_df)
            wx.info("[update_dgj_trading_data] page {} / {}, date {} : {}".format(page_counter, total_page, trade_date,
                                                                                  start_date))
        else:
            wx.info("[update_dgj_trading_data] page {} without updating data/ {}, date {} : {}".format(page_counter,
                                                                                                       total_page,
                                                                                                       trade_date,
                                                                                                       start_date))
        page_counter += 1
        if page_counter > total_page:
            loop_page = False
    wx.info("[update_dgj_trading_data] 董高监交易数据更新 {} 条".format(icounter))

@wx_timer
def update_whole_sales_data_from_eastmoney(force=False):
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

        # 保持 ws表的数据，从最新日期 到 今天 ，获取web最新数据
        # 在 whole_sales_start_date() 已将 start_date 的数据全部删除，以防 start_date数据不完整，因此从start_date开始重新下载
        start_date = web_data.whole_sales_start_date()
        if start_date is None:
            wx.info("[update_whole_sales_data] Checking lastest date None")
            return -1

    ws_df = pd.DataFrame()
    while True:
        ws_eastmoney_url = "http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=DZJYXQ&" \
                           "token=70f12f2f4f091e459a279469fe49eca5&cmd=&st=TDATE&sr=-1&" \
                           "p=" + str(page_counter) + "&ps=100&js=var%20doXCfrVg=%7Bpages:(tp),data:(x)%7D&" \
                                                      "filter=(Stype='EQA')(TDATE%3E=%5E" + start_date + \
                           "%5E%20and%20TDATE%3C=%5E" + today + "%5E)&rt=51576724"
        # wx.info(ws_eastmoney_url)
        daily_str = web_data.get_json_str(url=ws_eastmoney_url, web_flag='eastmoney')
        daily_str = re.sub(r'.*(?={pages)', r'', daily_str)  # 去掉 {pages 之前的字符串
        daily_str = re.sub(r'(pages)(.*)(data)', r'"\1"\2"\3"', daily_str)  # 给 pages data 加引号，变为合法的 json 串

        ws_tmp = web_data.east_ws_json_parse(daily_str)  # 组装 Dataframe，准备导入数据库

        if ws_tmp is None:
            wx.info("[update_whole_sales_data] Total Page {}, updated data NULL".format(web_data.page_count))
            web_data.db_load_into_ws(ws_df=ws_df)
            break
        else:
            if ws_df is None or len(ws_df) == 0:
                ws_df = ws_tmp.copy()
            else:
                ws_df = ws_df.append(ws_tmp, sort=False)
            wx.info("[Eastmoney_ws_data]Total Page:{}---{},record {}， Start date: {}\n========================================"
                    .format(web_data.page_count, page_counter, len(ws_df), start_date))
            if len(ws_df) > 10000:
                web_data.db_load_into_ws(ws_df=ws_df)
                wx.info("[update_whole_sales_data] 大宗交易 导入数据记录 {} 条".format(len(ws_df)))
                ws_df.drop(ws_df.index, inplace=True)

        if page_counter >= web_data.page_count:
            wx.info("Page : {} is the final page , End ".format(page_counter))
            web_data.db_load_into_ws(ws_df=ws_df)
            wx.info("[update_whole_sales_data] 大宗交易 导入数据记录 {} 条".format(len(ws_df)))
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


"""
# 更新Indicator MA 表 移动均值, 批量更新，减少数据库访问
"""


@wx_timer
def update_ind_ma_df(fresh=False, data_src='cq', bt_start_date='', bt_end_date=''):
    wx = lg.get_handle()
    web_data = ex_web_data()
    pre_id = ['68%', '002%', '30%', '00%', '60%']

    if data_src=='bt_qfq':
        if bt_end_date == '' or  bt_start_date == '':
            wx.info("[update_ind_ma_df] bt_qfq mode:  bt_start_date and bt_end_date Can't Empty, Err ")
            return
    ma = ma_kits()
    for pre in pre_id:
        # id_arr = web_data.db_fetch_stock_id(pre_id=pre)
        # if len(id_arr)>0:
        ma_ret = ma.calc_arr(pre_id=pre, fresh=fresh, data_src=data_src, bt_start_date=bt_start_date, bt_end_date=bt_end_date)
        if ma_ret is None or ma_ret.empty:
            wx.info("[update_ind_ma_df]=========== {} {} MA =========== Data Empty ".format(pre, data_src))
        else:
            wx.info("[update_ind_ma_df]=========== {} {} MA =========== Loading Data".format(pre, data_src))
            web_data.db_load_into_ind_xxx(ind_type='ma', ind_df=ma_ret, stock_type=pre, data_src=data_src)
            wx.info("[update_ind_ma_df]=========== {} {} MA =========== Data Loaded ALL ".format(pre, data_src))
        # else:
        #     wx.info("[update_ind_ma_df]=========== {} {} Stock =========== Empty [{}]".format(pre, data_src, len(id_arr)))


@wx_timer
def update_ind_ma_single(id_arr=None, data_src='qfq'):
    wx = lg.get_handle()
    if id_arr is None:
        wx.info("[update_ind_ma_single] id_arr is Empty, Err & Return ...")
        return
    web_data = ex_web_data()
    ma = ma_kits()
    ma_df_00 = pd.DataFrame()
    ma_df_002 = pd.DataFrame()
    ma_df_30 = pd.DataFrame()
    ma_df_60 = pd.DataFrame()
    ma_df_68 = pd.DataFrame()

    icount = 1
    for id in id_arr:
        ma_ret = ma.calc(stock_id=id[0:6], data_src=data_src)

        if re.match('002', id) is not None:
            ma_df_002 = ma_df_002.append(ma_ret, ignore_index=True)
        elif re.match('00', id) is not None:
            ma_df_00 = ma_df_00.append(ma_ret, ignore_index=True)
        elif re.match('30', id) is not None:
            ma_df_30 = ma_df_30.append(ma_ret, ignore_index=True)
        elif re.match('60', id) is not None:
            ma_df_60 = ma_df_60.append(ma_ret, ignore_index=True)
        elif re.match('68', id) is not None:
            ma_df_68 = ma_df_68.append(ma_ret, ignore_index=True)
        else:
            wx.info("[update_ind_ma_single] Stock ID does NOT match ('002','00','30','60','68')")
        wx.info("[update_ind_ma_single] {}/{} Stock {} MA data Calculated ".format(icount, len(id_arr), id))
        icount +=1

    ma_dict={'002':ma_df_002 , '00':ma_df_00, '30':ma_df_30, '60':ma_df_60, '68':ma_df_68}
    for key in ma_dict.keys():
        if ma_dict[key].empty == False:
            web_data.db_load_into_ind_xxx(ind_type='ma', ind_df=ma_dict[key], stock_type=key, data_src=data_src)
            wx.info("[update_ind_ma_single]=========== {} MA =========== Record Loaded  [{}] ".format(key, len(ma_dict[key])))
        else:
            wx.info("[update_ind_ma_single]=========== {} MA =========== Record Empty [{}]".format(key, len(ma_dict[key])))


"""
# 更新Indicator PSY 表 ，批量更新，减少数据库访问次数
"""


@wx_timer
def update_ind_psy_2(fresh=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    pre_id = ['00%', '002%', '30%', '60%']
    psy = psy_kits()
    # np_cprice = psy.get_cprice(stock_id="600000")
    # psy.calc(np_cprice)
    for pre in pre_id:
        id_arr = web_data.db_fetch_stock_id(pre_id=pre)
        df_psy = psy.calc_arr(stock_arr=id_arr, fresh=True)

        # 全部刷新，每股的数据量较大，每股更新数据库
        if fresh == True:
            icount = 1
            for id in id_arr:
                np_cprice = psy.get_cprice(stock_id=id[0])
                df_psy = psy.calc(np_cprice, fresh=True)
                if df_psy is not None:
                    web_data.db_load_into_ind_xxx(ind_type='psy', ind_df=df_psy, stock_type=pre)
                    wx.info("[update_ind_psy] {} PSY data loaded ALL [{}/{}]".format(id[0], icount, len(id_arr)))
                else:
                    wx.info("[update_ind_psy] {} PSY data Empty, go next... [{}/{}]".format(id[0], icount, len(id_arr)))
                icount += 1
        # 增量更新，一个板块 拼凑一个Dataframe ,统一更新数据库
        else:
            icount = 1
            for id in id_arr:
                np_cprice = psy.get_cprice(stock_id=id[0])
                df_psy = psy.calc(np_cprice, fresh=False)
                wx.info("[update_ind_psy] {} PSY data appended [{}/{}]".format(id[0], icount, len(id_arr)))
                icount += 1

            web_data.db_load_into_ind_xxx(ind_type='psy', ind_df=df_psy, stock_type=pre)
            wx.info("[update_ind_psy] ============={} PSY loaded ALL============".format(pre))


"""
# 更新Indicator PSY 表 移动均值
"""


@wx_timer
def update_ind_psy(fresh=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    pre_id = ['00%', '002%', '30%', '60%']
    psy = psy_kits()
    # np_cprice = psy.get_cprice(stock_id="600000")
    # psy.calc(np_cprice)
    for pre in pre_id:
        id_arr = web_data.db_fetch_stock_id(pre_id=pre)
        # 全部刷新，每股的数据量较大，每股更新数据库
        if fresh == True:
            icount = 1
            for id in id_arr:
                np_cprice = psy.get_cprice(stock_id=id[0])
                df_psy = psy.calc(np_cprice, fresh=True)
                if df_psy is not None:
                    web_data.db_load_into_ind_xxx(ind_type='psy', ind_df=df_psy, stock_type=pre)
                    wx.info("[update_ind_psy] {} PSY data loaded ALL [{}/{}]".format(id[0], icount, len(id_arr)))
                else:
                    wx.info("[update_ind_psy] {} PSY data Empty, go next... [{}/{}]".format(id[0], icount, len(id_arr)))
                icount += 1
        # 增量更新，一个板块 拼凑一个Dataframe ,统一更新数据库
        else:
            icount = 1
            for id in id_arr:
                np_cprice = psy.get_cprice(stock_id=id[0])
                df_psy = psy.calc(np_cprice, fresh=False)
                wx.info("[update_ind_psy] {} PSY data appended [{}/{}]".format(id[0], icount, len(id_arr)))
                icount += 1

            web_data.db_load_into_ind_xxx(ind_type='psy', ind_df=df_psy, stock_type=pre)
            wx.info("[update_ind_psy] ============={} PSY loaded ALL============".format(pre))


"""
# 使用全部刷新的方式，因为个股的回购状态更新，逐项比对更麻烦
# 东财的接口更新,此函数废弃
"""

@wx_timer
def update_repo_data_from_eastmoney():
    wx = lg.get_handle()
    web_data = ex_web_data()
    page_counter = 1
    start_date = (date.today() + timedelta(days=-550)).strftime('%Y-%m-%d')
    del_rows = web_data.repo_remove_data()
    wx.info("[update_repo_data] Force to refresh Repurchase data {} rows REMOVED, ".format(del_rows))

    loop_flag = True
    icounter = 0
    while loop_flag:
        east_money_repo_url = "http://api.dataide.eastmoney.com/data/gethglist?pageindex=" + str(page_counter) + \
                              "&pagesize=200&orderby=upd&order=desc&jsonp_callback=var%20vehXbliK=(x)&" \
                              "market=(0,1,2,3)&rt=51676827"
        repo_str = web_data.get_json_str(url=east_money_repo_url, web_flag='eastmoney')
        trunc_pos = repo_str.find('{"code":')
        repo_str = repo_str[trunc_pos:]
        df_repo = web_data.east_repo_json_parse(repo_str)
        wx.info("[update_repo_data_from_eastmoney] Page {} / {} loaded，includeing {} rows".format(page_counter,
                                                                                                  web_data.total_pages,
                                                                                                  len(df_repo)))

        web_data.db_load_into_repo(df_repo=df_repo, t_name='repo_201901')
        icounter += len(df_repo)
        page_counter += 1
        if page_counter > web_data.total_pages:
            loop_flag = False
            wx.info("[update_repo_data_from_eastmoney] Page {} is final , exit ".format(web_data.total_pages))
    wx.info("[update_repo_data_from_eastmoney] 回购数据更新 {} 条".format(icounter))


"""
# 使用全部刷新的方式，因为个股的回购状态更新，逐项比对更麻烦
# 使用东财新接口,目前正在使用此函数
"""

@wx_timer
def update_repo_data_from_eastmoney_2():
    wx = lg.get_handle()
    web_data = ex_web_data()
    page_counter = 1
    # start_date = (date.today() + timedelta(days=-550)).strftime('%Y-%m-%d')
    del_rows = web_data.repo_remove_data()
    wx.info("[update_repo_data] Force to refresh Repurchase data {} rows REMOVED, ".format(del_rows))

    loop_flag = True
    icounter = 0
    while loop_flag:
        # east_money_repo_url = "http://datacenter.eastmoney.com/api/data/get?type=RPTA_WEB_GETHGLIST&" \
        #                       "sty=ALL&source=WEB&p="+ str(page_counter) +"&ps=100&st=upd&sr=-1&" \
        #                                                                   "var=sWGWYDPB&rt=53034000"

        east_money_repo_url = "http://datacenter.eastmoney.com/api/data/get?callback=" \
                              "jQuery11230633586381441708_1606142536395&st=dim_date&sr=-1&ps=" \
                              "200&p=" + str(page_counter)+"&type=RPTA_WEB_GETHGLIST&sty=ALL&source=WEB"

        repo_str = web_data.get_json_str(url=east_money_repo_url, web_flag='eastmoney')
        trunc_pos = repo_str.find('{"version":')
        repo_str = repo_str[trunc_pos:-2]
        df_repo = web_data.east_repo_json_parse(repo_str)
        wx.info("[update_repo_data_from_eastmoney] Page {} / {} loaded，includeing {} rows".format(page_counter,
                                                                                                  web_data.total_pages,
                                                                                                  len(df_repo)))

        web_data.db_load_into_repo(df_repo=df_repo, t_name='repo_201901')
        icounter += len(df_repo)
        page_counter += 1
        if page_counter > web_data.total_pages:
            loop_flag = False
            wx.info("[update_repo_data_from_eastmoney] Page {} is final , exit ".format(web_data.total_pages))
    wx.info("[update_repo_data_from_eastmoney] 回购数据更新 {} 条".format(icounter))



@wx_timer
def report_total_amount():
    rp = ws_rp()
    rp.calc_total_amount()


@wx_timer
def report_days_vol(rp=None, days=0):
    if rp is not None:
        df_days_vol = rp.calc_days_vol(days)
        rp.output_table(dd_df=df_days_vol, sheet_name='vol_' + str(days) + '_days',
                        filename='volumne_' + str(days) + 'days', type='.xlsx', index=False)
    else:
        return -1


@wx_timer
def report_ws_price(rp=None, days=180):
    if rp is not None:
        df_price_comparision = rp.ws_price_compare_close_price(days=days, close_date=None)
        rp.output_table(dd_df=df_price_comparision, sheet_name='大宗交易_' + str(days) + '天统计表',
                        filename='大宗交易_' + str(days) + '天统计表', type='.xlsx', index=False)
    else:
        return -1


@wx_timer
def report_dgj_trading(rp=None, limit=100):
    if rp is not None:
        df_dgj = rp.db.select_table(t_name='dgj_201901', where="", order="", limit=100)
        df_dgj.rename(
            columns={'date': '日期', 'id': '证券代码', 'dgj_name': '高管姓名', 'dgj_pos': '职位',
                     'trader_name': '交易人', 'relation': '与高管关系', 'vol': '成交量(股)',
                     'price': '成交价', 'amount': '成交金额', 'pct_chg': '变动比例(%)',
                     'trading_type': '交易方式', 'in_hand': '仍持股量'}, inplace=True)
        rp.output_table(dd_df=df_dgj, sheet_name='企业高管交易数据',
                        filename='企业高管交易数据', type='.xlsx', index=False)
    else:
        return -1


@wx_timer
def report_repo_completion_data(rp=None):
    if rp is not None:
        sql = "select rp.id as `证券代码`, rp.name as `名称`, c002.close as `收盘价02-19`, " \
              "rp.buy_in_price_high as `最高回购价`, rp.buy_in_price_low as `最低回购价`, " \
              "rp.buy_in_vol as `回购数量`, rp.buy_in_amount as `回购金额`, " \
              "rp.end_date as `回购结束日期` from repo_201901 as rp " \
              "join code_002_201901 as c002 on c002.date=20190219 and c002.id =  rp.id  " \
              "where progress in (006) and end_date > 20180101 " \
              "order by notice_date desc"
        df_repo = rp.db._exec_sql(sql=sql)
        rp.output_table(dd_df=df_repo, sheet_name='回购股票', filename='回购股票', type='.xlsx', index=False)
    else:
        return -1

"""
# 分析热点板块
"""

@wx_timer
def analysis_hot_industry(duration = 5, level=1):
    ana = analyzer()
    ana.ana_hot_industry(duration = duration, level = level)


@wx_timer
def analysis_dgj():
    ana = analyzer()
    # ana_para = {'dgj_year':[-365,'5000000'], 'dgj_6mon':[-180,'3000000'], 'dgj_3mon':[-90,'1000000'],
    #               'dgj_1mon':[-30,'500000'], 'dgj_2wk':[-14,'100000']}

    ana_matrix = dict(ana.h_conf.rd_sec(sec='ass_dgj_buy'))
    for key in ana_matrix.keys():
        # ana_para[key].append(ana_matrix[key])
        ana_para = ana_matrix[key].split("#")
        start_date = (date.today() + timedelta(days=int(ana_para[1]))).strftime('%Y%m%d')
        ana.ana_dgj_trading(ass_type=key, start_date=start_date, vol=ana_para[2], ass_weight=ana_para[0])

    ana_matrix = dict(ana.h_conf.rd_sec(sec='ass_dgj_sell'))
    for key in ana_matrix.keys():
        # ana_para[key].append(ana_matrix[key])
        ana_para = ana_matrix[key].split("#")
        start_date = (date.today() + timedelta(days=int(ana_para[1]))).strftime('%Y%m%d')
        ana.ana_dgj_trading(ass_type=key, start_date=start_date, vol=ana_para[2], ass_weight=ana_para[0])


@wx_timer
def analysis_repo():
    ana = analyzer()
    # ana_para = {'repo_b_10m_30m' : ['between 10000000 and 30000000'],
    #             'repo_b_30m_50m' : ['between 30000000 and 50000000'],
    #             'repo_b_50m_70m' : ['between 50000000 and 70000000'],
    #             'repo_b_70m_90m' : ['between 70000000 and 90000000'],
    #             'repo_b_90m_110m': ['between 90000000 and 110000000'],
    #             'repo_b_110m_140m' : ['between 110000000 and 140000000'],
    #             'repo_b_140m_170m' : ['between 140000000 and 170000000'],
    #             'repo_b_170m_200m' : ['between 170000000 and 200000000'],
    #             'repo_b_200m_mm' : ['>200000000'],
    #             }

    ana_matrix = dict(ana.h_conf.rd_sec(sec='ass_repo_amount'))
    start_date = (date.today() + timedelta(days=-365)).strftime('%Y%m%d')
    for key in ana_matrix.keys():
        # ana_para[key].append(ana_matrix[key])
        ana_para = ana_matrix[key].split("#")
        ana.ana_repo(ass_type=key, start_date=start_date, vol=ana_para[1], ass_weight=ana_para[0])


@wx_timer
def analysis_ws():
    ana = analyzer()
    ana_matrix = dict(ana.h_conf.rd_sec(sec='ass_ws_disc'))
    start_date = (date.today() + timedelta(days=-550)).strftime('%Y%m%d')
    for key in ana_matrix.keys():
        ana_para = ana_matrix[key].split("#")
        ana.ana_ws(ass_type=key, start_date=start_date, ass_weight=ana_para[0], ass_disc=ana_para[1],
                   ass_amount=ana_para[2])

    #     pass


# @wx_timer
def analysis_summary_list(rp=None):
    ana = analyzer()
    summary_arr = ['repo%', 'dgj%', 'disc%']
    df_list = pd.DataFrame()
    for item in summary_arr:
        sql = "select id, name, left('" + item + "', 4) as type , sum(ass_weight) as weight " \
                                                 "from fruit where ass_type like '" + item + "' group by id, name having sum(ass_weight) > 0 " \
                                                                                             "order by sum(ass_weight) desc "
        df_tmp = ana.db._exec_sql(sql)

        if df_list.empty:
            df_list = df_tmp
        else:
            df_list = pd.merge(df_list, df_tmp, how='inner', on=['id', 'name'])

    rp.output_table(dd_df=df_list, sheet_name="综合分析列表",
                    filename='综合分析列表', type='.xlsx', index=False)
    id_arr = df_list['id'].tolist()
    return id_arr


@wx_timer
def analysis_single_stock(rp=None, id_arr=None):
    ana = analyzer()
    start_date = (date.today() + timedelta(days=-180)).strftime('%Y%m%d')
    for s_id in id_arr:
        ret_dict = ana.ana_single_stock(s_id=s_id, start_date=start_date)
        rp.output_docx(filename=ret_dict['title'], para_dict=ret_dict)


# 废弃函数，两个指标 交叉比对，导致不必要的复杂度
# 对每个指标 都单独设置权重，然后将对应股票列表导入 fruit 表

@wx_timer
def report_cross_dgj_ws(rp=None, ws_days=180, dgj_days=180):
    if rp is not None:
        df_price_comparision = rp.ws_price_compare_close_price(days=ws_days, close_date=None)  # close_date 默认是昨天
        df_dgj_summary = rp.dgj_trading_summary(days=dgj_days)
        df_cross = pd.merge(df_price_comparision, df_dgj_summary, how='outer',
                            left_on=['证券代码', '名称'], right_on=['证券代码', '名称'])

        close_price_date = (date.today() + timedelta(days=-1)).strftime('%Y%m%d')

        cols = ['证券代码', '名称', '大宗交易次数', '高管买入次数', '高管卖出次数',
                '大宗成交量（万股）', '高管买入（万股）', '高管卖出（万股）',
                '收盘价(' + close_price_date + ')', '大宗买入均价', '高管买入均价', '高管卖出均价',
                '大宗最高买价', '大宗最低买价', '高管最高买价', '高管最低买价',
                '高管最高卖价', '高管最低卖价']
        df_cross = df_cross[cols]
        rp.output_table(dd_df=df_cross, sheet_name="大宗" + str(ws_days) + "天-董监高" + str(dgj_days) + "天-收盘价",
                        filename='大宗-董高监交易-最新收盘价比对', type='.xlsx', index=False)
    else:
        return -1


@wx_timer_ret
def filter_A(data_src='qfq', f_start_date='', f_end_date=''):
    wx = lg.get_handle()
    filter_a = filter_fix(f_conf='filter_rules\\filter_001.conf', f_start_date=f_start_date, f_end_date=f_end_date, data_src=data_src)
    filter_b = filter_curve(f_conf='filter_rules\\filter_002.conf', f_start_date=f_start_date, f_end_date=f_end_date, data_src=data_src)

    target_dict = {}

    # 除权表
    df_pe_grp = filter_a.filter_pe()
    if df_pe_grp is None or df_pe_grp.empty:
        wx.info("[Filter_A][Filter PE] founded 0")
        df_pe_grp = pd.DataFrame()
    else:
        wx.info("[Filter_A][Filter PE] {} founded ".format(len(df_pe_grp)))
        df_pe_grp.rename(columns={'id': '股票代码'}, inplace=True)
    target_dict['PE Group'] = df_pe_grp

    """
    # 前复权表
    df_amount_grp = filter_a.filter_tt_amount()
    if df_amount_grp is None or df_amount_grp.empty:
        wx.info("[Filter_A][Filter Total Amount] founded 0")
        df_amount_grp = pd.DataFrame()
    else:
        wx.info("[Filter_A][Filter Total Amount] {} founded".format(len(df_amount_grp)))
    target_dict['Amount Group'] = df_amount_grp
    """

    # 前复权表
    df_below_ma55_grp = filter_a.filter_below_ma55()
    if df_below_ma55_grp is None or df_below_ma55_grp.empty:
        wx.info("[Filter_A][Filter Below Ma 55] founded 0")
        df_below_ma55_grp = pd.DataFrame()
    else:
        wx.info("[Filter_A][Filter Below Ma 55] {} founded".format(len(df_below_ma55_grp)))
        df_below_ma55_grp.rename(columns={'id': '股票代码', 'close': '最近交易日收盘价',
                                          'ma_5': '5日均值', 'ma_55': '55日均值'}, inplace=True)
    target_dict['Ma55 Group'] = df_below_ma55_grp


    # 前复权表
    df_high_price_grp = filter_a.filter_high_price()
    if df_high_price_grp is None or df_high_price_grp.empty:
        wx.info("[Filter_A][Filter High Price] founded 0")
        df_high_price_grp = pd.DataFrame()
    else:
        wx.info("[Filter_A][Filter High Price] {} founded".format(len(df_high_price_grp)))
        df_high_price_grp.rename(columns={'id': '股票代码', 'high': '最近交易日最高价'}, inplace=True)
    target_dict['High Price Group'] = df_high_price_grp

    df_target = pd.DataFrame()
    for key in target_dict.keys():
        if target_dict[key].empty:
            choose = input("[Filter_A] "+key+" 结果为空，请选择是否继续 Y/N : ")
            if choose.strip() == 'N' or choose.strip() == 'n':
                wx.info("[Filter_A] 筛选结果为空，因为 {} 为空".format(key))
                return None
            else:
                wx.info("[Filter_A] {} 筛选结果为空，跳过此条件继续筛选")
                continue
        elif len(df_target) == 0:
            df_target = target_dict[key]
        else:
            df_target = pd.merge(df_target, target_dict[key])

    # df_target.rename(columns={'id': '股票代码', 'tt_amount': '流通市值', 'close': '最近交易日收盘价',
    #                           'ma_5': '5日均值', 'ma_55': '55日均值', 'high': '最近交易日最高价'}, inplace=True)
    wx.info("[Filter_A] [PE + Amount + Ma55 + HighPrice] {} founded".format(len(df_target)))

    reporter = ws_rp()
    reporter.output_table(dd_df=df_target.round(2), sheet_name='PE_MA55_股本_股价筛选结果',
                          filename='选股清单_1_' + data_src, type='.xlsx', index=False)

    # 前复权表
    df_filter_side = filter_b.filter_side()
    reporter.output_table(dd_df=df_filter_side, sheet_name='涨幅筛选结果',
                          filename='选股清单_2_' + data_src, type='.xlsx', index=False)

    df_filter_result = pd.merge(df_filter_side, df_target.round(2))
    reporter.output_table(dd_df=df_filter_result, sheet_name='最终清单',
                          filename='选股清单_' + data_src, type='.xlsx', index=False)
    wx.info("[Filter_Curve Completed] 选股完成，合计： {} ".format(len(df_filter_result)))
    return df_filter_result



@wx_timer_ret
def filter_B(data_src='qfq', f_start_date='', f_end_date=''):
    wx = lg.get_handle()
    filter_a = filter_fix(f_conf='filter_rules\\filter_001.conf', f_start_date=f_start_date, f_end_date=f_end_date, data_src=data_src)
    # filter_b = filter_curve(f_conf='filter_rules\\filter_002.conf', f_start_date=f_start_date, f_end_date=f_end_date, data_src=data_src)

    target_dict = {}

    # 均线向上、 正向排列的股票
    # df_ma_up_grp = filter_a.filter_ma_up()
    # if df_ma_up_grp is None or df_ma_up_grp.empty:
    #     wx.info("[Filter_B][Filter Ma] founded 0")
    #     df_ma_up_grp = pd.DataFrame()
    # else:
    #     wx.info("[Filter_B][Filter Ma] {} founded ".format(len(df_ma_up_grp)))
    # target_dict['Ma UP Group'] = df_ma_up_grp

    # 热点板块的统计
    df_hot_industry = filter_a.filter_dd_pct_top()
    if df_hot_industry is None or df_hot_industry.empty:
        wx.info("[Filter_B][Filter hot industry] founded 0")
        df_hot_industry = pd.DataFrame()
    else:
        wx.info("[Filter_B][Filter hot industry] {} founded ".format(len(df_hot_industry)))

    # 统计有几个热点行业
    hot_industry_counter = df_hot_industry.industry_name.nunique()

    # 统计热点行业个股出现的个数
    df_hot_industry_count = df_hot_industry.groupby(by='industry_name',as_index=False).size()#.sort_values(ascending = False, inpalce = True)

    target_dict['hot industry'] = df_hot_industry
    df_target = pd.DataFrame()
    for key in target_dict.keys():
        if target_dict[key].empty:
            choose = input("[Filter_B] "+key+" 结果为空，请选择是否继续 Y/N : ")
            if choose.strip() == 'N' or choose.strip() == 'n':
                wx.info("[Filter_B] 筛选结果为空，因为 {} 为空".format(key))
                return None
            else:
                wx.info("[Filter_B] {} 筛选结果为空，跳过此条件继续筛选")
                continue
        elif len(df_target) == 0:
            df_target = target_dict[key]
        else:
            df_target = pd.merge(df_target, target_dict[key])

    df_target.rename(columns={'id': '股票代码'}, inplace=True)
    wx.info("[Filter_B] {} founded".format(len(df_target)))

    reporter = ws_rp()
    reporter.output_table(dd_df=df_target.round(2), sheet_name='均线向上筛选结果',
                          filename='选股清单_均线向上' + data_src, type='.xlsx', index=False)

    # 前复权表
    # df_filter_side = filter_b.filter_side()
    # reporter.output_table(dd_df=df_filter_side, sheet_name='涨幅筛选结果',
    #                       filename='选股清单_2_' + data_src, type='.xlsx', index=False)
    #
    # df_filter_result = pd.merge(df_filter_side, df_target.round(2))
    # reporter.output_table(dd_df=df_filter_result, sheet_name='最终清单',
    #                       filename='选股清单_' + data_src, type='.xlsx', index=False)
    wx.info("[Filter_Curve Completed] 选股完成，合计： {} ".format(len(df_target)))
    return df_target

"""
# 更新 dd_hot_industry 表，每日涨势股票及对应板块
"""
@wx_timer
def update_hot_industry(start_date='',end_date=''):
    web_data = ex_web_data()
    wx = lg.get_handle()
    h_conf = conf_handler(conf='stock_analyer.conf')
    daily_cq_t_00 = h_conf.rd_opt('db', 'daily_table_cq_00')
    daily_cq_t_30 = h_conf.rd_opt('db', 'daily_table_cq_30')
    daily_cq_t_60 = h_conf.rd_opt('db', 'daily_table_cq_60')
    daily_cq_t_68 = h_conf.rd_opt('db', 'daily_table_cq_68')
    daily_cq_t_002 = h_conf.rd_opt('db', 'daily_table_cq_002')
    host = h_conf.rd_opt('db', 'host')
    database = h_conf.rd_opt('db', 'database')
    user = h_conf.rd_opt('db', 'user')
    pwd = h_conf.rd_opt('db', 'pwd')
    db = db_ops(host=host, db=database, user=user, pwd=pwd)
    if end_date == '':
        end_date = datetime.now().strftime('%Y%m%d')

    tname_arr = [daily_cq_t_00, daily_cq_t_30, daily_cq_t_60,
                 daily_cq_t_002, daily_cq_t_68]

    df_pct_top_grp = pd.DataFrame()
    for t_name in tname_arr:
        if start_date == '':
            sql = "SELECT dd.id , dd.date ,la.name, la.sw_level_1, sw.industry_name as level_1_name, " \
                  "la.sw_level_2, sw1.industry_name as level_2_name, dd.pct_chg FROM " + t_name + \
                  " as dd left join list_a as la on la.id=dd.id " \
                  " left join sw_industry_code as sw on sw.industry_code =  la.sw_level_1" \
                  " left join sw_industry_code as sw1 on sw1.industry_code =  la.sw_level_2" \
                  " where dd.date =  " + end_date + " and dd.pct_chg >= 6"
        else:
            sql = "SELECT dd.id , dd.date ,la.name, la.sw_level_1, sw.industry_name as level_1_name, " \
                  "la.sw_level_2, sw1.industry_name as level_2_name, dd.pct_chg FROM " + t_name + \
                  " as dd left join list_a as la on la.id=dd.id " \
                  " left join sw_industry_code as sw on sw.industry_code =  la.sw_level_1" \
                  " left join sw_industry_code as sw1 on sw1.industry_code =  la.sw_level_2" \
                  " where dd.date between  "+start_date+" and " + end_date + " and dd.pct_chg >=6 "

        df_tmp = db._exec_sql(sql=sql)
        if df_tmp is None or df_tmp.empty:
            continue
        if df_pct_top_grp.empty:
            df_pct_top_grp = df_tmp.copy()
        else:
            df_pct_top_grp = df_pct_top_grp.append(df_tmp)

    if not df_pct_top_grp.empty or df_pct_top_grp is not None:
        df_pct_top_grp.reset_index(drop=True, inplace=True)
        df_pct_top_grp.replace([None], ['None'], inplace=True)
        web_data.db_load_into_hot_industry(df_hot_industry=df_pct_top_grp)
    else:
        wx.info("[update_hot_industry] {}-{} 时间段内 没有数据".format(start_date, end_date))


"""
# 从Start_date 到 当前日期，根据tushare的交易日历，查验数据库中是否有缺漏的交易日期数据
"""
@wx_timer
def verify_trade_date(start_date=''):
    wx = lg.get_handle()
    ts = ts_data()
    h_conf = conf_handler(conf='stock_analyer.conf')
    daily_cq_t_00 = h_conf.rd_opt('db', 'daily_table_cq_00')
    daily_cq_t_30 = h_conf.rd_opt('db', 'daily_table_cq_30')
    daily_cq_t_60 = h_conf.rd_opt('db', 'daily_table_cq_60')
    daily_cq_t_68 = h_conf.rd_opt('db', 'daily_table_cq_68')
    daily_cq_t_002 = h_conf.rd_opt('db', 'daily_table_cq_002')

    host = h_conf.rd_opt('db', 'host')
    database = h_conf.rd_opt('db', 'database')
    user = h_conf.rd_opt('db', 'user')
    pwd = h_conf.rd_opt('db', 'pwd')
    db = db_ops(host=host, db=database, user=user, pwd=pwd)

    if start_date == '':
        wx.info("[verify_trade_date] 起始日期为空，退出")
        return

    end_date_str = (date.today()).strftime('%Y%m%d')
    # end_date_str = (date.today() + timedelta(days=-1)).strftime('%Y%m%d')
    try:
        trade_date_df = ts.acquire_trade_cal(start_date=start_date, end_date=end_date_str)
        while trade_date_df is None:
            wx.info("[verify_trade_date]从Tushare获取 {}:{} 交易日历数据失败, 休眠10秒后重试 ...".
                    format(start_date, end_date_str))
            time.sleep(10)
            trade_date_df = ts.acquire_trade_cal(start_date=start_date, end_date=end_date_str)
    except Exception as e:
        wx.info("Err:[verify_trade_date]---{}".format(e))

    # 从数据表中检索所有日期的记录
    sql = "select date from "+daily_cq_t_60+" where date >= "+start_date+" group by date "
    db_date_df = db._exec_sql(sql = sql)

    # 转换成 set 类型，计算差集
    db_date_set = set(db_date_df['date'].tolist())
    trade_date_set = set(trade_date_df['date'].tolist())
    diff_set = trade_date_set - db_date_set
    print(diff_set)



"""
# 从东财接口 年季报表数据，东财数据更新不及时，无法增量更新（根据公告日期排序筛选）
# update = 'current' / 'all' 分别代表 只更新最新季度的报表，或所有 report_date_arr 数组中的季度报表 
# supplement = True / False 分别代表 按最终报告日期 进行增量更新 ，或刷新当前季度的所有报表
"""


@wx_timer
def update_fin_report_from_eastmoney(update='current', supplement = True):
    wx = lg.get_handle()
    web_data = ex_web_data()
    report_date_arr = (('2020Q3','2020-09-30','2020三季度'), ('2020Q2','2020-06-30','2020半年报'),
                       ('2020Q1','2020-03-31','2020一季度'), ('2019Q4','2019-12-31','2019四季度'),
                        ('2019Q3','2019-09-30','2019三季度') #,('2020Q4','2020-12-31','2020四季度'),
                        # ('2019Q2','2019-06-30','2019半年报'), ('2019Q1','2019-03-31','2019一季报'),
                        # ('2018Q4','2018-12-31','2018年报'),('2018Q3','2018-09-30','2018三季度'),
                        # ('2018Q2','2018-06-30','2018半年报'),('2018Q1','2018-03-31','2018一季度'),
                        # ('2017Q4','2017-12-31','2017年报'),('2017Q3', '2017-09-30', '2017三季度'),
                        # ('2017Q2', '2017-06-30', '2017半年报') ,('2017Q1', '2017-03-31', '2017一季度'),
                        # ('2016Q4','2016-12-31','2016年报'),('2016Q3', '2016-09-30', '2016三季度'),
                        # ('2016Q2', '2016-06-30', '2016半年报'),('2016Q1', '2016-03-31', '2016一季度'),
                        # ('2015Q4','2015-12-31','2015年报'),('2015Q3', '2015-09-30', '2015三季度'),
                        # ('2015Q2', '2015-06-30', '2015半年报'),('2015Q1', '2015-03-31', '2015一季度')
                      )
    try:

        for icounter , report_date in enumerate(report_date_arr):
            # 只更新当季的数据
            if update == 'current' and icounter > 0:
                break

            # 只做增量更新
            if supplement:
                sql = 'select distinct notice_date from fin_report where type = "%s" order by notice_date desc limit 3'%(report_date[0])
                start_date_str = web_data.get_start_date(src='fin',sql=sql, format=8)
                if start_date_str is None:
                    # supplement = False
                    start_date_str = report_date[1]
                    start_date_str = re.sub(r'\-','',start_date_str)
                    wx.info("[update_fin_report_from_eastmoney] {} 财报在数据库无记录，起始日期设置为 {}".format(report_date[0], report_date[1]))
            page_count = 1
            total_pages = 0
            items_page = 300
            df_report_record = pd.DataFrame()  # 保存期内 所有股票的 报表数据，最后一次性写入数据库
            loop_page = True
            while loop_page:
                east_report_url = "http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?" \
                                  "type=YJBB21_YJBB&token=70f12f2f4f091e459a279469fe49eca5&st=latestnoticedate&" \
                                  "sr=-1&p="+str(page_count)+"&ps="+str(items_page)+"&js=var%20IpgmqEsv={pages:(tp),data:%20(x),font:(font)}&" \
                                  "filter=(securitytypecode%20in%20(%27058001001%27,%27058001002%27))" \
                                  "(reportdate=^"+report_date[1]+"^)&rt=52309816"

                east_report_str = web_data.get_json_str(url=east_report_url, web_flag='eastmoney')

                if east_report_str is None or len(east_report_str)<=0:
                    wx.info("[update_fin_report_from_eastmoney] 类型：{}--- URL读取为空，即将开始获取下一季财报".format(report_date[2]))
                    break

                # 用替换 渠道字符串中所有空格和不显示的字符
                east_report_str =  re.sub(r'\s+', '' , east_report_str)
                # para_result = re.search(r'(?:\{pages\:)(\d+)', str)

                # 把字符串 拆分成 总页数、报表记录、数字映射 三个部分
                para_result = re.search(r'(?:\{pages\:)(\d+)(?:\S+\[)(.*)(?:\].*FontMapping\S+\[)(.*)(?:\])', east_report_str)
                if para_result is None:
                    wx.info("[update_fin_report_from_eastmoney] 类型：{}--- URL内容解析为空，即将开始获取下一季财报".format(report_date[2]))
                    break

                total_pages = int(para_result.group(1)) # 总页数
                font_mapping = para_result.group(3)  # 数字映射
                report_record_str =  para_result.group(2) # 报表记录
                wx.info("[update_fin_report_from_eastmoney] {}-- page {}/ {}处理中...".format(report_date[2], page_count, total_pages))

                # 把数字映射表，拆分成 数组形式
                font_mapping = re.sub(r'\"code\"\:','',font_mapping) # 删除"code"
                font_mapping = re.sub(r'\"value\"\:','',font_mapping) # 删除 "value"
                font_mapping = re.sub(r'\"','',font_mapping) # 删除 "\""
                font_mapping_arr = re.findall(r'(?:\{)(.*?)(?:\})', font_mapping)

                # 对 report_record_str完成数字映射，解密
                for single_mapping_str in font_mapping_arr:
                    single_mapping_arr = single_mapping_str.split(',')
                    report_record_str = re.sub(single_mapping_arr[0], single_mapping_arr[1], report_record_str)

                # 开始处理 报表数据，处理成 Json 格式，{data:[ report_record_str ]}
                json_report_record_str = '{"data":['+report_record_str+']}'
                df_tmp = web_data.east_fin_report_json_parse(json_str=json_report_record_str)

                if df_tmp is not None or not df_tmp.empty:
                    # 增量更新
                    if supplement:
                    #   发现 数据库已存在的start_date_str 在 DataFrame 中，表示数据出现重复，可以截断DataFrame，并停止下载下一页数据
                        if start_date_str in df_tmp["notice_date"].tolist():
                            df_tmp = df_tmp.loc[df_tmp["notice_date"] >= start_date_str].copy()
                            wx.info("第 {} 页 {} 条新数据，加入更新DataFrame·".format(page_count, len(df_tmp)))
                            loop_page = False
                        else:
                            wx.info("第 {} 页全部都是新数据，加入更新DataFrame·".format(page_count))

                    # 从 df_tmp 进入 df_report_record
                    if df_report_record.empty:
                        df_report_record = df_tmp.copy()
                        wx.info("[update_fin_report_from_eastmoney] 类型：{}--- 页数{}/{} 获得 {} 条报表数据".
                                format(report_date[2], page_count,total_pages, len(df_report_record)))
                    else:
                        df_report_record = df_report_record.append(df_tmp, sort=True)
                        wx.info("[update_fin_report_from_eastmoney] 类型：{}--- 页数{}/{} 获得 {} 条报表数据".
                                format(report_date[2], page_count,total_pages, len(df_tmp)))

                page_count += 1
                if page_count > total_pages:
                    loop_page = False

            if not df_report_record.empty:
                df_report_record['type'] = report_date[0]
                wx.info("[update_fin_report_from_eastmoney] 类型：{}--- 共获得 {} 条报表数据，写入数据表".
                        format(report_date[2], len(df_report_record)))
                df_report_record = df_report_record[['type', 'id', 'name', 'notice_date',
                                                     'basiceps',  'cutbasiceps',  'totaloperatereve',
                                                     'ystz',  'yshz',   'parentnetprofit', 'sjltz',  'sjlhz',
                                                     'roeweighted',  'bps',  'mgjyxjje', 'xsmll', 'industry_name',
                                                     'assigndscrpt', 'gxl']]
                df_report_record.replace('-',0,inplace = True)
                web_data.db_load_into_fin_data(df_fin_record= df_report_record)

    except Exception as e:
        wx.info("Err [update_daily_data_from_eastmoney]:捕获异常：{}".format(e))



"""
# 从东财接口 获取股权质押的数据
# update = 'current' / 'all'
# 
"""

def update_zhiya_from_eastmoney(supplement = True):
    wx = lg.get_handle()
    web_data = ex_web_data()

    if supplement:
        sql = 'select distinct notice_date from stock.dd_zhiya order by notice_date desc limit 3'
        start_date_str = web_data.get_start_date(src='zhiya', sql=sql, format=8)
        if start_date_str is None:
            wx.info("[update_zhiya_from_eastmoney] 股票质押数据库无记录, 需要将 Supplement 设置为False")
    else:
        sql = 'delete from stock.dd_zhiya'
        web_data.db._exec_sql(sql = sql)
        wx.info("[update_zhiya_from_eastmoney] 已清空质押数据")

    page_count = 1
    df_zhiya_record = pd.DataFrame()  # 保存期内 所有股票的 报表数据，最后一次性写入数据库
    loop_page = True
    while loop_page:
        total_pages = 0
        zhiya_url = "http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=GDZY_LB&" \
                   "token=70f12f2f4f091e459a279469fe49eca5&cmd=&st=ndate&sr=-1&p="+str(page_count)+"&ps=200&" \
                   "js=var%20iQprlwbx={pages:(tp),data:(x),font:(font)}&filter=(datatype=1)&rt=53162692"

        zhiya_str = web_data.get_json_str(url=zhiya_url, web_flag='eastmoney')


        if zhiya_str is None or len(zhiya_str) <= 0:
            wx.info("[update_zhiya_from_eastmoney] 类型：{}--- URL读取为空".format(zhiya_str[2]))
            break

            # 用替换 渠道字符串中所有空格和不显示的字符
        zhiya_str = re.sub(r'\s+', '', zhiya_str)
        # para_result = re.search(r'(?:\{pages\:)(\d+)', str)

        # 把字符串 拆分成 总页数、报表记录、数字映射 三个部分
        # para_result = re.search(r'(?:\{pages\:)(\d+)(?:\S+\[)(.*)(?:\].*FontMapping\S+\[)(.*)(?:\])', zhiya_str)
        para_result = re.search(r'(?:\{pages\:)(\d+)(?:\S+data\:\[)(.*)(?:\].*FontMapping\S+\[)(.*)(?:\])', zhiya_str)
        if para_result is None:
            wx.info("[update_zhiya_from_eastmoney] 第[{}]页 内容为空".format(page_count))
            break

        total_pages = int(para_result.group(1))  # 总页数
        font_mapping = para_result.group(3)  # 数字映射
        zhiya_record_str = para_result .group(2)  # 报表记录
        wx.info(
            "[update_zhiya_from_eastmoney] 开始第 {}/ {}页处理...".format(page_count, total_pages))

        # 把数字映射表，拆分成 数组形式
        font_mapping = re.sub(r'\"code\"\:', '', font_mapping)  # 删除"code"
        font_mapping = re.sub(r'\"value\"\:', '', font_mapping)  # 删除 "value"
        font_mapping = re.sub(r'\"', '', font_mapping)  # 删除 "\""
        font_mapping_arr = re.findall(r'(?:\{)(.*?)(?:\})', font_mapping)

        # 对 report_record_str完成数字映射，解密
        for single_mapping_str in font_mapping_arr:
            single_mapping_arr = single_mapping_str.split(',')
            zhiya_record_str = re.sub(single_mapping_arr[0], single_mapping_arr[1], zhiya_record_str)

        # 开始处理 报表数据，处理成 Json 格式，{data:[ report_record_str ]}
        json_zhiya_record_str = '{"data":[' + zhiya_record_str + ']}'
        df_tmp = web_data.east_zhiya_json_parse(json_str=json_zhiya_record_str)

        if df_tmp is not None or not df_tmp.empty:

            # 增量更新
            if supplement:
                #   发现 数据库已存在的start_date_str 在 DataFrame 中，表示数据出现重复，可以截断DataFrame，并停止下载下一页数据
                if start_date_str in df_tmp["notice_date"].tolist():
                    df_tmp = df_tmp.loc[df_tmp["notice_date"] > start_date_str].copy()
                    wx.info("第 {} 页 {} 条新数据，加入更新DataFrame·".format(page_count, len(df_tmp)))
                    loop_page = False
                else:
                    wx.info("第 {} 页全部都是新数据，加入更新DataFrame·".format(page_count))

            # 从 df_tmp 进入 df_zhiya_record
            if df_zhiya_record.empty:
                df_zhiya_record = df_tmp.copy()
                wx.info("[update_zhiya_from_eastmoney]  页数{}/{} 获得 {} 条报表数据".
                        format( page_count, total_pages, len(df_zhiya_record)))
            else:
                df_zhiya_record = df_zhiya_record.append(df_tmp)
                wx.info("[update_zhiya_from_eastmoney]  页数{}/{} 获得 {} 条报表数据".
                        format( page_count, total_pages, len(df_tmp)))


        # 按页数分段获取
        # if page_count == 200:
        #     break

        if page_count == total_pages:
            loop_page = False
            wx.info(
                "[update_zhiya_from_eastmoney] 已到达最后一页{}/{} ".format(page_count, total_pages))
        else:
            page_count += 1

        # 测试用
        # loop_page = False

    if not df_zhiya_record.empty and len(df_zhiya_record) > 0:
        df_zhiya_record.replace('-', 0, inplace=True)
        web_data.db_load_into_zhiya_data(df_record=df_zhiya_record)


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
