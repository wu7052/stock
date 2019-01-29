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
    str_code =  str(code)
    if re.match(r'\d{2}0{4}$',str_code):  # level ONE
        return 1
    elif re.match(r'\d{3}[1-9]0{2}$',str_code):  # level 2
        return 2
    elif re.match(r'\d{5}[1-9]$',str_code): #level 3
        return 3
    elif re.match(r'\d{4}[1-9]0$',str_code): #level 3
        return 3
    else:
        return 0


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
    sw_industry_arr = web_data.db_get_sw_industry_code(level = 2)
    code_counter = 1
    for code in sw_industry_arr:
        page_counter = 1
        while True:
            sina_industry_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/" \
                                "Market_Center.getHQNodeData?page=" +str(page_counter) + "&num=80&sort=symbol&asc=1&" \
                                "node=sw2_" +str(code[0])+ "&symbol=&_s_r_a=setlen"
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
            web_data.db_update_sw_industry_into_basic_info(code = code[0], id_arr = stock_id_arr)
            page_counter +=1
        code_counter += 1


@wx_timer
def update_daily_data_from_sina():
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
                today = datetime.now().strftime('%Y%m%d')

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
def update_whole_sales_data(force=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    today = datetime.now().strftime('%Y-%m-%d')

    # select * from ws_201901 where  str_to_date(date, '%Y%m%d') < str_to_date('20180701', '%Y%m%d');
    page_counter = 1

    # 强制刷新 ws 表，删除所有历史数据，重新导入
    if force:  #== True:
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
                            "filter=(Stype='EQA')(TDATE%3E=%5E" + start_date + "%5E%20and%20TDATE%3C=%5E" + today + \
                           "%5E)&rt=51576724"
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
            web_data.db_load_into_ws_share_holder(df_share_holder = df_share_holder )
            wx.info("{}/{} : {} Loaded Share Holders' Information".format(iCounter, len(arr_id), stock_id[0]))
    wx.info("update_ws_share_holder loaded completed ! ")

@wx_timer
def ws_supplement():
    wx = lg.get_handle()
    web_data = ex_web_data()
    wx.info("Start to supplement more information ")
    # 调用 mysql 存储过程，在 ws表中 补充 stock_name , h_name, buy_date, sell_date
    if (web_data.whole_sales_supplement_info()):
        wx.info("update_ws_share_holder succeed !")
    else:
        wx.info("update_ws_share_holder failed !")

@wx_timer
def report_total_amount():
    # wx = lg.get_handle()
    rp = ws_rp()
    rp.calc_total_amount()

@wx_timer
def report_days_vol(days = 0, type = None):
    # wx = lg.get_handle()
    rp = ws_rp()
    df_days_vol = rp.calc_days_vol(days, type)
    return df_days_vol

"""
# stock = ts_data()
# data = stock.basic_info()
# print(data)
# pass
"""

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

# workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
# pack_name = ['logger_package','stock_package','filePackage','db_package']
# for _ in pack_name:
#     pack_path = workpath+'\\'+_
#     sys.path.insert(0, pack_path)

# logfile_dir = os.path.dirname(os.path.abspath('.'))
# logfile_dir += '\\log\\'
# print (logfile_dir)

# path= sys.path
# print (path)

# path = sys.argv[0]
# print (sys.argv[0])
# print (os.path.abspath(sys.argv[0]))
# workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
# sys.path.insert(0, os.path.join(workpath, 'modules'))

"""
# 开始写入数据库 Stock -- 表list_a
for array in basic_info_df.get_values():
    sql = "select * from list_a where id ='"+array[0]+"'"
    # sql =  'select count(*) from list_a where id = \'%s\''%array[0]
    iCount = db.cursor.execute(sql) # 返回值，受影响的行数， 不需要 fetchall 来读取了
    if iCount == 0:
        sql ="insert into list_a (id, name, total_shares, flow_shares, list_date, full_name, industry, industry_code) " \
             "values (%s, %s, %s ,%s, %s, %s, %s, %s)"
        wx.info("Insert id={0}, name={1}, t_shares={2}, f_shares={3}, date={4}, f_name={5}, industry={6}, industry_code={7}".
                     format(array[0], array[1], array[2], array[3], array[4], array[5], array[6], array[7]))
        db.cursor.execute(sql,(array[0], array[1], float(array[2]), float(array[3]), array[4], array[5], array[6], array[7]))
        db.handle.commit()
    elif iCount == 1:
        wx.info("Existed\t[{0}==>{1}]".format(array[0], array[1]))
    else:
        wx.info("iCount == %d , what happended ???"% iCount)
"""
