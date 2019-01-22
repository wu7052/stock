import tushare as ts
from datetime import datetime, date, timedelta
import time
import new_logger as lg

class ts_data:
    __counter = 1
    __timer = 0
    def __init__(self):
        self.ts= ts.pro_api('9e78306f8bbe893520528008f70653779cc98c5ec88c07340a3b8f18')

    def basic_info(self):
        data = self.ts.stock_basic(exchange='', list_status='L', fields='symbol,name,area,industry,list_date')
        # data = self.ts.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        return data

    def trans_day(self):
        wx = lg.get_handle()
        today = date.today().strftime('%Y%m%d')
        yesterday = (date.today() + timedelta(days=-1)).strftime('%Y%m%d')
        wx.info("Yesterday:{} ---- Today date: {}".format(yesterday,today))
        return self.ts.trade_cal(exchange='', start_date=yesterday, end_date=today)

    def acquire_daily_data(self, code, period):
        wx = lg.get_handle()
        wx.info("tushare called {} times，id: {}".format(ts_data.__counter, code))
        if (ts_data.__counter == 1):  # 第一次调用，会重置 计时器
            ts_data.__timer = time.time()
        if (ts_data.__counter >= 200): # 达到 200 次调用，需要判断与第一次调用的时间间隔
            ts_data.__counter = 0      # 重置计数器=0，下面立即调用一次 计数器+1
            wait_sec = 60 - (int)(time.time() - ts_data.__timer) # 计算时间差
            # ts_data.__timer = time.time() # 重置计时器
            if (wait_sec > 0): # 在一分钟内已经累计200次调用，需要停下等待了
                wx.info("REACH THE LIMIT, MUST WAIT ({}) SECONDS".format(wait_sec))
                time.sleep(wait_sec+2)
                # ts_data.__timer = time.time() # 不需要重置计时器，因为上面重置了 计数器，下一次调用时，会重置计时器
            else:
                # 累计到 200次 调用，用时已超过1分钟，新的一分钟 怎么计算 200 次调用呢
                ts_data.__counter = 8 * abs(wait_sec)
                # ts_data.__timer += 60 + abs(wait_sec)
                ts_data.__timer = time.time()-abs(wait_sec)
                wx.info("Called 200 times More than 60 + {} seconds. New timer start at {}".format(abs(wait_sec),ts_data.__timer))

        end_date = date.today().strftime('%Y%m%d')
        start_date = (date.today() + timedelta(days = period)).strftime('%Y%m%d')
        try:
            df = self.ts.query('daily', ts_code=code, start_date=start_date, end_date=end_date)
        except Exception as e:
            wx.info("tushare exception: {}... sleep 60 seconds, retry....".format(e))
            time.sleep(60)
            df = self.ts.query('daily', ts_code=code, start_date=start_date, end_date=end_date)
            ts_data.__timer = time.time()
            ts_data.__counter = 0
        ts_data.__counter += 1
        return df