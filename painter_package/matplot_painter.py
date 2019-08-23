from matplotlib import pyplot as plt
from mpl_finance import candlestick_ohlc
from matplotlib.pylab import date2num
import matplotlib.dates as dates
from matplotlib.ticker import Formatter
from matplotlib import ticker
import pandas as pd
from datetime import datetime, date, timedelta
import new_logger as lg
import numpy as np


class MyFormatter(Formatter):
    def __init__(self, dates, fmt='%Y%m%d'):
        self.dates = dates
        self.fmt = fmt

    def __call__(self, x, pos=0):
        # 'Return the label for time x at position pos'
        # plt.show 时被调用， x 依次传入 fake_date_arr 的元素
        # 常量 730120 == 20000101 matplotlib 时间戳
        ind = int(np.round(x))-730120
        if ind >= len(self.dates) or ind < 0:
            return ''

        return self.dates[ind]


class mp_painter():
    def __init__(self):
        pass

    def __del__(self):
        pass

    def candle_draw(self,id='', df=None):
        wx = lg.get_handle()
        if df is None :
            wx.info("[mp_painter][candle_draw] dataframe is Empty, return")
            return

        fake_date_arr = ['20000101']
        fake_date = datetime.strptime('20000101', '%Y%m%d')
        i = 1
        while i < len(df):
            fake_date += timedelta(days=1)
            fake_date_arr.append(fake_date.strftime('%Y%m%d'))
            i += 1

        # 日期处理为时间戳
        real_trade_date = df['trade_date'].copy().tolist()
        df['trade_date'] = fake_date_arr
        df.trade_date = df.trade_date.apply(lambda x: date2num(datetime.strptime(x, "%Y%m%d")))

        # Tushare 返回的数据 按日期倒序，按日期升序后，需要把倒序的index先删掉，再重新生成升序的 index
        # 用 index 填充 日期列，当做日期使用
        # df.reset_index(drop=False, inplace=True)
        # df.drop('index', axis=1, inplace=True)
        # df.reset_index(drop=False, inplace=True)
        # df.drop('trade_date', axis=1, inplace=True)
        # df.index = df.index.apply(lambda x: date2num(x))

        candle_array = df.values
        # 创建一个子图
        fig, ax = plt.subplots(facecolor=(0.5, 0.5, 0.5))
        fig.subplots_adjust(bottom=0.1)
        # 设置X轴刻度为日期时间
        ax.xaxis_date()
        ax.autoscale_view()

        candlestick_ohlc(ax, candle_array, colordown='#53c156', colorup='#ff1717',width=0.3,alpha=1)
        # ax.xaxis.set_major_locator(ticker.MultipleLocator(6))
        # ax.set_xticks(range(len(real_trade_date)))
        # ax.set_xticklabels(real_trade_date)
        formatter = MyFormatter(real_trade_date)
        ax.xaxis.set_major_formatter(formatter)

        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

        # X轴刻度文字倾斜45度
        # plt.xticks( rotation=45)
        plt.grid(True)
        plt.title("回测结果:["+id+"] K线图")
        plt.xlabel("日期")
        plt.ylabel("价格(元)")
        # candlestick_ochl(ax, candle_array, colordown='#53c156', colorup='#ff1717', width=0.3, alpha=1)
        plt.show()

