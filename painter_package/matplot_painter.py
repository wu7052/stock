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
        self.cnames = {
                        'darkblue':             '#00008B',
                        'darkcyan':             '#008B8B',
                        'darkgoldenrod':        '#B8860B',
                        'darkgray':             '#A9A9A9',
                        'darkgreen':            '#006400',
                        'darkkhaki':            '#BDB76B',
                        'darkmagenta':          '#8B008B',
                        'darkolivegreen':       '#556B2F',
                        'darkorange':           '#FF8C00',
                        'darkorchid':           '#9932CC',
                        'darkred':              '#8B0000',
                        'darksalmon':           '#E9967A',
                        'darkseagreen':         '#8FBC8F',
                        'darkslateblue':        '#483D8B',
                        'darkslategray':        '#2F4F4F',
                        'darkturquoise':        '#00CED1',
                        'darkviolet':           '#9400D3',
                        'deeppink':             '#FF1493',
                        'deepskyblue':          '#00BFFF',
                        'aliceblue':             '#F0F8FF',
                        'antiquewhite':          '#FAEBD7',
                        'aqua':                   '#00FFFF',
                        'aquamarine':            '#7FFFD4',
                        'azure':                  '#F0FFFF',
                        'beige':                '#F5F5DC',
                        'bisque':               '#FFE4C4',
                        'black':                '#000000',
                        'blanchedalmond':       '#FFEBCD',
                        'blue':                 '#0000FF',
                        'blueviolet':           '#8A2BE2',
                        'brown':                '#A52A2A',
                        'burlywood':            '#DEB887',
                        'cadetblue':            '#5F9EA0',
                        'chartreuse':           '#7FFF00',
                        'chocolate':            '#D2691E',
                        'coral':                '#FF7F50',
                        'cornflowerblue':       '#6495ED',
                        'cornsilk':             '#FFF8DC',
                        'crimson':              '#DC143C',
                        'cyan':                 '#00FFFF',

                        'dimgray':              '#696969',
                        'dodgerblue':           '#1E90FF',
                        'firebrick':            '#B22222',
                        'floralwhite':          '#FFFAF0',
                        'forestgreen':          '#228B22',
                        'fuchsia':              '#FF00FF',
                        'gainsboro':            '#DCDCDC',
                        'ghostwhite':           '#F8F8FF',
                        'gold':                 '#FFD700',
                        'goldenrod':            '#DAA520',
                        'gray':                 '#808080',
                        'green':                '#008000',
                        'greenyellow':          '#ADFF2F',
                        'honeydew':             '#F0FFF0',
                        'hotpink':              '#FF69B4',
                        'indianred':            '#CD5C5C',
                        'indigo':               '#4B0082',
                        'ivory':                '#FFFFF0',
                        'khaki':                '#F0E68C',
                        'lavender':             '#E6E6FA',
                        'lavenderblush':        '#FFF0F5',
                        'lawngreen':            '#7CFC00',
                        'lemonchiffon':         '#FFFACD',
                        'lightblue':            '#ADD8E6',
                        'lightcoral':           '#F08080',
                        'lightcyan':            '#E0FFFF',
                        'lightgoldenrodyellow': '#FAFAD2',
                        'lightgreen':           '#90EE90',
                        'lightgray':            '#D3D3D3',
                        'lightpink':            '#FFB6C1',
                        'lightsalmon':          '#FFA07A',
                        'lightseagreen':        '#20B2AA',
                        'lightskyblue':         '#87CEFA',
                        'lightslategray':       '#778899',
                        'lightsteelblue':       '#B0C4DE',
                        'lightyellow':          '#FFFFE0',
                        'lime':                 '#00FF00',
                        'limegreen':            '#32CD32',
                        'linen':                '#FAF0E6',
                        'magenta':              '#FF00FF',
                        'maroon':               '#800000',
                        'mediumaquamarine':     '#66CDAA',
                        'mediumblue':           '#0000CD',
                        'mediumorchid':         '#BA55D3',
                        'mediumpurple':         '#9370DB',
                        'mediumseagreen':       '#3CB371',
                        'mediumslateblue':      '#7B68EE',
                        'mediumspringgreen':    '#00FA9A',
                        'mediumturquoise':      '#48D1CC',
                        'mediumvioletred':      '#C71585',
                        'midnightblue':         '#191970',
                        'mintcream':            '#F5FFFA',
                        'mistyrose':            '#FFE4E1',
                        'moccasin':             '#FFE4B5',
                        'navajowhite':          '#FFDEAD',
                        'navy':                 '#000080',
                        'oldlace':              '#FDF5E6',
                        'olive':                '#808000',
                        'olivedrab':            '#6B8E23',
                        'orange':               '#FFA500',
                        'orangered':            '#FF4500',
                        'orchid':               '#DA70D6',
                        'palegoldenrod':        '#EEE8AA',
                        'palegreen':            '#98FB98',
                        'paleturquoise':        '#AFEEEE',
                        'palevioletred':        '#DB7093',
                        'papayawhip':           '#FFEFD5',
                        'peachpuff':            '#FFDAB9',
                        'peru':                 '#CD853F',
                        'pink':                 '#FFC0CB',
                        'plum':                 '#DDA0DD',
                        'powderblue':           '#B0E0E6',
                        'purple':               '#800080',
                        'red':                  '#FF0000',
                        'rosybrown':            '#BC8F8F',
                        'royalblue':            '#4169E1',
                        'saddlebrown':          '#8B4513',
                        'salmon':               '#FA8072',
                        'sandybrown':           '#FAA460',
                        'seagreen':             '#2E8B57',
                        'seashell':             '#FFF5EE',
                        'sienna':               '#A0522D',
                        'silver':               '#C0C0C0',
                        'skyblue':              '#87CEEB',
                        'slateblue':            '#6A5ACD',
                        'slategray':            '#708090',
                        'snow':                 '#FFFAFA',
                        'springgreen':          '#00FF7F',
                        'steelblue':            '#4682B4',
                        'tan':                  '#D2B48C',
                        'teal':                 '#008080',
                        'thistle':              '#D8BFD8',
                        'tomato':               '#FF6347',
                        'turquoise':            '#40E0D0',
                        'violet':               '#EE82EE',
                        'wheat':                '#F5DEB3',
                        'white':                '#FFFFFF',
                        'whitesmoke':           '#F5F5F5',
                        'yellow':               '#FFFF00',
                        'yellowgreen':          '#9ACD32'}

    def __del__(self):
        pass


    # 并列柱状图 绘制
    # x_label 行业数组
    # x_sub_label 日期数组
    def para_pillar_draw(self, x_label=None, x_sub_label = None, df=None):
        wx = lg.get_handle()
        if df is None or x_label is None or x_sub_label is None:
            wx.info("[mp_painter][para_pillar_draw] dataframe is Empty, return")
            return


        # 每个行业之间空 0.2， 行业的柱子 总宽度 0.8
        total_width, n = 0.8, len(x_sub_label)
        # 每个柱子的宽度
        width = total_width / n

        # 日期序号[0,1,2,3,4,5...]
        col_num = list(range(len(x_sub_label)))
        col_pos_dict = {}
        # start_pos = [x*width for x in col_num]

        # 准备下一个行业的柱子 的位置
        for i in range(len(x_label)):
            pos = [i+x*width for x in col_num]
            col_pos_dict[x_label[i]] = pos

        # 颜色序号准备
        icolor = 0
        color_name = list(self.cnames.keys())
        for x in x_label:
            df_industry = df.loc[df["industry_name"] == x].sort_values(["date"],ascending=True)
            plt.bar(col_pos_dict[x], df_industry.qty.tolist(), width=width, label=x, fc=color_name[icolor])
            icolor += 1
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签

        plt.legend()
        plt.show()



    # K线图 绘制，id 股票ID， df 该股票ID的date 、 OHLC 价格
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

