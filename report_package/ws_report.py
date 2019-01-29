import new_logger as lg
from datetime import datetime, time, date, timedelta
import pandas as pd
from db_package import db_ops

class ws_rp():
    def __init__(self):
        wx = lg.get_handle()
        wx.info("ws_report class __init__ called")
        try:
            self.db = db_ops(host='127.0.0.1', db='stock', user='wx', pwd='5171013')
            wx.info("ex_web_data : __init__() called")
        except Exception as e:
            raise e

    def __del__(self):
        wx = lg.get_handle()
        self.db.cursor.close()
        self.db.handle.close()
        wx.info("ws_rp :{}: __del__() called".format(self))


    def calc_total_amount(self):
        wx = lg.get_handle()
        start_date = (date.today() + timedelta(days=0)).strftime('%Y%m%d')
        # wx.info("get list a total amount called")
        # self.db.db_call_procedure("list_a_total_amount", start_date, 1, 2, 3, 4, 5, 6)
        # self.db.cursor.callproc(p_name, (args[0], args[1], args[2], args[3], args[4], args[5], args[6]))
        p_name = "list_a_total_amount"
        self.db.cursor.callproc(p_name, ('20190124', 1, 2, 3, 4, 5, 6,7,8))
        self.db.cursor.execute(
            "select @_" + p_name + "_0, @_" + p_name + "_1, @_" + p_name + "_2, @_" + p_name + "_3, @_"
            + p_name + "_4, @_" + p_name + "_5, @_" + p_name + "_6, @_"+ p_name + "_7, @_"+ p_name + "_8")
        result = self.db.cursor.fetchall()
        self.db.handle.commit()
        wx.info(result)

    def calc_days_vol(self, days = 0 , type = None):
        # SELECT dd.id, sum(dd.vol) as sub_tt_vol, la.flow_shares as flow_shares, sum(dd.vol) /flow_shares as pct
        # FROM stock.code_60_201901 as dd left join list_a as la on dd.id = la.id
        # where str_to_date(dd.date,'%Y%m%d') > (current_date()-7)  group by dd.id;
        wx = lg.get_handle()
        if type == '60':
            sql = "SELECT dd.id, sum(dd.vol) , la.flow_shares as flow_shares, sum(dd.vol)/(flow_shares*100) as pct," \
                  " sum(dd.vol)/" +str(days) + " as ave FROM stock.code_60_201901 as dd left join list_a as la on dd.id = la.id " \
                  "where str_to_date(dd.date,'%Y%m%d') > (current_date()-" +str(days) + ")  group by dd.id order by pct desc;"
        elif type == '30':
            sql = "SELECT dd.id, sum(dd.vol) , la.flow_shares as flow_shares, sum(dd.vol)/(flow_shares*100) as pct," \
                  " sum(dd.vol)/" +str(days) + " as ave FROM stock.code_30_201901 as dd left join list_a as la on dd.id = la.id " \
                  "where str_to_date(dd.date,'%Y%m%d') > (current_date()-" +str(days) + ")  group by dd.id order by pct desc;"
        elif type == '002':
            sql = "SELECT dd.id, sum(dd.vol) , la.flow_shares as flow_shares, sum(dd.vol)/(flow_shares*100) as pct," \
                  " sum(dd.vol)/" +str(days) + " as ave FROM stock.code_002_201901 as dd left join list_a as la on dd.id = la.id " \
                  "where str_to_date(dd.date,'%Y%m%d') > (current_date()-" +str(days) + ")  group by dd.id order by pct desc;"
        elif type == '00':
            sql = "SELECT dd.id, sum(dd.vol) , la.flow_shares as flow_shares, sum(dd.vol)/(flow_shares*100) as pct," \
                  " sum(dd.vol)/" +str(days) + " as ave FROM stock.code_00_201901 as dd left join list_a as la on dd.id = la.id " \
                  "where str_to_date(dd.date,'%Y%m%d') > (current_date()-" +str(days) + ")  group by dd.id order by pct desc;"
        else:
            wx.info("[calc days vol] input Wrong Type {}".format(type))

        iCount = self.db.cursor.execute(sql)
        self.db.handle.commit()
        if iCount > 0:
            wx.info("[calc days vol] acquire {} rows of result".format(iCount))
            arr_days_vol = self.db.cursor.fetchall()
            columnDes = self.db.cursor.description  # 获取连接对象的描述信息
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]
            df_days_vol = pd.DataFrame([list(i) for i in arr_days_vol], columns=columnNames)
            return  df_days_vol
        else:
            wx.info("[calc days vol] failed to exec SQL {}".format(sql))
