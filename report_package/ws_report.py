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


    def get_list_a_total_amount(self):
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
        wx.info(result)
