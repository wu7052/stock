from conf import conf_handler
import new_logger as lg
from db_package import db_ops
from datetime import datetime, time, date, timedelta
import pandas as pd

class analyzer():
    def __init__(self):
        try:
            wx = lg.get_handle()
            self.h_conf = conf_handler(conf="stock_analyer.conf")
            # secs = h_conf.rd.sections()
            # wx.info("conf :{}".format(secs))
            host =  self.h_conf.read('db','host')
            database =  self.h_conf.read('db','database')
            user =  self.h_conf.read('db','user')
            pwd =  self.h_conf.read('db','pwd')
            # wx.info("conf :{}:{}:{}:{}".format(host, database, user, pwd))
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)
            wx.info("[OBJ] analyzer __init__() called")
        except Exception as e:
            raise e

    def __del__(self):
        wx = lg.get_handle()
        self.db.cursor.close()
        self.db.handle.close()
        wx.info("[OBJ] analyzer __del__() called")


    def ana_dgj_trading(self, ass_type=None, start_date = None, vol=10000):
        wx=lg.get_handle()
        ass_type_value = self.h_conf.read('ass_type',ass_type)
        ass_weight = self.h_conf.read('ass_weight',ass_type+'_weight')
        # ass_life = self.h_conf.read('ass_weight_life','dgj_life')
        today = (date.today()).strftime('%Y%m%d')

        sql = "select dgj.id, la.name, '"+ today +"'as date, '"+ass_type_value+"' as ass_type, '"\
              +ass_weight+"' as ass_weight, round(sum(dgj.vol)/10000,0) as ass_comment" \
              " from dgj_201901 as dgj left join list_a  as la on la.id =  dgj.id " \
              " where date > "+start_date+" group by dgj.id having sum(dgj.vol)>"+str(vol)
        df_dgj_buy = self.db._exec_sql(sql)

        sql = "select dgj.id, la.name, '"+ today +"' as date, '"+ass_type_value+"' as ass_type, '-"\
              +ass_weight+"' as ass_weight, round(sum(dgj.vol)/10000,0) as ass_comment " \
              " from dgj_201901 as dgj left join list_a  as la on la.id =  dgj.id " \
              " where date > " + start_date + " group by dgj.id having sum(dgj.vol)<-"+str(vol)
        df_dgj_sell = self.db._exec_sql(sql)
        df_dgj = pd.concat([df_dgj_buy, df_dgj_sell])

        self.db_refresh_fruit(type=ass_type_value, df_ana= df_dgj)
        wx.info("[db_refresh_fruit] Ass_Type {} is freshed".format(ass_type))

    def db_refresh_fruit(self, type=None, df_ana = None):
        wx = lg.get_handle()
        if df_ana is None :
            wx.info("[db_refresh_fruit] Err: Analysis Data Frame or type is Empty,")
            return -1
        ana_array = df_ana.values.tolist()
        i = 0
        while i < len(ana_array):
            ana_array[i] = tuple(ana_array[i])
            i += 1
        sql = "delete from fruit where ass_type="+type
        iCount = self.db.cursor.execute(sql)
        self.db.handle.commit()
        wx.info("[db_refresh_fruit] Refresh Ass_Type {}, {} rows of date removed ".format(type, iCount))

        sql = "REPLACE INTO fruit SET id=%s, name=%s, date=%s, " \
              "ass_type=%s, ass_weight=%s, ass_comment=%s"

        self.db.cursor.executemany(sql, ana_array)
        self.db.handle.commit()
        # wx.info("conf-dgj :{}:{}:{}".format(ass_type, ass_weight, ass_life))

