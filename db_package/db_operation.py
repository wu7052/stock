import pymysql
import new_logger as lg

class db_ops:
    wx = lg.get_handle()
    def __init__(self, host='localhost', db='mysql', user='root', pwd=None):
        wx = lg.get_handle()
        try:
            if pwd is None:
                wx.info("[Err DB_OP]===> {0}:{1}:{2} need password ".format(host, db, user))
                raise Exception("Password is Null")
            else:
                # self.pwd = pwd
                self.config = {
                    'host': host,
                    'user': user,
                    'password': pwd,
                    'database': db,
                    'charset': 'utf8',
                    'port': 3306  # 注意端口为int 而不是str
                }
                # self.handle = pymysql.connect(self.host, self.user, self.pwd, self.db_name)
                self.handle = pymysql.connect(**self.config)
                self.cursor = self.handle.cursor()
        except Exception as e:
            wx.info("Err occured in DB_OP __init__{}".format(e))
            raise e

    def __del__(self):
        wx = lg.get_handle()
        wx.info("db_ops : {}: __del__ called".format(self))