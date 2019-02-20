from datetime import datetime
import new_logger as lg
import configparser
import os
import sys

class conf_handler():

    def __init__(self, conf=None):
        wx = lg.get_handle()
        conf = os.path.dirname(os.path.abspath(sys.argv[0]))+"\\" + conf
        wx.info("[OBJ] Conf_handler : __init__ called ")
        self.rd = configparser.ConfigParser()
        self.rd.read(conf)  ##读取配置文件


    def __del__(self):
        wx = lg.get_handle()
        wx.info("[OBJ] Conf_handler : __del__ called")


    def read(self, sec=None, opt=None):
        return self.rd.get(sec, opt)
