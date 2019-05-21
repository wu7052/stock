from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import pandas as pd

from datetime import datetime, time, date, timedelta
import time

class ma(object):
    def __init__(self):
        wx = lg.get_handle()
        self.h_conf = conf_handler(conf="stock_analyer.conf")
        host = self.h_conf.rd_opt('ma', 'period')
        pass

