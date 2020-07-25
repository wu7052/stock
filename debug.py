# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ma_kits, psy_kits
from report_package import ws_rp
# ma = ma_kits()
# psy = psy_kits()
# np_cprice = psy.get_cprice(stock_id= "600000")
# psy.calc(np_cprice)

import pandas as pd
from filter_package import filter_fix, filter_industry
from assess_package import back_trader
from functions import *


wx.info("============================[update_zhiya_from_eastmoney]股权抵押数据==========================================")
update_zhiya_from_eastmoney(supplement= True)