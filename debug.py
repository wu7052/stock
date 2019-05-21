# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
# wx.info("this is new logger")

from report_package import ws_rp
from functions import *
# from conf import conf_handler

# update_sh_basic_info_2()
update_daily_data_from_ts(period = -55)