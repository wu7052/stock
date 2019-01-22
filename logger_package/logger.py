#! /usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Q1mi"

"""
logging配置
"""

import os
import logging.config
from datetime import datetime

class myLogger():
    def __init__(self, LogPath = None):

        # 定义三种日志输出格式 开始
        self.standard_format = '[%(asctime) -s][%(threadName)s:%(thread)d][task_id:%(name)s][%(filename)s:%(lineno)d]' \
                          '[%(levelname)s][%(message)s]'
        self.simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'
        self.id_simple_format = '[%(levelname)s][%(asctime)s] %(message)s'

        self.logfile_dir = os.path.dirname(os.path.abspath(LogPath)) + '\\log\\' # log文件的目录

        # 如果不存在定义的日志目录就创建一个
        if not os.path.isdir(self.logfile_dir):
            os.mkdir(self.logfile_dir)

        self.logfile_name = datetime.now().strftime('%Y-%m-%d')+ '.log'  # log文件名

        # log文件的全路径
        self.logfile_path = os.path.join(self.logfile_dir, self.logfile_name)

        # log配置字典
        self.LOGGING_DIC = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': self.standard_format,
                    'datefmt': '%Y-%m-%d %H:%M:%S',
                },
                'simple': {
                    'format': self.simple_format
                },
            },
            'filters': {},
            'handlers': {
                'console': {
                    'level': 'INFO',
                    'class': 'logging.StreamHandler',  # 打印到屏幕
                    'formatter': 'simple'
                },
                'default': {
                    'level': 'INFO',
                    'class': 'logging.handlers.RotatingFileHandler',  # 保存到文件
                    'filename': self.logfile_path,  # 日志文件
                    'maxBytes': 1024*1024*5,  # 日志大小 5M
                    'backupCount': 5,
                    'formatter': 'standard',
                    'encoding': 'utf-8',  # 日志文件的编码，再也不用担心中文log乱码了
                },
            },
            'loggers': {
                '': {
                    'handlers': ['default', 'console'],  # 这里把上面定义的两个handler都加上，即log数据既写入文件又打印到屏幕
                    'level': 'DEBUG',
                    'propagate': True,  # 向上（更高level的logger）传递
                },
            },
        }
        logging.config.dictConfig(self.LOGGING_DIC)  # 导入上面定义的配置
        self.wt = logging.getLogger(__name__)  # 生成一个log实例
        
# logger.info('It works!')  # 记录该文件的运行状态
# logger.debug('debug message')
# logger.info('info message')
# logger.warning('warning message')
# logger.error('error message')
# logger.critical('critical message')