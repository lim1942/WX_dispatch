# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-03-28 11:28:18
# @Last Modified by:  lim
# @Last Modified time:  2018-04-12 14:47:27
import os
import time
import logging
from cloghandler import ConcurrentRotatingFileHandler

from config import LOG_DIR,LOG_SIZE,LOG_BACKUP


def get_current_timestamp():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def get_current_day():
    return time.strftime('%d', time.localtime(time.time()))


def get_log_file_name(fname, log_dir=LOG_DIR):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), log_dir, fname + '.log')


def ensure_dir(fname):
    if fname.endswith(os.sep):
        fname = fname.rstrip(os.sep)
    fname = os.path.dirname(fname)
    dirs = fname.split(os.sep)
    for i in range(len(dirs)):
        d = os.sep.join(dirs[:i + 1])
        if not os.path.isdir(d):
            try:
                os.mkdir(d)
            except Exception as e:
                pass


def get_logger(name):
    fname = get_log_file_name(name)
    ensure_dir(fname)
    api_logger = logging.getLogger(fname)
    rotating_handler = ConcurrentRotatingFileHandler(
        fname, maxBytes=LOG_SIZE * 1024 * 1024, backupCount=LOG_BACKUP)
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(funcName)-10s %(message)s')
    rotating_handler.setFormatter(formatter)
    api_logger.addHandler(rotating_handler)
    api_logger.setLevel(logging.DEBUG)
    return api_logger


def error_record(code):
    line = get_current_timestamp() + ": " + code
    with open('warning.txt','a') as f:
        f.write(line+'\n')


