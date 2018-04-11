# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-04-04 10:06:34
# @Last Modified by:  lim
# @Last Modified time:  2018-04-11 11:40:16
import time
from tools import get_logger
from dbs.redis_db import redis_task
from config import LOOP_INTERVAL, DOING_CLEAR

CURSOR = redis_task()
clear_redis_log = get_logger('clear_redis')


def clear_redis():
    """func for trim redis finish & failed set."""
    record = 0
    tatoal = DOING_CLEAR*86400
    while True:
        time.sleep(LOOP_INTERVAL)
        try:
            record +=LOOP_INTERVAL
            CURSOR.trim_finish_set()
            CURSOR.trim_failed_set()
            if record >= tatoal:
                CURSOR.handle_bad_doing()
                record = 0
        except Exception as e:
            clear_redis_log.error('000:clear redis error{}'.format(e.message))
        else:
            clear_redis_log.info('success clear redis db')

if __name__ == '__main__':
    clear_redis()
