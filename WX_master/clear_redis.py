# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-04-04 10:06:34
# @Last Modified by:  lim
# @Last Modified time:  2018-04-12 10:39:09
import time
from dbs.redis_db import redis_task
from config import LOOP_INTERVAL, DOING_CLEAR

CURSOR = redis_task()


def clear_redis():
    """func for trim redis finish & failed set."""
    record = 0
    tatoal = DOING_CLEAR*86400

    while True:
        time.sleep(LOOP_INTERVAL)

        CURSOR.trim_finish_set()
        CURSOR.trim_failed_set()

        record +=LOOP_INTERVAL
        if record >= tatoal:
            CURSOR.handle_bad_doing()
            record = 0


if __name__ == '__main__':
    clear_redis()
