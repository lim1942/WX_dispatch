#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from config import WORKERS
from config import SERVER_PORT
from os import system


MASTER_JOBS = [
    """nohup gunicorn -w {} -k gevent -b 0.0.0.0:{} master:app --access-logfile log/web_access.log >>log/gunicorn.log 2>>log/gunicorn.err.log &""".format(
        WORKERS, SERVER_PORT),
    """nohup python clear_redis.py >/dev/null 2>&1 &""",
]

print "booting master services..."
for JOB in MASTER_JOBS:
    try:
        print ("boot command: {}".format(JOB))
        system(JOB)
    except Exception as e:
        print("{}:\n\tboot failed: {}".format(JOB, e.message))
