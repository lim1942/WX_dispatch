#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# @Author: lim
# @Date:  2018-04-11 14:29:20
# @Last Modified by:  lim
# @Last Modified time:  2018-04-11 14:31:20

from __future__ import absolute_import
from commands import getstatusoutput

params = [
    "master:app",
    "clear_redis.py",
]
comms = ["""ps -ef | grep %s | grep -v grep | awk '{print $2}'""" % param for param in params]

pids = []
for comm in comms:
    ret, opt = getstatusoutput(comm)
    if ret == 0:
        if '\n' not in opt:
            pids.append(opt)
        else:
            pids.extend(opt.split('\n'))

kill_pid_comm = "kill -9 {}"
kill_pid_commands = [kill_pid_comm.format(pid) for pid in pids]

print "stopping services..."
for comm in kill_pid_commands:
    ret, opt = getstatusoutput(comm)
    if ret == 0:
        print "{} success".format(comm)
    else:
        print "{} failed".format(comm)
print "service stop success"