# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-04-11 10:53:41
# @Last Modified by:  lim
# @Last Modified time:  2018-04-11 11:17:50
import time
from entrance import *
from config import ENTER_1,ENTER_2,ENTER_3, TIMEOUT


def test(task):

    entrance =  task['entrance']
    task['person'] = task['person']

    funcs =[]
    if '1' in entrance:
        for m in ENTER_1:
            func = eval(m+'.query_wrap')
            funcs.append(func)
    if '2' in entrance:
        for m in ENTER_2:
            func = eval(m+'.query_wrap')
            funcs.append(func)
    if '3' in entrance:
        for m in ENTER_3:
            func = eval(m+'.query_wrap')
            funcs.append(func)

    funcs = list(set(funcs))

    for func in funcs:
        a = time.time()
        func(task,TIMEOUT)
        b = time.time()
        print(func.__module__, '  use time {}'.format(b-a))


if __name__ == '__main__':
    task = {
    u'date': u'03',
    u'person':
        {
            u'idnum': u'440126196604295443',
            u'name': u'陈结玲'
        },
    u'entrance': u"[u'1', u'2', u'3']",
    u'retry': u'1',
    u'task_id': u'120103195506014210'
    }
    test(task)