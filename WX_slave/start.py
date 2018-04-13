# -*- coding: utf-8 -*-

import os
import time
import json
import codecs
import requests
import gevent
from gevent import monkey,pool
monkey.patch_all()

import win32file
win32file._setmaxstdio(2048)

from entrance import *
# for data backup
from backup import file_handle
#for slave local
from config import ENTER_1,ENTER_2,ENTER_3, TIMEOUT 
#for dispatch task
from config import (MASTER_IP, MASTER_PORT, LOOP_TIME, MAX_TASK,
                GET_TASKS_AMOUT, TASK_POOL,SECRET,OUT_TIMEOUT)


from tools import get_logger,error_record, get_datetime
tasks_log = get_logger('tasks')
dispatch_log = get_logger('dispatch')
backup_log = get_logger('backup')

#任务池队列
Q_TASK = []
#向mater回馈成功的taskid队列
Q_FINISH = []
#向master回馈失败的taskid队列
Q_FAILED = []


def back_taskids():
    """feedback 1000+ task`s status to master"""

    global Q_FINISH,Q_FAILED
    
    Q_FINISH_length = len(Q_FINISH)
    Q_FAILED_length = len(Q_FAILED)

    print (get_datetime()+'   **** tasks finish {} ; tasks '
        'failed {} ****'.format(Q_FINISH_length,Q_FAILED_length))

    if  (Q_FAILED_length+Q_FINISH_length) < 1000:
        return
    try:
        headers = {'secret':SECRET}
        data = {'finish':Q_FINISH,'failed':Q_FAILED}
        url = 'http://'+MASTER_IP+':'+MASTER_PORT+'/back_taskids'
        r = requests.get(url, headers=headers,json=data)
        if r.status_code == 200 and r.json()['msg'] == 'success':
            msg = '  >>>>>>> success back finish:{} failed:{} taskid to '\
                'master...\n'.format(len(Q_FINISH),len(Q_FAILED))
            print(get_datetime()+msg); tasks_log.info(msg)
            Q_FINISH = [];Q_FAILED = []
        else:
            raise Exception('failed back_taskids')
    except Exception as e:
        error_record('101')
        tasks_log.error('101:error in back taskid: {}'.format(e.message))



def get_tasks():
    """func get tasks from master and put task to pool"""

    #if slave`s tasks pool is full,will not to requests
    if GET_TASKS_AMOUT > TASK_POOL - len(Q_TASK):
        return 

    try:
        url = 'http://'+MASTER_IP+':'+MASTER_PORT+'/get_tasks'
        headers = {'secret':SECRET,'count':str(GET_TASKS_AMOUT)}
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            tasks = r.json()
        else:
            raise Exception('error')
    except Exception as e:
        error_record('102')
        tasks_log.warning('102:error in get_tasks: {}'.format(e.message))
        return

    tasks = tasks.get('tasks',[])
    if len(tasks)>0:
        msg = '   success get {} tasks from master  <<<<<<<<<\n'.format(len(tasks))
        print(get_datetime()+msg); tasks_log.info(msg)        
        map(lambda task:Q_TASK.append(task),tasks)
    else:
        msg = '   Master is empty,sleep 120s to get again  <<<<<<<<<\n'
        print(get_datetime()+msg); tasks_log.error(msg)        
        time.sleep(120)



def doing(task):
    """func for query"""

    if not isinstance(task,dict):
        return
    task_id = task['task_id']
    try:
        #split task to g_task
        entrance =  task['entrance']
        task['person'] = eval(task['person'])

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
        g_tasks = [gevent.spawn(func,task,TIMEOUT) for func in funcs]

        # if time over 25s ,desert this task,and back to master for another try
        with gevent.Timeout(OUT_TIMEOUT, requests.Timeout):
            gevent.joinall(g_tasks)
            Q_FINISH.append(task_id)

    except Exception as e:
        dispatch_log.warning('105:{} is failed:{}'.format(task_id,e.message))
        Q_FAILED.append(task_id)


def loop_system():
    """loop system to ensure tasks system"""
    toogle = True

    while True:

        # for data backup
        # backup yestoday`s data, sleep 30s
        # ensure yestoday`s task is finished
        if time.strftime('%H') =='00':
            if toogle:
                time.sleep(30)
                r =  file_handle()
                if r:
                    backup_log.info('success do backup') 
                    toogle = False
                else:
                    error_record('104')
                    backup_log.warning('104:fail to backup,try again!!!') 
        if time.strftime("%H") == '01':
            toogle = True

        #for loop system
        try:
            get_tasks()
            back_taskids()
            print (get_datetime()+'  slave-tasks pool size {}'.format(len(Q_TASK)))
        except Exception as e:
            error_record('100')
            tasks_log.warning('100:tasks system error:{}'.format(e.message))
    
        time.sleep(LOOP_TIME)


def dispatch():

    #爬取并发数量
    POOL = pool.Pool(MAX_TASK)

    POOL.spawn(loop_system)
    print(get_datetime()+'  >>> start a coroutines loop_system <<<')
    print(get_datetime()+'  >>> start many coroutines to doning work <<<\n\n')
    dispatch_log.info('start a process to get tasks...')

    while  True:
        if len(Q_TASK):
            task = Q_TASK.pop()
            POOL.spawn(doing,task)
        else:
            time.sleep(5)
            print(get_datetime()+ '  Q_TASK is empty,nothing to do ............')
        time.sleep(0.01)


if __name__ == '__main__':
    dispatch()

 
