#!/lib/anaconda2/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2018-03-08 10:23:48

@author: Maxing
'''


import sys
import redis
import demjson
from config import REDIS_HOST_2
from config import REDIS_PORT_2
from config import REDIS_PWD_2
from config import TASK_QUEUE
from config import RESULT_QUEUE
from config import REDIS_ENCODING



def gen_conn_pool():
    pool = redis.ConnectionPool(host=REDIS_HOST_2, port=REDIS_PORT_2, password=REDIS_PWD_2)
    r = redis.Redis(connection_pool=pool)
    return r




# 按需取出一定数量任务
def get_task(task_num):
    r = gen_conn_pool()
    list_length = r.llen(TASK_QUEUE)
    print 'length:', list_length
    tasks = []
    new_tasks = []
    if list_length > 0:
        if list_length > task_num:
            pop_num = task_num
        else:
            pop_num = list_length
        for i in xrange(pop_num):
            _task = r.lpop(TASK_QUEUE)
            task = _task.decode(REDIS_ENCODING)
            tasks.append(demjson.decode(task))
        new_tasks = task_formatter(tasks)
    return new_tasks

# 反馈执行结果
def push_result(results):
    r = gen_conn_pool()
    for result in results:
        r.rpush(RESULT_QUEUE, result)

def task_formatter_gz(tasks):
    new_tasks = []

    def clean_str(para_str): return para_str.strip().replace(
        "(", "").replace(")", "").replace("u'", "").replace("'", "")
    
    for task in tasks:
        paras = task.split(', ')
        task_id = clean_str(paras[0])
        task_type = clean_str(paras[1])
        message = clean_str(paras[2])
        name = clean_str(paras[3])
        account = clean_str(paras[4])
        mobile = clean_str(paras[5])
        creat_time = clean_str(paras[7])
        bill_id = clean_str(paras[8])

        new_task = {
            'task_id': task_id,          # 任务ID
            'task_type': task_type,      # 任务类型
            'message': message,          # 消息内容
            'name': name,                # 姓名
            'account': account,          # 账号
            'mobile': mobile,            # 手机号
            'creat_time': creat_time,    # 创建时间
            'bill_id': bill_id           # 案号
        }
        new_tasks.append(new_task)
    return new_tasks



def task_formatter(tasks):
    new_tasks = []
    for task in tasks:
        task_id = task.get('taskId', '')
        task_type = task.get('taskType', '')
        message = task.get('message', '')
        name = task.get('sendeeName', '')
        account = task.get('sendeeAccountNum', '')
        mobile = task.get('sendeePhone', '')
        add_account = task.get('add_account', '')
        creat_time = task.get('timestap', '')
        qq_num = task.get('QQNum', '')
        sendee_email = task.get('sendeeEmail', '')
        bill_id = task.get('billID', '')


        new_task = {
            'task_id': task_id,             # 任务ID
            'task_type': task_type,         # 任务类型
            'message': message,             # 消息内容
            'name': name,                   # 姓名
            'account': account,             # 账号
            'mobile': mobile,               # 手机号
            'add_account': add_account,     # 发送微信/QQ消息时用的之前添加成功的账号
            'creat_time': creat_time,       # 创建时间
            'bill_id': bill_id,             # 案号
            'qq_num': qq_num,               # QQ号码
            'sendee_email': sendee_email    # 接收人邮箱
        }
        new_tasks.append(new_task)
    return new_tasks
