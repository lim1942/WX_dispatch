# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-03-01 13:57:13
# @Last Modified by:  lim
# @Last Modified time:  2018-04-11 15:05:56
import os

# ENV = 'local' 
# ENV = 'product'
ENV = 'test'

#==============================================

# 本地环境
if ENV == 'local':
    MASTER_IP = ''
    MASTER_PORT = '6789'
    SECRET = ''
# 测试环境
if ENV == 'test':
    MASTER_IP = ''
    MASTER_PORT = '6789'
    SECRET = ''


# 内部脚本超时时间
TIMEOUT = 10 
# 外部超时时间
OUT_TIMEOUT = 35
#并发任务量
MAX_TASK = 50
#文件存储目录     
SAVE_PATH = 'data'
if not os.path.exists(SAVE_PATH):
    os.mkdir(SAVE_PATH)

#每次从主机取任务的量
GET_TASKS_AMOUT = 500
#任务缓冲池的大小
TASK_POOL = 1000
#完成多少任务向主机汇报
FINISH_FEEDBACK = 500
#任务系统循环时间
LOOP_TIME = 2


#日志目录
LOG_DIR = 'log'
#日志大小M
LOG_SIZE = 20
# 日志文件备份数
LOG_BACKUP = 50


#对公众号进行分类
ENTER_1 = ['mifenka','fujiandianxin','zhaolianniuka',
            'hubeidianxin','zhongguodianxin',
            'guangdongyouxian','jiyouxiaozhushou']
ENTER_2 = ['yidongduanhao']
ENTER_3 = ['mifenka','fujiandianxin','zhongguoliantong']

