#coding=utf-8

# worker 数量
WORKERS = 4
#服务端口
SERVER_PORT = 6789
#接口密码
SECRET = '123456789'
#slave 缺省的取任务数量
TO_SLAVE_COUNT = 500 
#任务最大重试次数
TASK_RETRY = 5 
#finsh set 清理的阀值
TASK_AMOUNT = 10000 
# redis任务容量(所有队列里的任务),
# 50万的任务，需600M内存。正常需要300M的内存，
# redis在持久化时需要额外的300M来确保子进程
# 写入rdb，确保有600M以上内存来容纳50万任务。
REDIS_AMOUNT = 500000
# clear_redis 循环的时间
LOOP_INTERVAL = 60
# clera_redis 清理TASK_DOING的天数
DOING_CLEAR = 1
#任务队列
TASK_TODO = 'wx_todo'
#正在做的任务
TASK_DOING = 'wx_doing'
#失败的任务
TASK_FAILED = 'wx_failed'
#完成的任务
TASK_FINISH = 'wx_finish'

#日志目录
LOG_DIR = 'log'
#日志大小M
LOG_SIZE = 20
# 日志文件备份数
LOG_BACKUP = 50

#广州aes加密的地址
AES_URL = ''

#微信公众号pg配置
PG_DB = 'guangzhou_db'
PG_USER = 'guangzhou_user'
PG_PWD = ''
PG_HOST = ''
PG_PORT = '5432'


#微信公众号redis配置
SENTINEL_MONITOR = False
# >>>>redis 单机模式
REDIS_HOST = '127.0.0.1'
REDIS_PORT = '7001'
REDIS_PWD = 'mujin258'
# >>>>哨兵模式，请配置下面哨兵地址，
# >>>>并设置 SENTINEL_MONITOR = True
REDIS_PWD = 'mujin258'
REDIS_MASTER_NAME = 'mymaster'
SENTINEL_LIST = [('192.168.0.10', 27001),
                 ('192.168.0.11', 27001),
                 ('192.168.0.12', 27001),]


#群控的redis配置
REDIS_HOST_2 = ''
REDIS_PORT_2 = '6379'
REDIS_PWD_2 = ''
TASK_QUEUE = 'groupcontrol_information_queue'
RESULT_QUEUE = 'groupcontrol_information_feedback'
REDIS_ENCODING = 'utf-8'



