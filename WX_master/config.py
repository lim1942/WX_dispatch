#coding=utf-8

# worker ����
WORKERS = 4
#����˿�
SERVER_PORT = 6789
#�ӿ�����
SECRET = '123456789'
#slave ȱʡ��ȡ��������
TO_SLAVE_COUNT = 500 
#����������Դ���
TASK_RETRY = 5 
#finsh set ����ķ�ֵ
TASK_AMOUNT = 10000 
# redis��������(���ж����������),
# 50���������600M�ڴ档������Ҫ300M���ڴ棬
# redis�ڳ־û�ʱ��Ҫ�����300M��ȷ���ӽ���
# д��rdb��ȷ����600M�����ڴ�������50������
REDIS_AMOUNT = 500000
# clear_redis ѭ����ʱ��
LOOP_INTERVAL = 60
# clera_redis ����TASK_DOING������
DOING_CLEAR = 1
#�������
TASK_TODO = 'wx_todo'
#������������
TASK_DOING = 'wx_doing'
#ʧ�ܵ�����
TASK_FAILED = 'wx_failed'
#��ɵ�����
TASK_FINISH = 'wx_finish'

#��־Ŀ¼
LOG_DIR = 'log'
#��־��СM
LOG_SIZE = 20
# ��־�ļ�������
LOG_BACKUP = 50

#����aes���ܵĵ�ַ
AES_URL = ''

#΢�Ź��ں�pg����
PG_DB = 'guangzhou_db'
PG_USER = 'guangzhou_user'
PG_PWD = ''
PG_HOST = ''
PG_PORT = '5432'


#΢�Ź��ں�redis����
SENTINEL_MONITOR = False
# >>>>redis ����ģʽ
REDIS_HOST = '127.0.0.1'
REDIS_PORT = '7001'
REDIS_PWD = 'mujin258'
# >>>>�ڱ�ģʽ�������������ڱ���ַ��
# >>>>������ SENTINEL_MONITOR = True
REDIS_PWD = 'mujin258'
REDIS_MASTER_NAME = 'mymaster'
SENTINEL_LIST = [('192.168.0.10', 27001),
                 ('192.168.0.11', 27001),
                 ('192.168.0.12', 27001),]


#Ⱥ�ص�redis����
REDIS_HOST_2 = ''
REDIS_PORT_2 = '6379'
REDIS_PWD_2 = ''
TASK_QUEUE = 'groupcontrol_information_queue'
RESULT_QUEUE = 'groupcontrol_information_feedback'
REDIS_ENCODING = 'utf-8'



