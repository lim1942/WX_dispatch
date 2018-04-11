 #coding=utf-8
import redis
import traceback
from redis.sentinel import Sentinel

from tools import get_current_day , get_logger, error_record
from config import *

redis_db_log = get_logger('redis_db')
clear_redis_log = get_logger('clear_redis')

SENTINEL = None
MASTER = None


def get_single_redis_conn():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT,db=0,password=REDIS_PWD)


def get_sentinel_redis_conn():
    return get_master()


def get_conn():
    """return a redis connect object"""
    
    # default False
    if SENTINEL_MONITOR:
        conn = get_sentinel_redis_conn()
    else:
        conn = get_single_redis_conn()
    return conn


class redis_task(object):


    def __init__(self):
        self.R = get_conn()
        self.P = self.R.pipeline(transaction=False)
        self.test = self.connection_test()


    def connection_test(self):
        try:
            self.R.set('test','test')
        except:
            error_record('101')
            redis_db_log.warning('101:Can not establish a connection to local redis DB')


    def save_task_to_redis(self,task):
        """save task to todo list"""
        if self.R.dbsize() >= REDIS_AMOUNT:
            return 'full'
        try:
            task_id = task['task_id']
            if self.R.keys(task_id):
                return 'duplicate'             
            task['retry'] = 0
            self.R.rpush(TASK_TODO, task_id)
            self.R.hmset(task_id, task)
        except:
            redis_db_log.warning('102:Save one task error')
            return False
        return True


    def save_tasks_to_redis(self,tasks):
        """save tasks to todo list"""
        if self.R.dbsize() >= REDIS_AMOUNT:
            return 'full'        
        try:
            for task in tasks:
                task_id = task['task_id']
                task['retry'] = 0
                self.P.rpush(TASK_TODO, task_id)
                self.P.hmset(task_id, task)
            self.P.execute()
            return True
        except Exception as e:
            error_record('103')
            redis_db_log.warning('103:Save many tasks error:{}'.format(e.message))


    def save_task_to_redis_promote(self,task):
        """save task to todo by promote channel"""
        if self.R.dbsize() >= REDIS_AMOUNT:
            return 'full'        
        try:
            task_id = task['task_id']
            if self.R.sismember(TASK_DOING, task_id) or \
                self.R.sismember(TASK_FINISH, task_id) or \
                self.R.sismember(TASK_FAILED, task_id):
                return True
            if self.R.lrem(TASK_TODO,task_id):
                self.R.lpush(TASK_TODO,task_id)
            else:
                self.R.lpush(TASK_TODO,task_id)
                task['retry'] = 0
                self.R.hmset(task_id, task)
            return True
        except Exception as e:
            redis_db_log.warning('104:Save one task promote error:{}'.format(e.message))
            return False


    def get_task(self,count):
        """get tasks to do"""
        def is_dict(item):
            if isinstance(item,dict):
                return True
        try:
            [self.P.lpop(TASK_TODO) for x in xrange(count)]
            task_ids = self.P.execute()
            for task_id in task_ids:
                if not task_id:
                    continue
                self.P.hincrby(task_id, 'retry', amount=1)
                self.P.hset(task_id,'date',get_current_day())
                self.P.sadd(TASK_DOING,task_id)
                self.P.hgetall(task_id)                
            tasks = filter(is_dict,self.P.execute())
            redis_db_log.info('success get {} tasks'.format(len(tasks)))
            return tasks
        except Exception as e:
            error_record('105')
            redis_db_log.warning('105:get tasks error:{}'.format(e.message))
            return []


    def task_finished(self,task_ids):
        """move task from doing to finished save task detail"""
        try:
            for task_id in task_ids:
                self.P.smove(TASK_DOING, TASK_FINISH, task_id)
            self.P.execute()
            redis_db_log.info('finish back {} tasks'.format(len(task_ids)))
            return True 
        except Exception as e:
            error_record('106')
            redis_db_log.warning('106:task finished error:{}'.format(e.message))


    def task_failed(self,task_id):
        """move task from doing to failed"""
        try:
            self.R.smove(TASK_DOING, TASK_FAILED, task_id)
            return True 
        except Exception as e:
            error_record('107')
            redis_db_log.warning('107:task failed error :{}'.format(e.message))



    def trim_finish_set(self):
        """trim finish set to empty"""
        try:
            count = self.R.scard(TASK_FINISH)
            if count <= TASK_AMOUNT:
                return True
            need_clear = count - TASK_AMOUNT
            clear_list = [self.R.spop(TASK_FINISH) for i in xrange(need_clear)]
            map(self.P.delete, clear_list)
            self.P.execute()
            clear_redis_log.info('success clear finish set,count:{}'.format(need_clear))
            return True
        except Exception as e:
            error_record('001')
            clear_redis_log.warning('001:trim_finsh_set error :{}'.format(e.message))


    def task_is_available(self,task_id):
        """Judge,if task is available return True,
           else del this task for db1"""
        try:
            retry = self.R.hget(task_id, 'retry')
            if not isinstance(retry,int):
                retry = int(retry)
            if retry < TASK_RETRY: 
                return True
            self.R.delete(task_id)
        except Exception as e:
            clear_redis_log.warning('004:task_is_available error :{}'.format(e.message))
        return False


    def back_task_to_todo(self,task_id):
        """back failed task to todo list"""
        try:
            self.R.rpush(TASK_TODO, task_id)
        except Exception as e:
            clear_redis_log.warning('005:back_task_to_todo error  :{}'.format(e.message))


    def trim_failed_set(self):
        """trim failed set and back task to todo list"""
        try:
            count = self.R.scard(TASK_FAILED)
            failed_list = [self.R.spop(TASK_FAILED) for i in xrange(count)]
            back_list = filter(self.task_is_available, failed_list)
            map(self.back_task_to_todo,back_list)
            clear_redis_log.info('success clear failed set,'
                'total:{},back{}'.format(count,len(back_list)))
            return True
        except Exception as e:
            error_record('002')
            clear_redis_log.warning('002:trim_failed_set error :{}'.format(e.message))


    def handle_bad_doing(self):
        """if task stay in doing-set over 1 day 
        back it to todo-list for another try"""
        try:
            count = 0
            doing_list = self.R.smembers(TASK_DOING)
            for task_id in doing_list:
                date = self.R.hget(task_id, 'date')
                if not isinstance(date,int):
                    date = int(date)
                if abs(date - int(get_current_day())) >0 :
                    count +=1
                    self.R.srem(TASK_DOING,task_id)
                    self.back_task_to_todo(task_id)
            clear_redis_log.info('success handle doing set,'
                'back count:{}'.format(count))
            return True
        except Exception as e:
            error_record('003')
            clear_redis_log.warning('003:handle_bad_doing error :{}'.format(e.message))



def get_sentinel(refresh=False):
    """get a sentinel connection object"""
    global SENTINEL
    if refresh :
        SENTINEL = None
    if not SENTINEL:
        try:
            SENTINEL = Sentinel(
                SENTINEL_LIST,
                password=REDIS_PWD,
                decode_responses=True,
                socket_timeout=0.1)
            assert SENTINEL.discover_master(REDIS_MASTER_NAME)
            assert SENTINEL.discover_slaves(REDIS_MASTER_NAME)
        except Exception as e:
            error_record('101')
            redis_db_log.warning('101:Can not establish a connection to redis DB'.format(e.message))            
    return SENTINEL


def get_master(refresh=False):
    """get a master instance"""
    global SENTINEL, MASTER
    if refresh :
        MASTER = None
    if not MASTER :
        sentinel = get_sentinel()
        if sentinel:
            try:
                MASTER = sentinel.master_for(REDIS_MASTER_NAME)
            except Exception as e:
                error_record('101')
                redis_db_log.warning('101:Can not establish a connection to redis DB'.format(e.message))  
    return MASTER



if __name__ == '__main__':
    c = redis_task()
    print c.trim_finish_set()
    print c.trim_failed_set()
    print c.handle_bad_doing()

