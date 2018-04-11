#coding=utf-8
import redis
from redis.sentinel import Sentinel


MASTER = None
SLAVE = None
SENTINEL = None
TASK_RETRY = 5
TASK_AMOUNT = 2500
TASK_TODO = 'wx_todo'
TASK_DOING = 'wx_doing'
TASK_FAILED = 'wx_failed'
TASK_FINISH = 'wx_finish'
REDIS_MASTER_NAME = 'mymaster'
SENTINEL_LIST = [('127.0.0.1', 27001),
                 ('127.0.0.1', 27002),
                 ('127.0.0.1', 27003),]


def get_sentinel(refresh=False):
    """get a sentinel connection object"""
    global SENTINEL
    if refresh :
        SENTINEL = None
    if not SENTINEL:
        try:
            SENTINEL = Sentinel(
                SENTINEL_LIST,
                # password=REDIS_AUTH,
                decode_responses=True,
                socket_timeout=0.1)
            assert SENTINEL.discover_master(REDIS_MASTER_NAME)
            assert SENTINEL.discover_slaves(REDIS_MASTER_NAME)
        except Exception as e:
            print(e)
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
                print(e)
    return MASTER


def get_slave(refresh=False):
    """get a slave instance"""
    global SENTINEL, SLAVE
    if refresh:
        SLAVE = None
    if not SLAVE :
        sentinel = get_sentinel()
        if sentinel:
            try:
                SLAVE = sentinel.slave_for(REDIS_MASTER_NAME)
            except Exception as e:
                print(e)
    return SLAVE


def get_master_and_slave(refresh=False):
    """get redis master and redis slave instance"""
    return get_master(refresh), get_slave(refresh)


def save_task_to_redis(task):
    """save task to todo list"""
    try:
        task_id = task['task_id']
        if get_slave().keys(task_id):
            return False
        get_master().rpush(TASK_TODO, task_id)
        get_master().hmset(task_id, task)
    except Exception as e:
        print (e)
        return False
    return True


def save_task_to_redis_promote(task):
    try:
        task_id = task['task_id']
        if get_slave().sismember(TASK_DOING, task_id) or \
            get_slave().sismember(TASK_FINISH, task_id) or \
            get_slave().sismember(TASK_FAILED, task_id):
            return 'duplicate'
        if get_master().lrem(TASK_TODO,task_id):
            get_master().lpush(TASK_TODO,task_id)
        else:
            get_master().lpush(TASK_TODO,task_id)
            get_master().hmset(task_id, task)
        return True
    except Exception as e:
        print(e)
        return False


def  task_is_available(task_id):
    try:
        retry = get_slave().hget(task_id, 'retry')
        if not isinstance(retry,int):
            retry = int(retry)
        if retry < TASK_RETRY: 
            return True
        get_master().delete(task_id)
    except Exception as e:
        print(e)
    return False


def trim_finsh_set():
    try:
        count = get_slave().scard(TASK_FINISH)
        if count <= TASK_AMOUNT:
            return True
        need_clear = count - TASK_AMOUNT
        clear_list = [get_master().spop(TASK_FINISH) for i in xrange(need_clear)]
        map(get_master().delete, clear_list)
        return True
    except Exception as e:
        print(e)


def back_to_todo(task_id):
    try:
        get_master().rpush(TASK_TODO, task_id)
    except Exception as e:
        print(e)


def trim_failed_set():
    try:
        count = get_slave().scard(TASK_FAILED)
        failed_list = [get_master().spop(TASK_FAILED) for i in xrange(count)]
        map(back_to_todo,filter(task_is_available, failed_list))
        return True
    except Exception as e:
        print(e)



#------for slave

def get_task():
    try:
        task_id = get_master().lpop(TASK_TODO)
        if not task_id:
            return
        get_master().hincrby(task_id, 'retry', amount=1)
        get_master().sadd(TASK_DOING,task_id)
        task = get_master().hgetall(task_id)
        return task
    except Exception as e:
        print (e)


def task_finished(task_id):
    try:
        get_master().smove(TASK_DOING, TASK_FINISH, task_id)
        return True 
    except Exception as e:
        print(e)


def task_failed(task_id):
    try:
        get_master().smove(TASK_DOING, TASK_FAILED, task_id)
        return True 
    except Exception as e:
        print(e)


# if __name__ == '__main__':
        
    # save_task_to_redis({'task_id':'123456789','retry':0})
    # print get_task()

    # task_finished('2333232')
    # task_failed('123456789')
    # trim_failed_set()
