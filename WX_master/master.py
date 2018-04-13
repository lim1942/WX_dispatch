#coding=utf-8
import time
import requests
from flask import Flask, jsonify, request, abort

from dbs.pg_db import PgSql
from dbs.redis_db import redis_task
from tools import get_current_timestamp
from dbs.redis_db_2 import get_task,push_result
from config import SECRET, SERVER_PORT, TO_SLAVE_COUNT, AES_URL

app = Flask(__name__)
PG = PgSql()
CURSOR = redis_task()


@app.errorhandler(404)
def not_found(error):
    """ handle 404 error"""

    msg = error.description
    return jsonify({
        'code': '404',
        'msg': 'not found' if not msg else msg,
        'timestamp': get_current_timestamp(),
    }), 200


@app.errorhandler(403)  #
def not_allowed(error):
    """handle 403 error"""

    msg = error.description
    return jsonify({
        'code': '403',
        'msg': 'unauthorized' if not msg else msg,
        'timestamp': get_current_timestamp(),
    }), 200


@app.errorhandler(500)  
def internal_error(error):
    """handle 500 error"""

    msg = error.description
    return jsonify({
        'code': '500',
        'msg': 'internal error' if not msg else msg,
        'timestamp': get_current_timestamp(),
    }), 200


@app.route('/', methods=['GET'])
def index():
    """url for test index"""

    return jsonify({
        'code': '1',
        'msg': 'success',
        'timestamp': get_current_timestamp(),
    }), 200


@app.route('/queue',methods=['GET', 'POST'])
def queue():
    """url for outer to put one task to db"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    task = request.get_json()
    if not isinstance(task,dict):
        abort(403,'task is wrong')

    task_id = task.get('task_id','')
    if not task_id:
        abort(403,'task task_id error')

    entrance = task.get('entrance','')
    if not isinstance(entrance,list):
        abort(403,'task enrance is null')

    person = task.get('person','')
    if not isinstance(person,dict):
        abort(403,'task person is error')

    r = CURSOR.save_task_to_redis(task)
    if not r:
        abort(500,'save task error')

    if r == 'duplicate':
        return jsonify({
            'code': '0',
            'msg': 'duplicate',
            'timestamp': get_current_timestamp()})

    if r == 'full':
        return jsonify({
            'code': '0',
            'msg': 'Failed,master is full,wait...',
            'timestamp': get_current_timestamp()})        

    return jsonify({
        'code': '1',
        'msg': 'success',
        'timestamp': get_current_timestamp()})


@app.route('/promote',methods=['GET','POST'])
def promote():
    """url for outer to put one task superior"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    task = request.get_json()
    if not isinstance(task,dict):
        abort(403,'task is wrong')

    task_id = task.get('task_id','')
    if not task_id:
        abort(403,'task task_id error')

    entrance = task.get('entrance','')
    if not isinstance(entrance,list):
        abort(403,'task enrance is null')

    person = task.get('person','')
    if not isinstance(person,dict):
        abort(403,'task person is error')

    r = CURSOR.save_task_to_redis_promote(task)
    if not r:
        abort(500,'save task error')

    if r == 'full':
        return jsonify({
            'code': '0',
            'msg': 'Failed,master is full,wait...',
            'timestamp': get_current_timestamp()})

    return jsonify({
        'code': '1',
        'msg': 'success',
        'timestamp': get_current_timestamp()})


@app.route('/queues',methods=['GET', 'POST'])
def queues():
    """url for outer to put multi tasks to redis"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    tasks = request.get_json()
    if not isinstance(tasks,list):
        abort(403,'tasks is wrong')

    r = CURSOR.save_tasks_to_redis(tasks)
    if not r:
        abort(500,'save task error')     

    if r == 'full':
        return jsonify({
            'code': '0',
            'msg': 'Failed,master is full,wait...',
            'timestamp': get_current_timestamp()})        

    return jsonify({
        'code': '1',
        'msg': 'success',
        'timestamp': get_current_timestamp()})


@app.route('/get_tasks',methods=['GET', 'POST'])
def get_tasks():
    """url for slave to get tasks to do"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    try:
        count = request.headers.get('count')
        if not isinstance(count,int):
            count = int(count)
    except:
        count = TO_SLAVE_COUNT
    tasks = CURSOR.get_task(count)
    return jsonify({
        'code': '1',
        'tasks':tasks ,
        'timestamp': get_current_timestamp()})


@app.route('/back_taskids',methods=['GET', 'POST'])
def back_taskids():
    """url for slave to feedback taskid,
        back to finish ser or fail set"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    task_ids = request.get_json()
    if not isinstance(task_ids,dict):
        abort(403,'results is wrong')
        
    task_ids_finish = task_ids['finish']
    r1 = CURSOR.task_finished(task_ids_finish)

    task_ids_failed = task_ids['failed']
    r2 = CURSOR.task_failed(task_ids_failed)

    if r1 or r2:
        return jsonify({
            'code': '1',
            'msg': 'success',
            'timestamp': get_current_timestamp()})
    else:
        return jsonify({
            'code': '0',
            'msg': 'failed',
            'timestamp': get_current_timestamp()})


@app.route('/save_data_to_guangzhou',methods=['GET', 'POST'])
def save_data_to_guangzhou():
    """url for slave to back results,
    insert data to pgsql table"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    datas = request.get_json()   
    if not isinstance(datas,list):
        abort(403,'bad data')

    state_of_insert = PG.insert_datas(datas)

    if state_of_insert:
        return jsonify({
            'code': '1',
            'msg': 'success',
            'timestamp': get_current_timestamp()})
    else:
        return jsonify({
            'code': '0',
            'msg': 'failed',
            'timestamp': get_current_timestamp()})


@app.route('/get_aes_from_guangzhou',methods=['GET', 'POST'])
def get_aes_from_guangzhou():
    """url for field get guangzhou aes jiami"""

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    field = request.headers.get('field', '')
    if not field:
        encrypt_ = {}
    try:
        field = field.encode('utf-8')
        url = AES_URL + field
        encrypt_ = requests.get(url).text
    except:
        encrypt_ = {}

    return jsonify({
        'code': '1',
        'field': encrypt_,
        'timestamp': get_current_timestamp()})



#>>>>>>>>>>>>>>>>>>>>>>>>>>> for qunkong phone
@app.route('/get_gc_tasks',methods=['GET', 'POST'])
def get_gc_tasks():

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    task_num = request.headers.get('task_num', '')
    if not task_num:
        abort(403, 'task_num')
    if not isinstance(task_num,int):
        task_num = int(task_num)

    tasks = get_task(task_num)
    return jsonify({
        'code': '1',
        'msg': tasks,
        'timestamp': get_current_timestamp()})


@app.route('/push_gc_tasks',methods=['GET', 'POST'])
def push_gc_tasks():

    secret = request.headers.get('secret', '')
    if not secret or secret != SECRET:
        abort(403, 'bad secret')

    resutls = request.get_json()
    try:
        push_result(results)
        return jsonify({
            'code': '1',
            'msg': 'success',
            'timestamp': get_current_timestamp()})

    except:
       return jsonify({
            'code': '0',
            'msg': 'failed',
            'timestamp': get_current_timestamp()})




if __name__ == '__main__':
    from gevent import pywsgi,monkey
    monkey.patch_all()
    
    print('starting server at 6789  ...')
    gevent_server = pywsgi.WSGIServer(('0.0.0.0',SERVER_PORT), app)
    gevent_server.serve_forever()
