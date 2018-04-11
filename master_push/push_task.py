#coding =utf-8
import time
import uuid
import codecs
import requests

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


URL = '' # aliyun test
# URL = '' # vps
headers = {}


def get_save_date():
    date = time.strftime("%Y-%m-%d %H:%M:%S")
    date = date +'  '
    return date


def test_index():
    """to test master is alive?"""
    a = time.time()
    r = requests.get(URL)
    print(r.text)
    b = time.time()
    print(b-a)


def write_record(line_index):
    with open('record.txt','w') as f:
        f.write(str(line_index))


def read_record():
    with open('record.txt') as f:
        line_index = int(f.read())
    return line_index


def generate(line_index):
    """a generate to get data line from file"""
    with open('dataset/data_set2/processed_id_name_mobile_no_duplicates.txt') as f:
        for i in xrange(line_index):
            f.readline()
        for i in f:
            yield i


def put_20000_tasks(put_times=300):
    """every time put 20000 tasks to master,
    config line_index to skin how many line in dataset"""
    url = URL +'queues'
    tasks = []
    #push times
    times = 0
    # task in tasks
    count = 0
    # record line index ,for next start
    # will continue from here
    line_index = read_record()
    print(get_save_date()+'start at line index {} ....'.format(line_index))

    for line in generate(line_index):
        con = line[:-1].split('^')
        idnum = con[0]
        name = con[1]
        phone = con[2]
        task_id = str(uuid.uuid1())
        task = {'task_id':task_id,
                "entrance":['1','2','3'],
                'person':{'name':name,'idnum':idnum,'phone':phone}
                }
        tasks.append(task);count +=1

        if count >= 20000:
            lens = len(tasks)
            print('\n'+ get_save_date()+'put {} tasks to {}'.format(lens,url))

            while True:
                try:
                    a = time.time()
                    r = requests.get(url,headers=headers,json=tasks)
                    if r.status_code == 200 and r.json()['msg'] == 'success':
                        b = time.time()
                        tasks = []; count=0; times += 1
                        line_index +=lens; write_record(line_index)
                        print(get_save_date()+'push success,spend {}s,wait 30s push next...'.format(b-a))
                        time.sleep(30)
                        break

                    elif r.status_code == 200 and 'wait' in r.content:
                        b = time.time()
                        print(get_save_date()+'spend {}s , master is full sleep 400s to push again'.format(b-a))                    
                        time.sleep(400)

                    else:
                        raise Exception('error')

                except Exception as e:
                    b = time.time()
                    print(get_save_date()+'spend {}s ,something wrong,wait 10s push again ....\n'.format(b-a),e)                      
                    time.sleep(10)

        if times>=put_times:
            break


if __name__ == '__main__':
   put_20000_tasks()
