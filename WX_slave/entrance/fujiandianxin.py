#coding=utf-8
import json
import requests
import traceback

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from tools import aes_1, aes_2, field_handle, save_file, channel_3
from tools import get_logger
script_log = get_logger('script')

def query(person,to):

    idnum = person['idnum']
    name = person['name']

    headers={
    "Host":"wt.fj.189.cn",
    "Connection":"keep-alive",
    "Accept":"application/json, text/plain, */*",
    "Origin":"http://wt.fj.189.cn",
    "User-Agent":("Mozilla/5.0 (Linux; Android 4.4.4; 7 plus Build/KTU84P)"
        " AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0."
        "0 Safari/537.36 MicroMessenger/6.6.2.1240(0x26060236) NetType/WIFI"
        " Language/zh_CN"),
    "Content-Type":"application/json;charset=UTF-8",
    "Accept-Encoding":"gzip,deflate",
    "Accept-Language":"zh-CN,en-US;q=0.8",
    "X-Requested-With":"com.tencent.mm",
    }

    data = {"areaName":"福州","name":name,"certNum":idnum}
    url = "http://wt.fj.189.cn/custwx/bindController/qryBroadLists"

    retry = 3
    while retry:
        try:
            r = requests.post(url,headers=headers,json=data)
            if r.status_code == 200:
                if u'没有找到您的宽带信息' not in r.text and u"返回内容为空" not in r.text:
                    result = dict()
                    res = json.loads(r.text)
                    result["address"] = res["object"][0].get("address",'')
                    result["account"] = res["object"][0].get("account",'')
                    return result
                else:
                    return
            else:
                raise Exception('status_code error')
        except Exception as e:
            retry -= 1
            script_log.warning('fujiandianxin error,{}'.format(e.message))


def query_wrap(task,to):
    """wrap func"""
    person = task['person']
    if not person['idnum'] or not person['name']:
        return
    result = query(person, to)
    if not result: 
        return
    idnum = field_handle(person.get('idnum',''))
    name = field_handle(person.get('name',''))
    account = field_handle(result.get('account',''))
    address = field_handle(result.get('address',''))

    # for lacal file
    filename = 'fujiandianxin'
    head = 'idnum^name^account^address'
    field_list = [idnum,name,account,address]
    file = '^'.join(field_list)
    save_file(filename,file,head)

    #for pg channel
    idnum = aes_1(idnum)
    address = aes_2(address)
    field_list = [idnum,u'本人','',name,'',u'居住地址',address,'1']
    channel_3(field_list)


    
if __name__ == '__main__':
    print (query({'name':'陈海疆','idnum':'110108197404076356'},10))
