# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-03-01 13:42:07
# @Last Modified by:  lim
# @Last Modified time:  2018-04-11 12:34:37

import json
import requests
import traceback

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from tools import aes_1, aes_2, field_handle, save_file, channel_1, channel_3
from tools import get_logger
script_log = get_logger('script')

def query(person,to):

    idnum = person['idnum']
    name = person['name']
    
    headers ={
    "Host":"wapzt.189.cn",
    "Connection":"keep-alive",
    "Accept":"application/json, text/javascript, */*; q=0.01",
    "Origin":"http://wapzt.189.cn",
    "X-Requested-With":"XMLHttpRequest",
    "User-Agent":("Mozilla/5.0 (Linux; Android 4.4.4; 7 plus Build/KT"
        "U84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chr"
        "ome/33.0.0.0 Safari/537.36 MicroMessenger/6.6.2.1240(0x26060"
        "236) NetType/WIFI Language/zh_CN"),
    "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
    "Accept-Encoding":"gzip,deflate",
    "Accept-Language":"zh-CN,en-US;q=0.8",
    }
    data = {
    "headerInfo": {
     "functionCode": "qryOrderByReginfo",
     "sessionId": ""
     },
    "requestContent":{
    "type":"",
    "pageindex":"1",
    "pagesize":"10",
    "starttime":"",
    "endtime":"",
    "custname":name,
    "custcardno":idnum
    }}
    url = "http://wapzt.189.cn/map/wapJSON"

    retry = 3
    while  retry:
        try:
            r = requests.post(url,headers=headers,json=data,timeout=to)
            if r.status_code == 200:
                result = dict()
                if ('"responseContent":{"totalCount":0,"userId":"FSD88888",'
                    '"serviceCode":"0","' not in r.text and u'服务器君暂时晕倒' 
                    not in r.text and u'网络不给力啊' not in r.text):
                    res = json.loads(r.text)
                    con = res['responseContent']['orders'][0]['mainOrder']
                    # 订单状态
                    result['orderStatusName'] = con.get('orderStatusName','')
                    # 订单编号
                    result['orderId'] = con.get('orderId','')
                    # 订单金额
                    result['realPrice'] = con.get('realPrice','')
                    # 下单时间
                    result['createTime'] = con.get('createTime','')

                    data = {
                    "headerInfo": {
                     "functionCode": "cardPwdOrderDetail",
                     "sessionId": ""
                     },
                    "requestContent":{
                    "type":"byRW",
                    "ordertype":"1",
                    'orderid':result['orderId'],
                    "custname":name,
                    "custcardno":idnum
                    }}
                else:
                    return

                    r = requests.post(url, json=data,headers=headers,timeout=to)
                    if r.status_code == 200:
                        res2 = json.loads(r.text)
                        con = res2['responseContent']
                        # 订单商品
                        result['salesProName'] = con['orderInfo']['items'][0].get('salesProName','')
                        # 客户姓名
                        result['custName'] = con.get('custName','')
                        # 入网手机号
                        result['number'] = con.get('number','')
                        # 归属地
                        result['location'] = con.get('provinceName','')
                        #邮寄电话
                        result['cusMobile'] = con['orderInfo']['consignee'].get('cusMobile','')
                        # 省
                        result['provinceName'] = con['orderInfo']['consignee'].get('provinceName','')
                        # 市
                        result['cityName'] = con['orderInfo']['consignee'].get('cityName','')
                        # 区
                        result['countyName'] = con['orderInfo']['consignee'].get('countyName','')
                        # 地址
                        result['address'] = con['orderInfo']['consignee'].get('address','')

                        return result
                    else:
                        raise Exception('status_code error')
            else:
                raise Exception('status_code error')
            print retry
        except Exception as e:
            retry =retry -1
            script_log.warning('mifenka error,{}'.format(e.message))


def query_wrap(task,to):
    """wrap func"""
   
    person = task['person']
    if not person['idnum'] or not person['name']:
        return
    result = query(person, to)
    if not result: 
        return
    idnum = field_handle(person['idnum'])
    name = field_handle(person['name'])
    custName = field_handle(result.get('custName',''))
    cusMobile = field_handle(result.get('cusMobile',''))
    number = field_handle(result.get('number',''))
    orderId = field_handle(result.get('orderId',''))
    realPrice = field_handle(result.get('realPrice',''))
    salesProName = field_handle(result.get('salesProName',''))
    orderStatusName = field_handle(result.get('orderStatusName',''))
    location = field_handle(result.get('location',''))
    address = field_handle(result.get('address',''))
    countyName = field_handle(result.get('countyName',''))
    cityName = field_handle(result.get('cityName',''))
    provinceName = field_handle(result.get('provinceName',''))
    createTime = result.get('createTime','')
    

    # for lacal file
    filename = 'mifenka'
    head = ("idnum^name^custNamet^cusMobile^number^orderId^realPrice"
            "^salesProName^orderStatusName^location^address^countyName"
            "^cityName^provinceName^createTime")
    field_list = [idnum ,name ,custName ,cusMobile ,number ,
        orderId ,realPrice ,salesProName ,orderStatusName ,location
        ,address ,countyName ,cityName ,provinceName ,createTime]
    file = '^'.join(field_list)
    save_file(filename,file,head)

    #for pg channel 
    idnum = aes_1(idnum)
    cusMobile = aes_2(cusMobile)
    number = aes_2(number)
    address = aes_2(address)
    #channel 1
    field_list = [idnum,cusMobile,u'本人',name,'','1']
    channel_1(field_list)#第一个号码
    field_list = [idnum,number,u'本人',name,'','1']
    channel_1(field_list)#第二个号码
    #channel 3
    field_list = [idnum,u'本人','',name,cusMobile,u'居住地址',address,'1']
    channel_3(field_list)#第一个号码
    field_list = [idnum,u'本人','',name,number,u'居住地址',address,'1']
    channel_3(field_list)#第二个号码


if __name__ == '__main__':
    print(query({'name':'孙进舟','idnum':'110101193907030015'},10))
