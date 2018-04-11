# coding=utf-8
import time
import json
import codecs
import gevent
import requests

from gevent import monkey
from tools import save_file, field_handle, channel_1, channel_3, aes_1, aes_2, get_logger
from utils.value_formater import is_valid_idnum, is_valid_phone, is_valid_name, get_all_valid_phones, format_value_list


script_log = get_logger('script')

monkey.patch_all()

class_name = "zhongguodianxin"

headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Length": "213",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "wapzt.189.cn",
    "Origin": "http://wapzt.189.cn",
    "Proxy-Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

field_name_mapper = {
}

query_url = u"http://wapzt.189.cn/map/wapJSON"

raw_file_path = ur"D:\催收平台的爬取数据\中国电信_订单查询/raw_data2.txt"
# raw_file_path = ur"D:\催收平台的爬取数据\中国电信_订单查询/raw_data_example.txt"
processed_file_path = ur"D:\催收平台的爬取数据\中国电信_订单查询/processed_data2.txt"

item_header = "order_id^order_type^order_status^item_name^item_real_price^item_current_price^order_create_time^order_name^order_province^order_city^order_number^consignee_name^consignee_phone^consignee_province^consignee_city^consignee_subdistrict^consignee_address^consignee_postcode^order_price^id_number^fail_reason^crawled_time"
file_item_header = "query_id_number^query_name^query_province^query_city^order_id^order_type^order_status^item_name^item_real_price^item_current_price^order_create_time^order_name^order_province^order_city^order_number^consignee_name^consignee_phone^consignee_province^consignee_city^consignee_subdistrict^consignee_address^consignee_postcode^order_price^id_number^fail_reason^crawled_time"

save_file_name = "zhongguodianxin"

specify_keyword = u""

worker_num = 5
crawl_num = 0
skip_num = 0

crawled_set = set()

timeout_ratio = 3


def get_content(url, to=10, payload=None, method="GET"):
    error_times = 0
    while 1:
        try:
            with gevent.Timeout(to, requests.Timeout):
                if method == "GET":
                    response = requests.get(url, headers=headers, params=payload, verify=False,
                                            allow_redirects=False, timeout=5)
                else:
                    response = requests.post(url, headers=headers, json=payload, verify=False,
                                             allow_redirects=False, timeout=5)
                response.encoding = "utf-8"

                # if response.status_code == 302:
                #     session.cookies.update(response.cookies)

                if u"正在抢救" in response.text:
                    error_times += 1

                    if error_times >= 5:
                        script_log.warning("zhongguodianxin error, connect timeout")
                        return response.text
                    else:
                        continue

                if response.status_code in (302, 200):
                    return response.text
                elif response.status_code in (404, 500):
                    return ""
                else:
                    time.sleep(0.5)
        except Exception:
            error_times += 1
            continue


def process_one_with_dict(line):
    value_lists = process_one(line)

    value_dicts = []

    for value_list in value_lists:
        value_dict = dict(zip(item_header.split("^"), value_list))

        # new_value_dict = {
        #     u"手机号": value_dict[u"手机号"]
        #     u"收件人手机号": value_dict[u"手机号"]
        #     u"手机号": value_dict[u"手机号"]
        # }

        value_dicts.append(value_dict)

    return value_dicts


def process_one(line, channel_no=None):
    items = json.loads(line)

    value_lists = []

    for item in items:

        if item.get("content", {}).get("responseContent", {}).get("totalCount", 0) == 0:
            return value_lists

        content_key = "content"
        content_count = 1
        while 1:
            if content_key in item.keys():
                orders = item.get(content_key, {}).get("responseContent", {}).get("orders")

                for order in orders:

                    order_id = order.get("mainOrder", {}).get("orderId", "")
                    order_type_name = order.get("mainOrder", {}).get("orderTypeName", "")
                    order_status_name = order.get("mainOrder", {}).get("orderStatusName", "")
                    order_item_name = order.get("subOrders", [{}])[0].get("salesProds", {})[0].get("name", "")
                    order_market_price = order.get("mainOrder", {}).get("marketPrice", "")
                    real_price = order.get("mainOrder", {}).get("realPrice", "")
                    order_create_time = order.get("mainOrder", {}).get("createTime", "")

                    detail = json.loads(order.get("detail", "{}"))
                    if detail is None:
                        detail = {}

                    response_content = detail.get("responseContent", {})
                    if response_content is None:
                        response_content = {}

                    order_info = response_content.get("orderInfo", {})
                    if order_info is None:
                        order_info = {}

                    consignee = order_info.get("consignee", {})
                    if consignee is None:
                        consignee = {}

                    order_province = response_content.get("provinceName", "")
                    order_city = response_content.get("cityName", "")
                    order_number = response_content.get("number", "")
                    order_name = response_content.get("custName", "")

                    if order_province is None:
                        order_province = ""

                    if order_city is None:
                        order_city = ""

                    consignee_name = consignee.get("userName", "")
                    consignee_phone = consignee.get("cusMobile", "")
                    consignee_province_name = consignee.get("provinceName", "")
                    consignee_city_name = consignee.get("cityName", "")
                    consignee_country_name = consignee.get("countyName", "")
                    consignee_address = consignee.get("address", "")
                    consignee_post_code = consignee.get("postCode", "")

                    if consignee_province_name is None:
                        consignee_province_name = ""

                    if consignee_city_name is None:
                        consignee_city_name = ""

                    order_price = order_info.get("orderPrice", "")
                    id_card_no = response_content.get("idcardno", "")
                    fail_reason = response_content.get("failReason", "")

                    if channel_no is None:

                        value_list = [
                            item.get("query_id_number", ""),
                            item.get("query_name", ""),
                            item.get("query_province", ""),
                            item.get("query_city", ""),

                            order_id,
                            order_type_name,
                            order_status_name,
                            order_item_name,
                            str(order_market_price),
                            str(real_price),
                            order_create_time,
                            order_name,
                            order_province,
                            order_city,
                            order_number,
                            consignee_name,
                            consignee_phone,
                            consignee_province_name,
                            consignee_city_name,
                            consignee_country_name,
                            consignee_address,
                            str(consignee_post_code),
                            str(order_price),
                            id_card_no,
                            fail_reason,

                            item.get("crawled_time", ""),
                        ]
                    elif channel_no == 1:
                        phones = []
                        phones.extend(get_all_valid_phones(order_number))
                        phones.extend(get_all_valid_phones(consignee_phone))

                        value_list = [
                            order_status_name,
                            item.get("query_id_number", ""),
                            item.get("query_name", ""),
                            phones
                        ]
                    elif channel_no == 2:
                        value_list = []
                    else:
                        phones = []
                        phones.extend(get_all_valid_phones(order_number))
                        phones.extend(get_all_valid_phones(consignee_phone))

                        address = "".join(
                            [consignee_province_name, consignee_city_name, consignee_country_name, consignee_address])

                        value_list = [
                            order_status_name,
                            item.get("query_id_number", ""),
                            item.get("query_name", ""),
                            phones,
                            address
                        ]

                    # header
                    # 查询时所用的身份证^查询时所用的姓名^查询时所用的省份^查询时所用的城市^订单ID^订单类型^订单状态^商品名字^商品实际价格^商品当前价格^订单创建时间^入网人姓名^入网手机号归属省^入网手机号归属市^入网手机号^收件人姓名^收件人手机号^收件人省地址^收件市地址^收件县地址^收件详细地址^邮政编码^订单价格^身份证号码^失败原因^爬取时间
                    # query_id_number^query_name^query_province^query_city^order_id^order_type^order_status^item_name^item_real_price^item_current_price^order_create_time^order_name^order_province^order_city^order_number^consignee_name^consignee_phone^consignee_province^consignee_city^consignee_subdistrict^consignee_address^consignee_postcode^order_price^id_number^fail_reason^crawled_time

                    # 订单ID^订单类型^订单状态^商品名字^商品实际价格^商品当前价格^订单创建时间^入网人姓名^入网手机号归属省^入网手机号归属市^入网手机号^收件人姓名^收件人手机号^收件人省地址^收件市地址^收件县地址^收件详细地址^邮政编码^订单价格^身份证号码^失败原因^爬取时间
                    # order_id^order_type^order_status^item_name^item_real_price^item_current_price^order_create_time^order_name^order_province^order_city^order_number^consignee_name^consignee_phone^consignee_province^consignee_city^consignee_subdistrict^consignee_address^consignee_postcode^order_price^id_number^fail_reason^crawled_time

                    value_lists.append(value_list)
                    # save_value_list(processed_file_path, value_list)

                content_key = "content" + str(content_count)

                content_count += 1
            else:
                break

    return value_lists


def query_by_phone(phone, name, to):
    return query(phone, name, to, is_phone=True)


def query_by_id_number(id_number, name, to):
    return query(id_number, name, to, is_phone=False)


def query(idnum_or_phone, name, to, is_phone=False):
    result_dict = {
        "query_id_number": "",
        "query_phone": "",
        "query_name": name,
        "crawled_time": get_current_datetime()
    }

    if is_phone:
        result_dict["query_phone"] = idnum_or_phone
        if is_valid_phone(idnum_or_phone) is False or is_valid_name(name) is False:
            return result_dict
    else:
        result_dict["query_id_number"] = idnum_or_phone
        if is_valid_idnum(idnum_or_phone) is False or is_valid_name(name) is False:
            return result_dict

    page_index = 1

    while 1:
        if is_phone:
            payload = {"headerInfo": {"functionCode": "qryOrderByAddress", "sessionId": ""},
                       "requestContent": {"type": "", "pageindex": page_index, "pagesize": "10",
                                          "starttime": "", "endtime": "",
                                          "consigneename": name,
                                          "consigneenumber": idnum_or_phone}}
        else:
            payload = {"headerInfo": {"functionCode": "qryOrderByReginfo", "sessionId": ""},
                       "requestContent": {"type": "", "pageindex": page_index, "pagesize": "10",
                                          "starttime": "", "endtime": "",
                                          "custname": name,
                                          "custcardno": idnum_or_phone}}

        content = get_content(url=query_url, payload=payload, to=timeout_ratio * to, method="POST")

        if content != "":
            json_content = json.loads(content)

            for i in xrange(len(json_content.get("responseContent", {}).get("orders", []))):
                order_id = json_content.get("responseContent", {}).get("orders", [])[i].get("mainOrder", {}).get(
                    "orderId", "")
                op_code = json_content.get("responseContent", {}).get("orders", [])[i].get("mainOrder", {}).get(
                    "opCode", "")

                if order_id == "":
                    continue

                # 抓订单详情
                if is_phone:
                    payload = {"headerInfo": {"functionCode": "cardPwdOrderDetail", "sessionId": ""},
                               "requestContent": {"orderid": order_id, "ordertype": "1",
                                                  "type": "bySH", "consigneename": name,
                                                  "consigneenumber": idnum_or_phone}}
                else:
                    payload = {"headerInfo": {"functionCode": "cardPwdOrderDetail", "sessionId": ""},
                               "requestContent": {"orderid": order_id, "ordertype": "1",
                                                  "type": "byRW", "custname": name,
                                                  "custcardno": idnum_or_phone}}

                content = get_content(url=query_url, payload=payload, method="POST")

                json_content.get("responseContent", {}).get("orders", [])[i].update({"detail": content})

                # 抓详细物流

                payload = {"headerInfo": {"functionCode": "logistics", "sessionId": ""},
                           "requestContent": {"orderid": order_id}}
                content = get_content(url=query_url, payload=payload, to=timeout_ratio * to, method="POST")
                json_content.get("responseContent", {}).get("orders", [])[i].update({"ship": content})

            if "content" not in result_dict:
                result_dict.update({"content": json_content})
            else:
                new_content_key = "content" + str(page_index)
                result_dict.update({new_content_key: json_content})

            if json_content.get("responseContent", {}).get("totalCount") > 10 * page_index:
                # 说明还有下一页
                page_index += 1
            else:
                break

    return result_dict


def process(file_path=None):
    if file_path is None:
        file_path = raw_file_path

    with codecs.open(file_path, 'r', encoding="utf-8") as read_file:

        for line in read_file:
            value_lists = process_one(line)

            if len(value_lists) == 0:
                continue

            for value_list in value_lists:
                save_value_list(processed_file_path, value_list)


def querry(task, to):
    id_number = task.get("person", {}).get("idnum", "")
    name = task.get("person", {}).get("name", "")
    phone = task.get("person", {}).get("phone", "")

    return_results = []

    if id_number is not None and id_number.strip() != "":
        result_dict = query_by_id_number(id_number, name, to)
        return_results.append(result_dict)

    if phone is not None and phone.strip() != "":
        result_dict = query_by_phone(phone, name, to)
        return_results.append(result_dict)

    return return_results


def get_current_datetime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def save_value_list(file_path, value_list, sep="^"):
    for i in xrange(len(value_list)):
        if value_list[i] is None:
            value_list[i] = ""
        else:
            value_list[i] = unicode(value_list[i])

        if value_list[i] == "null":
            value_list[i] = ""

        value_list[i] = value_list[i].replace("\r", "").replace("\n", "").replace("^", "|").strip()

    codecs.open(file_path, 'a', encoding="utf-8").write(sep.join(value_list) + "\n")


def save_item_with_json(file_path, item):
    codecs.open(file_path, 'a', encoding="utf-8").write(json.dumps(item, ensure_ascii=False) + "\n")


def process_for_file(result_dict):
    value_lists = process_one(json.dumps(result_dict))

    return value_lists


def process_for_channel_1(result_dict):
    raw_value_lists = process_one(json.dumps(result_dict), channel_no=1)

    value_lists = []

    for raw_value_list in raw_value_lists:
        if raw_value_list:
            if raw_value_list[0] == u"交易完成":
                id_number = field_handle(raw_value_list[1])
                name = field_handle(raw_value_list[2])

                for phone in raw_value_list[3]:
                    if phone:
                        value_list = list([
                            aes_1(field_handle(id_number)),  # idnumber
                            aes_2(field_handle(phone)),  # c_phone
                            u"本人",  # relation
                            field_handle(name),  # c_name
                            "",  # comment
                            "1",  # level
                        ])

                        value_lists.append(value_list)

    return value_lists


def process_for_channel_3(result_dict):
    raw_value_lists = process_one(json.dumps(result_dict), channel_no=3)

    value_lists = []

    for raw_value_list in raw_value_lists:
        if raw_value_list:
            if raw_value_list[0] == u"交易完成":
                id_number = field_handle(raw_value_list[1])
                name = field_handle(raw_value_list[2])

                try:
                    phone = field_handle(raw_value_list[3][0])
                except:
                    phone = ""

                address = raw_value_list[4]
                if address:
                    value_list = list([
                        aes_1(field_handle(id_number)),  # idnumber
                        u"本人",  # relation
                        "",  # comment
                        field_handle(name),  # c_name
                        aes_2(field_handle(phone)),  # c_phone
                        field_handle(u"居住地址"),  # type
                        aes_2(field_handle(address)),  # address
                        "1",  # level
                    ])

                    value_lists.append(value_list)

    return value_lists


def query_wrap(task, to):
    try:
        raw_result = querry(task, to)

        value_lists = process_for_file(raw_result)
        for value_list in value_lists:
            if value_list:
                value_list = format_value_list(value_list)

                value_str = "^".join(value_list)

                save_file(filename=save_file_name, head=file_item_header, file=value_str)

        channel_1_value_list = process_for_channel_1(raw_result)
        for channel_value in channel_1_value_list:
            if channel_value:

                channel_1(channel_value)

        channel_3_value_list = process_for_channel_3(raw_result)
        for channel_value in channel_3_value_list:
            if channel_value:

                channel_3(channel_value)
    except:
        script_log.warning("zhongguodianxin crawled failed, task: {}".format(task))


def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'idnum': u'',
                u'name': u'詹冠群',
                u'phone': u'13705775728',
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    # query_wrap(task, 10)


if __name__ == '__main__':
    main()
