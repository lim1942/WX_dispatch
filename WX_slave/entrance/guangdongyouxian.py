# coding=utf-8
import re
import time
import json
import codecs
import gevent
import requests

from gevent import monkey
from utils.value_formater import is_valid_idnum, is_valid_phone, format_value_list
from tools import save_file, channel_1, channel_3, aes_1, aes_2, field_handle, get_logger


script_log = get_logger('script')

monkey.patch_all()

class_name = "guangdongyouxian"

headers = {
    "Host": "www.96956.com.cn",
    "Proxy-Connection": "keep-alive",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "http://www.96956.com.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat QBCore/3.43.691.400 QQBrowser/9.0.2524.400",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "http://www.96956.com.cn/mcrweb/pages/hot/interactive/interactiveDetail.shtml?artid=14528&HDindex=2550",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
}

item_header = "addrs^cardno^custname^mobile^phone^crawled_time"
file_item_header = "query_id_number^query_name^addrs^cardno^custname^mobile^phone^crawled_time"

field_name_mapper = {
    u"": "",
}

query_url = "http://www.96956.com.cn/mcrapp/mcr/regist/register!queUserinfo"

appid = "wxb308ad712f6decea"
token_query_url = "http://180.121.134.143:7070/get?appid={}".format(appid)

raw_file_path = ur"D:\催收平台的爬取数据\微信公众号_广东有线/新建文本文档.txt"
# raw_file_path = ur"D:\催收平台的爬取数据\微信公众号_广东有线/raw_data_example.txt"
processed_file_path = ur"D:\催收平台的爬取数据\微信公众号_广东有线/processed_data1.txt"

save_file_name = "guangdongyouxian"
useless_text_pattern = ur"[\u4e00-\u9fa5、，。？/：（）\(\)\\\s]"

worker_num = 20
crawl_num = 0
skip_num = 0

crawled_set = set()
proxies = {'http': 'http://127.0.0.1:1080'}

timeout_ratio = 3


def fill_cookies():
    global headers

    content = get_content(url=token_query_url)
    if content != "":
        json_content = json.loads(content)

        if json_content.get("code", 504) == 200:
            cookies = json_content.get("data", {})

            headers["Cookie"] = "; ".join(["{k}={v}".format(k=k, v=v) for k, v in cookies.items()])


def get_content(url, to=10, payload=None, method="GET", is_return_text=True):
    error_times = 0
    while 1:
        if "180.121.134.143" not in url:
            fill_cookies()

        if error_times >= 5:
            if "180.121.134.143" in url:
                script_log.warning("guangdongyouxian error, unable get cookies")
            else:
                script_log.warning("guangdongyouxian error, connect time out")
            return ""

        try:
            with gevent.Timeout(to, requests.Timeout):
                if method == "GET":
                    response = requests.get(url, headers=headers, params=payload, verify=False)
                else:
                    response = requests.post(url, headers=headers, json=payload, verify=False)
                response.encoding = "utf-8"

                if u"获取地市信息出错" in response.text or u'系统繁忙' in response.text:
                    error_times += 1
                    continue

                if response.status_code == 200:
                    return response.text if is_return_text else response.content
                elif response.status_code in (404, 500):
                    return ""
                else:
                    error_times += 1
        except Exception:
            error_times += 1
            continue


def process_one_with_dict(line):
    value_lists = process_one(line)

    value_dicts = []

    for value_list in value_lists:
        value_dict = dict(zip(item_header.split("^"), value_list))
        value_dicts.append(value_dict)

    return value_dicts


def process_one(line, channel_no=None):
    value_lists = []
    item = json.loads(line)

    if item.get("result", "") in ("", None):
        return value_lists
    try:
        item["result"] = json.loads(item["result"])
    except:
        # print line
        return value_lists

    try:
        if item.get("result", {}) is None:
            return value_lists

        if item.get("result", {}).get("custInfoList", []) is None:
            return value_lists

        for custinfo in item.get("result", {}).get("custInfoList", []):

            # 对 mobile 和 phone 这两个字段进行处理

            mobile = custinfo.get(u"mobile", "")
            phone = custinfo.get(u"phone", "")
            if mobile is None:
                mobile = ""

            if phone is None:
                phone = ""

            mobile = re.sub(useless_text_pattern, ",", mobile)
            phone = re.sub(useless_text_pattern, ",", phone)

            mobile = re.sub(",+", ",", mobile)
            phone = re.sub(",+", ",", phone)

            mobile = set(mobile.split(","))
            phone = set(phone.split(","))

            if "" in phone:
                phone.remove("")

            if "" in mobile:
                mobile.remove("")

            mobile = ",".join(mobile)
            phone = ",".join(phone)

            if len(mobile) < 7:
                mobile = ""

            if len(phone) < 7:
                phone = ""

            phone = phone[phone.rfind("-") + 1:]

            phones = []

            phones.extend(phone.split(","))
            phones.extend(mobile.split(","))

            # 1. 把[中文|括号]都换成 ,
            # 2. 把 ,+ 都换成 ,
            #

            new_phones = set()

            for phone in phones:
                if phone and phone[-4:] != "0000":
                    new_phones.update([phone])

            phones = new_phones

            value_list = list()

            cust_name = custinfo.get("custname", "").replace(u"(已合并客户)", "").replace(u"(已合并)", "")

            addr_list = []
            if custinfo.get("addrList", []) is not None:
                for addr in custinfo.get("addrList", []):
                    temp_addr = addr.get("addr", "")
                    if temp_addr != "":
                        addr_list.append(temp_addr)

            if channel_no is None:

                value_list.append(item.get("query_id_number", ""))
                value_list.append(item.get("query_name", ""))

                value_list.append(",".join(addr_list))
                value_list.append(custinfo.get("cardno", ""))

                value_list.append(cust_name)
                value_list.append(mobile)
                value_list.append(phone)

                value_list.append(item.get("crawled_time", ""))
            elif channel_no == 1:
                if item.get("query_name", "") != "":
                    name = item.get("query_name", "")
                else:
                    name = cust_name

                value_list.append(item.get("query_id_number", ""))
                value_list.append(name)
                value_list.append(phones)

            elif channel_no == 2:
                pass
            else:
                if item.get("query_name", "") != "":
                    name = item.get("query_name", "")
                else:
                    name = cust_name

                value_list.append(item.get("query_id_number", ""))
                value_list.append(name)
                value_list.append(phone)

                value_list.append(addr_list)

            value_lists.append(value_list)

    except Exception:
        pass

    return value_lists


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


def process_for_file(result_dict):
    value_lists = process_one(json.dumps(result_dict))

    return value_lists


def process_for_channel_1(result_dict):
    raw_value_lists = process_one(json.dumps(result_dict), channel_no=1)

    value_lists = []

    for raw_value_list in raw_value_lists:
        if raw_value_list:
            id_number = field_handle(raw_value_list[0])
            name = field_handle(raw_value_list[1])

            for phone in raw_value_list[2]:
                if phone:
                    value_list = list([
                        aes_1(field_handle(id_number)),  # idnumber
                        aes_2(field_handle(phone)),  # c_phone
                        u"本人",  # relation
                        field_handle(name),  # c_name
                        "",  # comment
                        "1",  # level
                    ])

                    value_list = format_value_list(value_list)

                    value_lists.append(value_list)

    return value_lists


def process_for_channel_3(result_dict):
    raw_value_lists = process_one(json.dumps(result_dict), channel_no=3)

    value_lists = []

    for raw_value_list in raw_value_lists:
        if raw_value_list:
            id_number = field_handle(raw_value_list[0])
            name = field_handle(raw_value_list[1])
            try:
                phone = field_handle(raw_value_list[2][0])
            except:
                phone = ""

            for address in raw_value_list[3]:
                if address:
                    value_list = list([
                        aes_1(field_handle(id_number)),  # idnumber
                        u"本人",  # relation
                        "",  # comment
                        field_handle(name),  # c_name
                        aes_2(field_handle(phone)),  # c_phone
                        field_handle(u"家庭地址"),  # type
                        aes_2(field_handle(address)),  # address
                        "1",  # level
                    ])

                    value_list = format_value_list(value_list)

                    value_lists.append(value_list)

    return value_lists


def query_by_phone(phone, name, to):
    return query(phone, name, to, is_phone=True)


def query_by_id_number(id_number, name, to):
    return query(id_number, name, to, is_phone=False)


def query(id_number_or_phone, name, to=10, is_phone=False):
    if is_phone:
        payload = {"regInput": {"identifierType": "1", "identifier": id_number_or_phone}}

        result_dict = {
            "query_id_number": "",
            "query_name": name,
            "query_phone": id_number_or_phone,
            "crawled_time": get_current_datetime()
        }
    else:
        payload = {"regInput": {"identifierType": "0", "identifier": id_number_or_phone}}

        result_dict = {
            "query_id_number": id_number_or_phone,
            "query_name": name,
            "query_phone": "",
            "crawled_time": get_current_datetime()
        }

    if is_phone:
        if is_valid_phone(id_number_or_phone) is False:
            return result_dict
    else:
        if is_valid_idnum(id_number_or_phone) is False:
            return result_dict

    content = get_content(query_url, payload=payload, method="POST", to=to * timeout_ratio)

    if content != "":
        if u"根据查询条件查询不到客户信息" in content or u"客户端合法性验证出错：" in content:
            return result_dict

        result_dict.update({"result": content})

    return result_dict


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


def remove_useless_data():
    save_file = codecs.open(raw_file_path + "1", 'w', encoding="utf-8")

    with codecs.open(raw_file_path, 'r', encoding="utf-8") as read_file:
        for line in read_file:
            if u"获取地市信息出错" not in line and u"系统繁忙" not in line:
                save_file.write(line)


def get_current_datetime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def save_value_list(file_path, value_list, sep="^"):
    for i in xrange(len(value_list)):
        if value_list[i] is None:
            value_list[i] = ""
        value_list[i] = value_list[i].replace("\r", "").replace("\n", "").replace("^", "|").strip()

    codecs.open(file_path, 'a', encoding="utf-8").write(sep.join(value_list) + "\n")


def save_item_with_json(file_path, item):
    codecs.open(file_path, 'a', encoding="utf-8").write(json.dumps(item, ensure_ascii=False) + "\n")


def query_wrap(task, to):
    try:
        raw_result_s = querry(task, to)

        for raw_result in raw_result_s:
            value_lists = process_for_file(raw_result)
            for value_list in value_lists:
                if value_list:
                    value_list = format_value_list(value_list)
                    value_str = "^".join(value_list)

                    save_file(filename=save_file_name, head=file_item_header, file=value_str)

            channel_processed_set = set()
            channel_1_value_list = process_for_channel_1(raw_result)
            for channel_value in channel_1_value_list:
                if channel_value:
                    value_str = "^".join(channel_value)

                    if hash(value_str) not in channel_processed_set:
                        channel_processed_set.update([hash(value_str)])

                        channel_1(channel_value)

            channel_3_value_list = process_for_channel_3(raw_result)
            for channel_value in channel_3_value_list:
                if channel_value:
                    channel_3(channel_value)
    except:
        script_log.warning("guangdongyouxian crawled failed, task: {}".format(task))


def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'idnum': u'440126196604295443',
                u'name': u'陈结玲'
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    query_wrap(task, 10)


if __name__ == '__main__':
    main()
