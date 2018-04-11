# coding=utf-8
import time
import json
import codecs
import gevent
import requests

from gevent import monkey
from utils.id_belong_mapping import ID_BELONG_MAPPING
from tools import save_file, channel_1, field_handle, aes_1, aes_2, get_logger
from utils.value_formater import is_valid_idnum, is_valid_name, get_all_valid_phones, format_value_list

monkey.patch_all()

requests.packages.urllib3.disable_warnings()

class_name = "jiyouxiaozhushou"

script_log = get_logger('script')

appid = "wx83fd00e39502c5a7"

headers = {
    "Host": "wx.dafysz.cn",
    "Connection": "keep-alive",
    "Content-Length": "92",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://wx.dafysz.cn",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat QBCore/3.43.691.400 QQBrowser/9.0.2524.400",
    "Referer": "https://wx.dafysz.cn/myDafy/manage/bind?redirect=&state=",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.8,en-us;q=0.6,en;q=0.5;q=0.4",
}

field_name_mapper = {
    u"": "",
}

query_url = "https://wx.dafysz.cn/wechat-web/bind/getPhoneForBind"
token_query_url = "http://180.121.134.143:7070/get?appid={}".format(appid)

raw_file_path = ur"D:\催收平台的爬取数据\微信公众号_既有小助手/raw_data.txt"
# raw_file_path = ur"D:\催收平台的爬取数据\微信公众号_既有小助手/raw_data_example.txt"
processed_file_path = ur"D:\催收平台的爬取数据\微信公众号_既有小助手/processed_data.txt"

save_file_name = "jiyouxiaozhushou"

worker_num = 10
crawl_num = 0

crawled_set = set()

file_item_header = "query_id_number^query_name^query_province^query_city^phone^person_id^crawled_time"

item_header = "phone^crawled_time"

timeout_ratio = 1


def fill_token(payload):
    content = get_content(url=token_query_url)
    if content != "":
        json_content = json.loads(content)

        if json_content.get("code", 504) == 200:
            token = json_content.get("data", {}).get("token", "")

            payload["token"] = token

    return payload


def get_content(url, to=10, headers=None, payload=None, method="GET"):
    error_times = 0

    while 1:
        if "180.121.134.143" not in url and payload is not None:
            payload = fill_token(payload)

        if error_times >= 5:
            if "180.121.134.143" in url:
                script_log.warning("jiyouxiaozhushou error, unable get token")
            else:
                script_log.warning("jiyouxiaozhushou error, connect timeout")
            return ""

        try:
            with gevent.Timeout(to, requests.Timeout):
                if method == "GET":
                    response = requests.get(url, headers=headers, params=payload, verify=False, allow_redirects=False)
                else:
                    response = requests.post(url, headers=headers, json=payload, verify=False, allow_redirects=False)
                response.encoding = "utf-8"

                if response.status_code in (302, 200):
                    return response.text
                elif response.status_code in (404, 500):
                    return ""
                else:
                    error_times += 1
                    time.sleep(0.5)
        except Exception:
            error_times += 1
            continue


def process_for_file(result_dict):
    value_lists = process_one(json.dumps(result_dict))

    # header
    # 查询时所用的身份证^查询时所用的姓名^查询时所用的省份^查询时所用的城市^订单代码^地址^宽带账号^宽带密码^爬取时间
    # query_id_number^query_name^query_province^query_city^code^address^account^password^crawled_time

    return [value_lists]


def process_for_channel_1(result_dict):
    phones = get_all_valid_phones(result_dict.get(u"phone", ""))

    item = result_dict

    value_lists = []

    for phone in phones:
        value_list = list([
            aes_1(field_handle(item.get("query_id_number", ""))),  # idnumber
            aes_2(field_handle(phone)),                     # c_phone
            u"本人",                                         # relation
            field_handle(item.get("query_name", "")),       # c_name
            "",                                              # comment
            "1",                                             # level
        ])

        value_list = format_value_list(value_list)

        value_lists.append(value_list)

    return value_lists


def process_one_with_dict(line):
    value_list = process_one(line)

    if value_list is not None:
        value_dict = dict(zip(item_header.split("^"), value_list))
    else:
        return ""

    return value_dict


def process_one(line):
    item = json.loads(line)

    phone = item.get(u"phone", "")
    if phone is None or phone.strip() == "":
        return None

    value_list = list()

    value_list.append(item.get("query_id_number"))
    value_list.append(item.get("query_name"))
    value_list.append(item.get("query_province"))
    value_list.append(item.get("query_city"))
    value_list.append(item.get("phone"))
    value_list.append(str(item.get("personId")))
    value_list.append(item.get("crawled_time"))

    value_list = format_value_list(value_list)

    return value_list


def process(file_path=None):
    if file_path is None:
        file_path = raw_file_path

    with codecs.open(file_path, 'r', encoding="utf-8") as read_file:
        for line in read_file:

            value_list = process_one(line)

            # header
            # 查询时身份证号^查询时姓名^查询时省份^查询时城市^电话^个人ID^爬取时间
            # query_id_number^query_name^query_province^query_city^phone^person_id^crawled_time
            if value_list is None:
                continue

            save_value_list(processed_file_path, value_list)


def query(idnum, name, province, city, to):
    result_dict = {
        "query_id_number": idnum,
        "query_name": name,
        "query_province": province,
        "query_city": city,
        "crawled_time": get_current_datetime()
    }

    payload = {
        "ident": idnum,
        "name": name,
    }

    if is_valid_idnum(idnum) is False or is_valid_name(name) is False:
        return result_dict

    content = get_content(query_url, headers=headers, payload=payload, to=to * timeout_ratio, method="POST")

    try:
        if content != "":
            if u"不存在" in content:
                return result_dict
            elif u"重新" in content:
                exit(0)

            json_content = json.loads(content)

            if json_content.get("status", "") == "success":
                result_dict.update({"phone": json_content.get("data", {}).get("phone", "")})
                result_dict.update({"personId": json_content.get("data", {}).get("personId", "")})
    except:
        pass

    return result_dict


def querry(task, to):
    id_number = task.get("person", {}).get("idnum", "")
    name = task.get("person", {}).get("name", "")

    try:
        short_id_number = id_number[:4]

        province, city = ID_BELONG_MAPPING[short_id_number]
    except:
        province, city = "", ""

    result_dict = query(id_number, name, province, city, to)

    # result = process_one_with_dict(json.dumps(result_dict))

    return result_dict


def get_current_datetime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def save_value_list(file_path, value_list, sep="^"):
    for i in xrange(len(value_list)):
        if value_list[i] is None:
            value_list[i] = ""
        if isinstance(value_list[i], int):
            value_list[i] = unicode(value_list[i])
        value_list[i] = value_list[i].replace("\r", "").replace("\n", "").replace("^", "|").strip()

    codecs.open(file_path, 'a', encoding="utf-8").write(sep.join(value_list) + "\n")


def save_item_with_json(file_path, item):
    codecs.open(file_path, 'a', encoding="utf-8").write(json.dumps(item, ensure_ascii=False) + "\n")


def query_wrap(task, to):
    try:
        raw_result = querry(task, to)

        value_lists = process_for_file(raw_result)
        for value_list in value_lists:
            if value_list:
                value_str = "^".join(value_list)

                save_file(filename=save_file_name, head=file_item_header, file=value_str)

        channel_value_list = process_for_channel_1(raw_result)
        for channel_value in channel_value_list:
            if channel_value:

                channel_1(channel_value)
    except:
        script_log.warning("jiyouxiaozhushou crawled failed, task: {}".format(task))

def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'idnum': u'110105198302190049',
                u'name': u'吴尘'
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    # query_wrap(task, 10)


if __name__ == '__main__':
    main()
