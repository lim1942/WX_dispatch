# coding=utf-8
import re
import sys
import time
import json
import requests

import gevent

from gevent import monkey
from utils.value_formater import format_value_list
from tools import save_file, field_handle, aes_2, channel_2, get_logger

reload(sys)
sys.setdefaultencoding("utf-8")

script_log = get_logger('script')

monkey.patch_all()

RAW_FILE_PATH = ur""

CSRF_PATTERN = ur'name="CSRFToken" value="(.*?)"'

# RAW_FOLDER = ur"E:\资源\广东短号电话文件\2E组/"
RAW_FOLDER = ur"D:\data\temp_files\guangdong_shortphone/"
STORAGE_FOLDER = ur"D:\data/crawl_10086_short_number/"
UPLOAD_FOLDER = ur"D:\data1/batch_duanhao/"

save_file_name = "yidongduanhao"

MAIN_URL = u"http://gd.10086.cn/group/zd"
QUERY_URL = u"http://gd.10086.cn/group/myPreferentialController/getMemberCornetInformation.jspx"

file_item_header = "ec_number^ec_name^phone^short_phone^crawled_time"

HEADERS_MAIN = {
    "Host": "gd.10086.cn",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9"
}

HEADERS_QUERY = {
    "Host": "gd.10086.cn",
    "Connection": "keep-alive",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "http://gd.10086.cn",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Referer": "http://gd.10086.cn/group/zd",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9"
}

CRAWLED_PHONES = set()

timeout_ratio = 2


def get_current_time():
    return int(time.time() * 1000)


def get_current_datetime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def get_datetime(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def get_content(session, url, headers, payload=None, method="GET", cookies=None, to=10):
    error_times = 0
    while 1:
        if error_times > 5:
            script_log.warning("yidongduanhao error, connect timeout")
            return ""
        try:
            with gevent.Timeout(to, requests.Timeout):
                if method == "GET":
                    response = session.get(url, headers=headers, params=payload, verify=False, allow_redirects=False,
                                           cookies=cookies)
                else:
                    response = session.post(url, headers=headers, data=payload, verify=False, allow_redirects=False,
                                            cookies=cookies)
                response.encoding = "utf-8"

                if response.status_code == 200:
                    if '"resultStatus":"BUSY"' in response.text:
                        time.sleep(0.5)
                        continue

                    return response.text
                elif response.status_code in (302, 404, 500):
                    return ""
                else:
                    error_times += 1
        except Exception:
            error_times += 1

            continue


def process_one(result_dict, channel_no=None):
    raw_item = result_dict
    value_list = []

    if raw_item.get("data", {}) in (None, {}):
        return value_list

    if channel_no is None:
        try:
            value_list = [
                raw_item.get("data", {})["ecCode"],
                raw_item.get("data", {})["ecName"],
                raw_item.get("data", {})["phoneNumber"],
                raw_item.get("data", {})["cornet"],
                get_datetime(raw_item.get("data", {})["crawled_time"] / 1000.0),
            ]

        except Exception:
            pass
    elif channel_no == 2:
        try:
            value_list = [
                aes_2(field_handle(raw_item.get("data", {})["phoneNumber"])),
                field_handle(raw_item.get("data", {})["ecName"])
            ]
        except Exception:
            pass

    value_list = format_value_list(value_list)

    return value_list


def process_for_file(result_dict):
    value_lists = process_one(result_dict)

    return value_lists


def process_for_channel_2(result_dict):
    value_lists = process_one(result_dict, channel_no=2)

    return value_lists


def query_result(phone, to):
    # 重取一下cookies
    # 即一次性 session

    session = requests.Session()

    content = get_content(session, MAIN_URL, HEADERS_MAIN)
    cookies = session.cookies
    # 顺便提取csrf
    match = re.findall(CSRF_PATTERN, content)

    json_content = json.loads("{}")

    try:
        if len(match) > 0:
            csrf = match[0]

            payload = {
                "CSRFToken": csrf,
                "phoneNumber": phone
            }

            content = get_content(session, QUERY_URL, payload=payload, headers=HEADERS_QUERY, method="POST", cookies=cookies,
                                  to=to * timeout_ratio)

            error_time = 0

            if content != "":
                while 1:
                    try:
                        if u"机主未加入短号" not in content:
                            json_content = json.loads(content)
                        break
                    except:
                        error_time += 1
                        content = get_content(session, QUERY_URL, payload=payload, headers=HEADERS_QUERY, method="POST",
                                              cookies=cookies)

                        if error_time == 5:
                            return

                        continue

                if json_content.get("data", None) is not None:
                    json_content["data"]["crawled_time"] = get_current_time()
                else:
                    json_content["data"] = {
                        "phoneNumber": payload["phoneNumber"],
                        "crawled_time": get_current_time()
                    }
    except:
        pass

    return json_content


def query_wrap(task, to):
    try:
        phone = task.get("person", {}).get("phone", "")

        if phone in ("", None) or len(phone) != 11:
            return

        raw_result = query_result(phone, to)

        value_list = process_for_file(raw_result)
        if value_list:
            value_str = "^".join(value_list)

            save_file(filename=save_file_name, head=file_item_header, file=value_str)

        channel_value = process_for_channel_2(raw_result)
        if channel_value:
            channel_2(channel_value)
    except:
        script_log.warning("yidongduanhao crawled failed, task: {}".format(task))


def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'phone': u'13532457942'
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    # query_wrap(task, 10)


if __name__ == '__main__':
    main()
    # test()
