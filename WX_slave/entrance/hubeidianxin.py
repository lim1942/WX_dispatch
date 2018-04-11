# coding=utf-8
import time
import json
import codecs
import gevent
import requests

from gevent import monkey
from utils.id_belong_mapping import ID_BELONG_MAPPING
from utils.value_formater import is_valid_idnum, get_all_valid_phones, format_value_list
from tools import save_file, channel_1, field_handle, aes_1, aes_2, get_logger


script_log = get_logger('script')

monkey.patch_all()

class_name = "hubeidianxin"

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "12333.jingmen.gov.cn",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36"
}

field_name_mapper = {
}

install_query_url = u"http://w.02786310000.cn/Wap/InstallSelect/selectAction"
kuandai_query_url = u"http://w.02786310000.cn/Wap/KdinfoCode/selectkdinfo"

# raw_file_path = ur"D:\催收平台的爬取数据\微信公众号_湖北电信/raw_data.txt"
raw_file_path = ur"D:\催收平台的爬取数据\微信公众号_湖北电信/raw_combine.txt"
processed_file_path = ur"D:\催收平台的爬取数据\微信公众号_湖北电信/processed_data.txt"

specify_province = u"湖北省"

kuandai_account_pattern = ur"[a-zA-Z_]"

specify_citys = {
    u"武汉": "1001",
    u"襄阳": "1003",
    u"黄冈": "1004",
    u"宜昌": "1005",
    u"孝感": "1006",
    u"鄂州": "1007",
    u"咸宁": "1008",
    u"十堰": "1009",
    u"荆门": "1010",
    u"黄石": "1011",
    u"随州": "1012",
    u"恩施": "1013",
    u"仙桃": "1014",
    u"天门": "1015",
    u"潜江": "1016",
    u"神农架": "1017",
    u"荆州": "1018",
}

item_header = "name^address^account^phone^crawled_time"
file_item_header = "query_id_number^query_name^query_province^query_city^name^address^account^phone^crawled_time"

save_file_name = "hubeidianxin"

worker_num = 1
crawl_num = 0
skip_num = 0

crawled_set = set()

timeout_ratio = 3


def get_content(url, to=10, payload=None, method="GET"):
    error_count = 0
    while 1:

        if error_count >= 5:
            script_log.warning("hubeidianxin error, connect timeout")
            return ""

        try:
            with gevent.Timeout(to, requests.Timeout):
                if method == "GET":
                    response = requests.get(url, headers=headers, params=payload, verify=False,
                                            allow_redirects=False)
                else:
                    response = requests.post(url, headers=headers, data=payload, verify=False,
                                             allow_redirects=False)
                response.encoding = "gbk"

                if response.status_code in (302, 200):

                    return response.text
                elif response.status_code in (404, 500):
                    return ""
                else:
                    error_count += 1
                    time.sleep(0.5)
        except Exception:
            error_count += 1
            continue


def process_one(id_num, line, channel_no=None):
    item = json.loads(line)

    value_lists = []

    kuandai = item.get("kuandai", None)
    install = item.get("install", None)

    install_link_mobile = ""

    if install is not None:
        install_list = install.get("data", {})

        if isinstance(install_list, dict):
            install_list = [install_list]
        elif isinstance(install_list, unicode):
            install_list = []

        link_mobile = set()
        telphone = set()

        id_numbers = set()

        for install in install_list:
            if isinstance(install.get("LinkMobile", ""), list):
                link_mobile.update(install.get("LinkMobile", ""))
            else:
                link_mobile.update([install.get("LinkMobile", "")])

            if isinstance(install.get("Telephone", ""), list):
                telphone.update(install.get("Telephone", ""))
            else:
                telphone.update([install.get("Telephone", "")])

            id_numbers.update([install.get("CertificateNo", "")])

        if len(id_numbers) == 1 and list(id_numbers)[0] == id_num:
            install_link_mobile = ",".join(link_mobile)

    if install is None and u"查询失败！" in line:
        return value_lists

    if u"查询失败！" not in line and kuandai is not None:
        kuandai_list = kuandai.get("data", [])

        if kuandai_list is None:
            kuandai_list = []

        for kuandai in kuandai_list:

            kuandai_name = kuandai.get('username', "")
            kuandai_address = kuandai.get("useraddress", "")
            kuandai_account = kuandai.get("net", "")

            if channel_no is None:

                value_list = [
                    item.get("query_id_number", ""),
                    item.get("query_name", ""),
                    item.get("query_province", ""),
                    item.get("query_city", ""),

                    kuandai_name,
                    kuandai_address,
                    kuandai_account,
                    install_link_mobile,

                    item.get("crawled_time", ""),
                ]

                value_list = format_value_list(value_list)

                value_lists.append(value_list)

            elif channel_no == 1:
                phones = set()
                phones.update(install_link_mobile.split(","))
                phones.update(get_all_valid_phones(kuandai_account))

                for phone in phones:
                    if phone:
                        value_list = [
                            aes_1(field_handle(item.get("query_id_number", ""))),
                            aes_2(field_handle(phone)),
                            u"本人",
                            field_handle(item.get("query_name", "")),
                            "",
                            "1",
                        ]

                        value_list = format_value_list(value_list)

                        value_lists.append(value_list)

            elif channel_no == 2:
                pass
            else:
                pass

            # header
            # 姓名^宽带安装地址^宽带账号^联系电话^爬取时间
            # name^address^account^phone^crawled_time

    return value_lists


def process_one_with_dict(id_number, line):
    value_lists = process_one(id_number, line)

    value_dicts = []

    for value_list in value_lists:
        value_dict = dict(zip(item_header.split("^"), value_list))
        value_dict.pop("address")

        value_dicts.append(value_dict)

    return value_dicts


def process(file_path=None):
    if file_path is None:
        file_path = raw_file_path

    process_id = []

    with codecs.open(file_path, 'r', encoding="utf-8") as read_file:
        for line in read_file:
            item = json.loads(line)
            id_num = item.get("query_id_number", "")

            if id_num not in process_id:
                process_id.append(id_num)
            else:
                continue

            value_lists = process_one(id_num, line)

            for value_list in value_lists:
                save_value_list(processed_file_path, value_list)


def process_for_file(id_number, result_dict):
    value_lists = process_one(id_number, json.dumps(result_dict))

    return value_lists


def process_for_channel_1(id_number, result_dict):
    value_lists = process_one(id_number, json.dumps(result_dict), channel_no=1)

    return value_lists


def query(id_number, name, province, city, to):
    def get_install_information(id_number, city):
        localNetId = [v for c, v in specify_citys.items() if c in city]

        payload = {
            "custId": id_number,
            "localNetId": localNetId,
            "accNbr": "",
            "coNbr": "",
            "prodId": ""
        }

        content = get_content(install_query_url, payload=payload, to=to * timeout_ratio, method="POST")
        json_content = json.loads("{}")

        try:
            if content != "":
                if u"暂无正在处理的订单" not in content:
                    json_content = json.loads(content)
        except:
            pass

        return json_content

    def get_kuandai_information(id_number, city):
        areaname = [c for c in specify_citys.keys() if c in city]

        payload = {
            "areaname": areaname,
            "idcode": id_number
        }

        content = get_content(kuandai_query_url, payload=payload, to=to * timeout_ratio, method="POST")
        json_content = json.loads("{}")

        try:
            if content != "":
                if u"查询失败" not in content:
                    json_content = json.loads(content)
        except:
            pass

        return json_content

    result_dict = {
        "query_id_number": id_number,
        "query_name": name,
        "query_province": province,
        "query_city": city,
        "crawled_time": get_current_datetime()
    }

    if is_valid_idnum(idnum=id_number) is False or city in ("", None):
        return result_dict

    install_infomation = get_install_information(id_number, city)
    kuandai_infomation = get_kuandai_information(id_number, city)

    result_dict.update({"install": install_infomation})
    result_dict.update({"kuandai": kuandai_infomation})

    return result_dict


def combine():
    write_file = codecs.open(ur"D:\催收平台的爬取数据\微信公众号_湖北电信/raw_combine.txt", 'w', encoding="utf-8")

    data_dict = {}

    with codecs.open(ur"D:\催收平台的爬取数据\微信公众号_湖北电信/raw_data.txt", 'r', encoding="utf-8") as read_file:
        for line in read_file:
            if u"暂无正在处理的订单！" in line:
                continue

            item = json.loads(line)

            install = item.get("install", {})
            data = install.get("data", [{}])

            if isinstance(data, list):
                if len(data) == 0:
                    continue
                else:
                    id_number = data[0].get("CertificateNo", "")
            else:
                if isinstance(data, unicode):
                    continue
                if data.get("CertificateNo", None) in ("", None):
                    continue
                else:
                    id_number = data.get("CertificateNo", "")

            current_id_number = item.get("query_id_number", "")

            if id_number == current_id_number and id_number.strip() != "":
                if id_number not in data_dict.keys():
                    data_dict.update({id_number: install})

    with codecs.open(ur"D:\催收平台的爬取数据\微信公众号_湖北电信/raw_data_kuandai.txt", 'r', encoding="utf-8") as read_file:
        for line in read_file:
            item = json.loads(line.strip())

            id_number = item.get("query_id_number", "")

            if id_number in data_dict.keys():
                item.update({"install": data_dict[id_number]})

            write_file.write(json.dumps(item, ensure_ascii=False) + "\n")


def querry(task, to):
    id_number = task.get("person", {}).get("idnum", "")
    name = task.get("person", {}).get("name", "")

    short_id_num = id_number[:4]
    try:
        province, city = ID_BELONG_MAPPING[short_id_num]
    except:
        province, city = "", ""

    # province = task.get("person", {}).get("id_province", "")
    # city = task.get("person", {}).get("id_city", "")

    result_dict = query(id_number, name, province, city, to)

    return result_dict


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
        raw_result = querry(task, to)

        query_id_number = task.get("person", {}).get("idnum", "")

        value_lists = process_for_file(query_id_number, raw_result)
        for value_list in value_lists:
            if value_list:
                value_str = "^".join(value_list)

                save_file(filename=save_file_name, head=file_item_header, file=value_str)

        channel_value_list = process_for_channel_1(query_id_number, raw_result)
        for channel_value in channel_value_list:
            if channel_value:

                channel_1(channel_value)
    except:
        script_log.warning("hubeidianxin crawled failed, task: {}".format(task))


def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'idnum': u'420114199405292818',
                u'name': u'陈才'
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    # query_wrap(task, 10)


if __name__ == '__main__':
    main()
