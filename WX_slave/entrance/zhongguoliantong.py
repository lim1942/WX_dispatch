# coding=utf-8
import re
import time
import json
import codecs
import gevent
import requests
import datetime

# from abuyun import PROXIE
from gevent import monkey
from utils.id_belong_mapping import ID_BELONG_MAPPING
from utils.value_formater import is_valid_idnum, get_all_valid_phones, format_value_list
from tools import save_file, field_handle, channel_1, channel_3, aes_1, aes_2, get_logger

script_log = get_logger('script')

PROXIE = ""

CITY_MAPPER = {
    "010": {u"巴彦": "105", u"锡林": "111", u"鄂尔": "104", u"通辽": "109", u"赤峰": "107", u"乌海": "106", u"呼和": "101",
            u"兴安": "113", u"包头": "102", u"阿拉": "114", u"呼伦": "108", u"乌兰": "103"},
    "011": {u"北京": "110"},
    "013": {u"天津": "130"},
    "017": {u"枣庄": "157", u"东营": "156", u"济南": "170", u"济宁": "158", u"菏泽": "159", u"临沂": "153", u"烟台": "161",
            u"莱芜": "160", u"潍坊": "155", u"聊城": "174", u"日照": "154", u"泰安": "172", u"德州": "173", u"滨州": "151",
            u"青岛": "166", u"威海": "152", u"淄博": "150"},
    "018": {u"廊坊": "183", u"邯郸": "186", u"邢台": "185", u"石家": "188", u"张家": "184", u"承德": "189", u"唐山": "181",
            u"沧州": "180", u"保定": "187", u"衡水": "720", u"秦皇": "182"},
    "019": {u"晋城": "194", u"忻州": "198", u"阳泉": "192", u"晋中": "191", u"吕梁": "200", u"朔州": "199", u"大同": "193",
            u"运城": "196", u"临汾": "197", u"太原": "190", u"长治": "195"},
    "030": {u"淮南": "307", u"阜阳": "306", u"铜陵": "308", u"宣城": "311", u"宿州": "313", u"宿县": "313", u"滁州": "312",
            u"滁县": "312", u"黄山": "316",
            u"芜湖": "303", u"蚌埠": "301", u"淮北": "314", u"安庆": "302", u"亳州": "318", u"马鞍": "300", u"六安": "304",
            u"巢湖": "309", u"池州": "317", u"合肥": "305", u"": "307", u"徽州": "307"},
    "031": {u"上海": "310"},
    "034": {u"常州": "440", u"泰州": "445", u"南京": "340", u"扬州": "430", u"无锡": "330", u"宿迁": "349", u"镇江": "343",
            u"盐城": "348", u"徐州": "350", u"苏州": "450", u"淮安": "354", u"连云": "346", u"南通": "358"},
    "036": {u"金华": "367", u"湖州": "362", u"台州": "476", u"温州": "470", u"杭州": "360", u"丽水": "469", u"舟山": "364",
            u"绍兴": "365", u"宁波": "370", u"嘉兴": "363", u"衢州": "468"},
    "038": {u"南平": "387", u"厦门": "390", u"福州": "380", u"三明": "389", u"宁德": "386", u"泉州": "480", u"莆田": "385",
            u"漳州": "395", u"龙岩": "384"},
    "050": {u"昌江": "517", u"万宁": "508", u"临高": "512", u"定安": "509", u"东方": "506", u"文昌": "505", u"五指": "507",
            u"乐东": "516", u"保亭": "515", u"琼中": "514", u"屯昌": "519", u"海口": "501", u"陵水": "513",
            u"儋州": "503", u"琼海": "504", u"白沙": "518", u"三亚": "502", u"澄迈": "511", u"": "517", u"三沙": "517"},
    "051": {u"广州": "510", u"珠海": "620", u"惠州": "570", u"汕头": "560", u"中山": "556", u"湛江": "520", u"梅州": "528",
            u"东莞": "580", u"云浮": "538", u"阳江": "565", u"清远": "535", u"江门": "550", u"揭阳": "526", u"汕尾": "525",
            u"茂名": "568", u"河源": "670", u"韶关": "558", u"肇庆": "536", u"潮州": "531", u"深圳": "540", u"佛山": "530"},
    "059": {u"贵港": "589", u"钦州": "597", u"河池": "598", u"梧州": "594", u"来宾": "601", u"贺州": "588", u"柳州": "593",
            u"北海": "599", u"崇左": "600", u"玉林": "595", u"南宁": "591", u"百色": "596", u"桂林": "592", u"防城": "590",
            u"": "589", u"广西": "589"},
    "070": {u"西宁": "700", u"果洛": "708", u"海东": "701", u"格尔": "702", u"黄南": "707", u"海南": "705",
            u"海北": "706", u"玉树": "709", u"海西": "704"},
    "071": {u"仙桃": "713", u"天门": "725", u"随州": "723", u"咸宁": "719", u"十堰": "721", u"恩施": "727", u"襄阳": "716",
            u"荆州": "712",
            u"宜昌": "711", u"潜江": "726", u"武汉": "710", u"神农": "722", u"黄冈": "714", u"孝感": "717", u"黄石": "715",
            u"荆门": "724",
            u"鄂州": "718"},
    "074": {u"益阳": "747", u"娄底": "791", u"衡阳": "744", u"湘西": "793", u"怀化": "795", u"郴州": "748", u"常德": "749",
            u"岳阳": "745", u"浏阳": "746", u"永州": "796", u"长沙": "741", u"株洲": "742", u"张家": "794", u"邵阳": "792",
            u"湘潭": "743", u"": "747", u"零陵": "747"},
    "075": {u"吉安": "751", u"赣州": "752", u"九江": "755", u"鹰潭": "754", u"抚州": "759", u"宜春": "756", u"新余": "753",
            u"景德": "740",
            u"萍乡": "758", u"上饶": "757", u"南昌": "750"},
    "076": {u"新乡": "764", u"安阳": "767", u"南阳": "777", u"漯河": "766", u"平顶": "769", u"商丘": "768", u"开封": "762",
            u"驻马": "771", u"郑州": "760", u"济源": "775", u"三门": "772", u"周口": "770", u"焦作": "763", u"濮阳": "773",
            u"信阳": "776", u"许昌": "765", u"鹤壁": "774", u"洛阳": "761"},
    "079": {u"拉萨": "790", u"林芝": "799", u"日喀": "797", u"那曲": "801", u"阿里": "802", u"山南": "798", u"昌都": "800"},
    "081": {u"广元": "826", u"凉山": "812", u"泸州": "815", u"资阳": "830", u"攀枝": "813", u"德阳": "825", u"眉山": "819",
            u"甘孜": "828", u"成都": "810", u"乐山": "814", u"阿坝": "829", u"内江": "816", u"自贡": "818", u"巴中": "827",
            u"达州": "820", u"宜宾": "817", u"绵阳": "824", u"遂宁": "821", u"雅安": "811", u"南充": "822", u"广安": "823",
            u"": "826", u"黔江": "826", u"万县": "826", u"涪陵": "826"},
    "083": {u"重庆": "831"},
    "084": {u"宝鸡": "840", u"延安": "842", u"汉中": "849", u"西安": "841", u"榆林": "845", u"渭南": "843", u"咸阳": "844",
            u"安康": "848", u"铜川": "846", u"商洛": "847"},
    "085": {u"黔东": "786", u"六盘": "853", u"遵义": "787", u"毕节": "851", u"安顺": "789", u"铜仁": "785", u"贵阳": "850",
            u"黔南": "788", u"黔西": "852"},
    "086": {u"玉溪": "865", u"昭通": "867", u"曲靖": "866", u"文山": "732", u"德宏": "730", u"景洪": "868",
            u"保山": "731", u"丽江": "863", u"迪庆": "735", u"普洱": "869", u"昆明": "860", u"西双": "736",
            u"楚雄": "864", u"红河": "861", u"临沧": "733", u"怒江": "734", u"大理": "862", u"": "865", u"思茅": "865"},
    "087": {u"陇南": "960", u"临夏": "878", u"定西": "871", u"酒泉": "931", u"甘南": "961", u"庆阳": "873", u"白银": "879",
            u"天水": "877", u"张掖": "875", u"嘉峪": "876", u"金昌": "930", u"武威": "874", u"平凉": "872", u"兰州": "870"},
    "088": {u"中卫": "886", u"固原": "885", u"吴忠": "883", u"石嘴": "884", u"银川": "880"},
    "089": {u"昌吉": "891", u"伊犁": "898", u"博尔": "951", u"阿克": "896", u"石河": "893", u"喀什": "897",
            u"巴音": "895", u"乌鲁": "890", u"塔城": "952", u"和田": "955", u"克拉": "899", u"奎屯": "892",
            u"哈密": "900", u"阿勒": "953", u"吐鲁": "894", u"克孜": "954", u"": "891", u"省直": "891"},
    "090": {u"松原": "904", u"吉林": "902", u"白城": "907", u"四平": "903", u"延边": "909", u"长春": "901", u"通化": "905",
            u"白山": "908", u"辽源": "906"},
    "091": {u"盘锦": "921", u"丹东": "915", u"营口": "917", u"沈阳": "910", u"辽阳": "919", u"鞍山": "912", u"大连": "940",
            u"朝阳": "920", u"阜新": "918", u"抚顺": "913", u"本溪": "914", u"铁岭": "911", u"锦州": "916", u"葫芦": "922"},
    "097": {u"双鸭": "994", u"七台": "992", u"齐齐": "973", u"大庆": "981", u"大兴": "995", u"鸡西": "991", u"绥化": "989",
            u"黑河": "990", u"佳木": "976", u"伊春": "996", u"哈尔": "971", u"鹤岗": "993", u"牡丹": "988"},
}

PROVINCE_MAPPER = {
    u"内蒙古自治区": "010",
    u"北京市": "011",
    u"天津市": "013",
    u"山东省": "017",
    u"河北省": "018",
    u"山西省": "019",
    u"安徽省": "030",
    u"上海市": "031",
    u"江苏省": "034",
    u"浙江省": "036",
    u"福建省": "038",
    u"海南省": "050",
    u"广东省": "051",
    u"广西壮族自治区": "059",
    u"青海省": "070",
    u"湖北省": "071",
    u"湖南省": "074",
    u"江西省": "075",
    u"河南省": "076",
    u"西藏自治区": "079",
    u"四川省": "081",
    u"重庆市": "083",
    u"陕西省": "084",
    u"贵州省": "085",
    u"云南省": "086",
    u"甘肃省": "087",
    u"宁夏回族自治区": "088",
    u"新疆维吾尔自治区": "089",
    u"新疆": "089",
    u"吉林省": "090",
    u"辽宁省": "091",
    u"黑龙江省": "097",
}

monkey.patch_all()

headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "Host": "iservice.10010.com",
    "Origin": "http://iservice.10010.com",
    "Proxy-Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

field_name_mapper = {
}

class_name = "zhongguoliantong"

query_url = u"http://iservice.10010.com/e3/static/life/broadbandinfoquery?_={}"

raw_file_path = ur"D:\催收平台的爬取数据\中国联通_宽带查询/raw_data.txt"
# raw_file_path = ur"D:\催收平台的爬取数据\中国联通_宽带查询/raw_data_example.txt"
processed_file_path = ur"D:\催收平台的爬取数据\中国联通_宽带查询/processed_data.txt"

item_header = "name^account^phone^address^install_date^crawled_time"
file_item_header = "query_id_number^query_name^query_province^query_city^name^account^phone^address^install_date^crawled_time"

save_name = "zhongguoliantong"

specify_keyword = u""

worker_num_of_direct = 20
worker_num_of_proxy = 10
crawl_num = 0
skip_num = 0

using_proxy_count = 0

crawled_set = set()

save_file_name = "zhongguoliantong"

timeout_ratio = 2


def get_content(url, to=10, payload=None, method="GET", is_need_proxy=False):
    error_times = 0

    while 1:
        if error_times >= 5:
            script_log.warning("zhonguoliantong error, connect timeout")
            return ""

        try:
            with gevent.Timeout(to, requests.Timeout):
                if method == "GET":
                    response = requests.get(url, headers=headers, params=payload, verify=False,
                                            allow_redirects=False, proxies=(PROXIE if is_need_proxy else None))
                else:
                    response = requests.post(url, headers=headers, data=payload, verify=False,
                                             allow_redirects=False, proxies=(PROXIE if is_need_proxy else None))
                response.encoding = "utf-8"

                is_need_proxy = False

                if response.status_code in (302, 200):

                    if '{"acclimit":"true"}' in response.text:
                        time.sleep(3)
                        continue

                    return response.text
                elif response.status_code in (404, 500):
                    return ""
                else:
                    error_times += 1
                    continue
        except Exception:
            error_times += 1

            continue


def process_for_file(result_dict):
    value_lists = process_one(result_dict)

    return value_lists


def process_for_channel_1(result_dict):
    raw_value_lists = process_one(result_dict, channel_no=1)

    value_lists = []

    for raw_value_list in raw_value_lists:
        if raw_value_list:
            id_number = field_handle(raw_value_list[0])
            name = field_handle(raw_value_list[1])
            phones = raw_value_list[2]

            for phone in phones:
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
    raw_value_lists = process_one(result_dict, channel_no=3)

    value_lists = []

    for raw_value_list in raw_value_lists:
        if raw_value_list:
                id_number = field_handle(raw_value_list[0])
                name = field_handle(raw_value_list[1])
                try:
                    phone = field_handle(raw_value_list[2][0])
                except:
                    phone = ""

                address = raw_value_list[3]
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


def process_one(item, channel_no=None):
    content = json.loads(item.get("content", "{}"))
    result_list = content.get("result", {})
    broad_info_list = result_list.get("broadbandinfo", [])

    value_lists = []

    for broad_info in broad_info_list:

        account = broad_info.get("broadbandcode", "")
        contact_phone = broad_info.get("contactphone", "")
        install_address = broad_info.get("installaddr", "")
        install_date = get_format_time(broad_info.get("installdate", ""))
        install_name = broad_info.get("username", "")

        if contact_phone == u"无":
            contact_phone = ""

        if len(re.findall(ur"[\u4E00-\u9FA5]", contact_phone)) > 0:
            install_address += " " + contact_phone
            contact_phone = ""

        if len(set(contact_phone)) <= 3:
            contact_phone = ""

        if install_address.strip() == "-1":
            install_address = ""

        if "?????" in install_address:
            install_address = ""

        if channel_no is None:

            value_list = [
                item.get("query_id_number"),
                item.get("query_name"),
                item.get("query_province"),
                item.get("query_city"),

                install_name,
                account,
                contact_phone,
                install_address,
                install_date,

                item.get("crawled_time"),
            ]

            value_lists.append(value_list)
        elif channel_no == 1:
            phones = set()
            phones.update(get_all_valid_phones(account))
            phones.update(get_all_valid_phones(contact_phone))

            value_list = [
                item.get("query_id_number"),
                item.get("query_name"),
                list(phones),
            ]

            value_lists.append(value_list)
        elif channel_no == 2:
            pass
        else:
            phones = set()
            phones.update(get_all_valid_phones(account))
            phones.update(get_all_valid_phones(contact_phone))

            value_list = [
                item.get("query_id_number"),
                item.get("query_name"),
                list(phones),
                install_address,
            ]

            value_lists.append(value_list)

    return value_lists


def process(file_path=None):
    if file_path is None:
        file_path = raw_file_path

    with codecs.open(file_path, 'r', encoding="utf-8") as read_file:

        for line in read_file:
            if u"查询无记录" in line or u"尊敬的客户" in line:
                continue

            item = json.loads(line)

            content = item.get("content", None)

            if content is None:
                continue

            value_lists = process_one(item)

            for value_list in value_lists:
                # header
                # 查询时所用的身份证^查询时所用的姓名^查询时所用的省份^查询时所用的城市^姓名^宽带账号^联系电话^宽带安装地址^宽带安装日期^爬取时间
                # query_id_number^query_name^query_province^query_city^name^account^phone^address^install_date^crawled_time
                save_value_list(processed_file_path, value_list)


def process_one_with_dict(item):
    value_lists = process_one(item)

    value_dicts = []

    for value_list in value_lists:
        value_dict = dict(zip(item_header.split("^"), value_list))
        value_dicts.append(value_dict)

    return value_dicts


def query(id_number, name, province, city, to):
    result_dict = {
        "query_id_number": id_number,
        "query_name": name,
        "query_province": province,
        "query_city": city,
        "crawled_time": get_current_datetime()
    }

    if not is_valid_idnum(idnum=id_number) or province in ("", None) or city in ("", None):
        return result_dict

    t = int(time.time() * 1000)

    province_code = PROVINCE_MAPPER.get(province)
    if province_code == "":
        return result_dict

    city_code = CITY_MAPPER[province_code].get(city[:2])
    if city_code == "":
        if len(CITY_MAPPER[province_code].values()) == 1:
            city_code = CITY_MAPPER[province_code].values()[0]
        elif CITY_MAPPER.get(u"", "") != "":
            city_code = CITY_MAPPER[u""]
        else:
            return result_dict

    url = query_url.format(t)

    payload = {
        "provinceCode": province_code,
        "cityCode": city_code,
        "carId": id_number,
        "telphone": "",
    }

    content = get_content(url=url, payload=payload, method="POST", to=timeout_ratio * to, is_need_proxy=False)

    if content != "" and u"您查询的号码无记录" not in content and u"确认宽带号码" not in content and u"出了一点点问题" not in content:
        result_dict.update({"content": content})

    return result_dict


def querry(task, to):
    id_number = task.get("person", {}).get("idnum", "")
    name = task.get("person", {}).get("name", "")

    try:
        short_id_number = id_number[:4]
        province, city = ID_BELONG_MAPPING[short_id_number]
    except:
        province, city = "", ""

    result_dict = []

    try:
        result_dict = query(id_number, name, province, city, to)

    except:
        pass

    return result_dict


def get_current_datetime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))


def get_format_time(t):
    return datetime.datetime.strptime(t, '%Y%m%d%H%M%S').strftime("%Y-%m-%d %H:%M:%S")


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
        script_log.warning("zhongguoliantong crawled failed, task: {}".format(task))


def main():
    task = {
        u'date': u'03',
        u'person':
            {
                u'idnum': u'130223198209296932',
                u'name': u'岳文杰',
                u'phone': u'',
            },
        u'entrance': u"[u'1', u'2', u'3']",
        u'retry': u'1',
        u'task_id': u'120103195506014210'
    }

    # query_wrap(task, 10)


if __name__ == '__main__':
    main()
