# coding=utf-8
import re

pattern_idnum_15 = ur"\d{15}"
pattern_idnum_18 = ur"\d{17}[0-9Xx]"

pattern_phone = ur"\d{11}"
pattern_name = ur"[\u4e00-\u9fa5]+"

pattern_format_mobile = ur"((13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\d{8})"
pattern_format_phone = ur"(^(\d{2,4}[-_－—]?)?\d{3,8}([-_－—]?\d{3,8})?([-_－—]?\d{1,7})?$)|(^0?1[35]\d{9}$)"

alphabet_pattern = ur"[a-zA-Z_]"


def is_string_not_empty(string):
    try:

        if string is None:
            return False

        if string.strip() == "":
            return False

        return True
    except AttributeError:
        return False


def is_valid_idnum(idnum):
    if is_string_not_empty(idnum):
        try:
            idnum = str(idnum).upper()

            return len(idnum) in (15, 18) and (idnum == re.findall(pattern_idnum_15, idnum)[0] or idnum == re.findall(pattern_idnum_18, idnum)[0])

        except:
            return False

    return False


def is_valid_phone(phone):
    if is_string_not_empty(phone):
        try:
            phone = str(phone)

            return phone == re.findall(pattern_phone, phone)[0]

        except:
            return False

    return False


def get_format_mobiles(mobile_str):
    matchs = re.findall(pattern_format_mobile, mobile_str)

    mobiles = []

    for match in matchs:
        mobiles.append(match[0])

    return mobiles


def get_format_phones(phone_str):
    matchs = re.findall(pattern_format_phone, phone_str)

    phones = []

    for match in matchs:
        phones.append(match[3].replace("-", ""))

    return phones


def get_all_valid_phones(phone_str):
    # 步骤 1
    # str[:str.find("@")]
    # 步骤 2
    # 判断是否存在英文 ur"a-zA-Z_"
    # 步骤 3
    # 使用手机正则提取
    # 步骤 4
    # 只留下长度为 7, 8, 11 的

    phones = set()

    if phone_str.find("@") >= 0:
        phone_str = phone_str[:phone_str.find("@")]

    if len(re.findall(alphabet_pattern, phone_str)) <= 0:
        mobiles_match = re.findall(pattern_format_mobile, phone_str)

        for mobile in mobiles_match:
            phone_str.replace(mobile[0], "")

            phones.update([mobile[0]])

        if (len(phone_str) == 11 and phone_str[0] == "0") or (len(phone_str) in (7, 8) and phone_str[0] != "0"):
            phones.update([phone_str])

    return phones


def format_value_list(value_list):
    for i in xrange(len(value_list)):
        if value_list[i] is None:
            value_list[i] = ""
        value_list[i] = value_list[i].replace("\r", "").replace("\n", "").replace("^", "|").strip()

    return value_list


def is_valid_name(name):
    if is_string_not_empty(name):
        try:
            name = convert_to_unicode(name)
            return name == re.findall(pattern_name, name)[0]

        except:
            return False

    return False


def convert_to_unicode(string):
    if isinstance(string, unicode):
        return string
    else:
        try:
            if isinstance(string, str):
                return string.decode("utf-8")
        except ValueError:
            return string


def main():
    # print (is_valid_idnum("441502199411191336"))
    # print (is_valid_idnum("44150219941119133X"))
    # print (is_valid_idnum("4415021994111913XX"))
    # print (is_valid_idnum(u"阿萨德"))
    # print (is_valid_idnum("454564"))
    # print (is_valid_idnum("asdad214456163X"))
    # print (is_valid_idnum([]))
    # print (is_valid_idnum({}))
    # print (is_valid_idnum([{}]))

    # print (is_valid_phone("18814118010"))
    # print (is_valid_phone("441502199411191336"))
    # print (is_valid_phone("44150219941119133X"))
    # print (is_valid_phone("441502"))
    # print (is_valid_phone("441502asd"))
    # print (is_valid_phone("441502asd阿萨德"))
    # print (is_valid_phone([]))
    # print (is_valid_phone([{}]))
    # print (is_valid_phone({}))
    # print (is_valid_phone(""))

    # print (is_valid_name("阿萨德"))
    # print (is_valid_name(u"阿萨德"))
    # print (is_valid_name(""))
    # print (is_valid_name("阿萨德123"))
    # print (is_valid_name(u"阿萨德123"))
    # print (is_valid_name("12大时代"))
    # print (is_valid_name(u"12大时代"))
    # print (is_valid_name("12大时代asd"))
    # print (is_valid_name(u"12大时代asd"))
    # print (is_valid_name("adsa"))
    # print (is_valid_name(u"adsa"))
    # print (is_valid_name("12adasd"))
    # print (is_valid_name(u"12adasd"))

    # print get_all_valid_phones("18814118010-18565748010")
    print get_all_valid_phones("bsht88888@ADSL")


if __name__ == '__main__':
    main()
