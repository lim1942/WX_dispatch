# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-03-01 14:45:22
# @Last Modified by:  lim
# @Last Modified time:  2018-04-11 10:23:22

import os
import sys 
import time
import codecs
import base64
import requests
import logging
from logging.handlers import RotatingFileHandler
from Crypto.Cipher import AES  

from config import LOG_DIR,LOG_SIZE,LOG_BACKUP
from config import SAVE_PATH, MASTER_IP, MASTER_PORT, SECRET
if not os.path.exists(SAVE_PATH):
    os.makedirs('data')

AES_ENCRYPT = None


class AesEncrypt():  
    """用于本地的aes加密"""
    def __init__(self):  
        self.length = 16
        self.key = '2@%#(){}KLL@delv' 
        self.iv = '0000000000000000'
        self.mode = AES.MODE_CBC  
       
    def encrypt(self, field):  
        if field:
            # utf-8 encode to encrypt
            field = convert_to_unicode(field).encode('utf-8')
            # delet space
            field = (field.replace('\t','').replace(' ',''
                    ).replace('\n','').replace('\r\n',''
                    ).replace('\r','').strip())
            count = len(field)  
            if(count % self.length != 0) :  
                add = self.length - (count % self.length)  
            else:  
                add = 0  
            field = field + ('\0' * add)
            cryptor = AES.new(self.key, self.mode, self.iv)  
            self.ciphertext = cryptor.encrypt(field)  
            field = base64.b64encode(self.ciphertext).strip()
            return field
    def decrypt(self, field):  
        cryptor = AES.new(self.key, self.mode, self.iv)  
        plain_text = cryptor.decrypt(base64.b64decode(field))  
        return plain_text.rstrip('\0') 


def convert_to_unicode(string):
    if isinstance(string, unicode):
        return string
    else:
        try:
            if isinstance(string, str):
                return string.decode("utf-8")
        except ValueError:
            return string


def covertLineToDict(line):
    """处理原数据"""
    if '^' not in line:
        return
    idnum= name= phone =''
    con = line.split('^')
    for item in con: 
        if len(item) == 11 and item.isdigit() and item.startswith('1'):
            phone = item
        elif item[0:-1].isdigit() and (len(item)==15 or len(item)==18):
            idnum = item.replace('X','x')
        elif not item[:-1].isdigit():
            name = item
    pwd = idnum[-6:]
    return  {
        'idnum':idnum,
        'phone':phone,
        'name':name,
        'pwd':pwd
        }


def get_datetime():
    datetime = time.strftime("%Y-%m-%d*%H:%M:%S")
    return datetime


def get_time():
    return time.time()


def get_save_date():
    """写文件时间"""
    date = time.strftime("%Y-%m-%d")
    date = '_' + date
    return date


def get_crawl_date():
    """爬取时间"""
    date = time.strftime("%Y-%m-%d")
    return date


def channel_1(data_list):
    """连接插入 pg的表 1"""
    headers = {'channel':'1'}
    data_list.append(get_crawl_date())
    channel(data_list, headers)


def channel_2(data_list):
    """连接插入 pg的表 2"""
    headers = {'channel':'2'}
    data_list.append(get_crawl_date())
    channel(data_list, headers)


def channel_3(data_list):
    """连接插入 pg的表 3"""
    headers = {'channel':'3'}
    data_list.append(get_crawl_date())
    channel(data_list, headers)


def channel(data_list,headers,retry=5):
    """向广州插入数据"""
    while  retry:
        try:
            headers.update({'secret':SECRET})
            url = 'http://{}:{}/save_data_to_guangzhou'.format(MASTER_IP,MASTER_PORT)
            r = requests.get(url,headers=headers,json=data_list,timeout=10)
            if r.status_code ==200:
                return True
            else:
                raise Exception('error')
        except Exception as e:
            time.sleep(3)
            retry -=1

    if not retry:
        error_record('201')
        get_logger('guangzhou_api').warning('201:Can not insert data to guangzhou db:{}'.format(e.message))


def aes_1(field):
    """广州aes加密"""
    return field
    # if not field:
    #     return ''
    # headers = {'secret':'123456789','field':field}
    # url = 'http://{}:{}/get_aes_from_guangzhou'.format(MASTER_IP,MASTER_PORT)
    # try:
    #     field = requests.get(url,headers=headers,timeout=10).json()
    #     return field
    # except Exception as e:
    #     get_logger('guangzhou_api').warning('Can not insert data to guangzhou db {}'.format(e.message))
    #     print(e)
        # return ''


def aes_2(field):
    """本地加密的aes加密"""
    global AES_ENCRYPT 
    if not AES_ENCRYPT:
        AES_ENCRYPT = AesEncrypt()
    try:
        return AES_ENCRYPT.encrypt(field)
    except AttributeError:
        AES_ENCRYPT = AesEncrypt()
    except:
        pass
    return ''


def field_handle(field):
    """ 对字段去空格和^"""
    try:
        if field is None:
            field = ''
        if not isinstance(field,str):
            field = str(field)
        field = field.replace('^','')
        field = field.replace('\t','').replace(' ',''
                ).replace('\n','').replace('\r\n',''
                ).replace('\r','').strip()
        return field
    except:
        return ''


def save_file(filename,file,head):
    """存储本地文件"""
    if not file or not filename:
        return
    try:     
        filename = filename+ get_save_date()+'.txt'
        filename = os.sep.join([SAVE_PATH,filename])
        if not os.path.exists(filename):
            with codecs.open(filename,'a','utf-8') as f:
                f.write(head+'\n')
        with codecs.open(filename,'a','utf-8') as f:
            f.write(file+'\n')
        return True
    except Exception as e:
        print(e)



#for log ------------------------
def get_log_file_name(fname, log_dir=LOG_DIR):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), log_dir, fname + '.log')


def ensure_dir(fname):
    if fname.endswith(os.sep):
        fname = fname.rstrip(os.sep)
    fname = os.path.dirname(fname)
    dirs = fname.split(os.sep)
    for i in range(len(dirs)):
        d = os.sep.join(dirs[:i + 1])
        if not os.path.isdir(d):
            try:
                os.mkdir(d)
            except Exception as e:
                pass


def get_logger(name):
    fname = get_log_file_name(name)
    ensure_dir(fname)
    api_logger = logging.getLogger(fname)
    # stream_handler = logging.StreamHandler()
    rotating_handler = RotatingFileHandler(
        fname, maxBytes=LOG_SIZE * 1024 * 1024, backupCount=LOG_BACKUP)
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(funcName)-10s %(message)s')
    # stream_handler.setFormatter(formatter)
    rotating_handler.setFormatter(formatter)
    # logger.addHandler(stream_handler)
    api_logger.addHandler(rotating_handler)
    api_logger.setLevel(logging.DEBUG)
    return api_logger



def error_record(code):
    line = get_datetime() + ": " + code
    with open('warning.txt','a') as f:
        f.write(line+'\n')



# if __name__ == '__main__':
#     print channel(['34082293061260112we','156463ew54863',u'本人',u'李华','','1','2018-04-04'], {'channel':'1'})
     # print aes_2('test')
     # a = AesEncrypt()
     # print a.decrypt('NVqHReeCIT2SsVLzlkAQCuPFlkq+0iNLr+uXjGRvNJgkPyw4et1h1HTltNhG9fjId0+5uHFcKOaARZ4c+JPRkg==')