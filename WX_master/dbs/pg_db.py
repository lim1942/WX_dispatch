# -*- coding: utf-8 -*-
# @Author: lim
# @Email: 940711277@qq.com
# @Date:  2018-04-02 14:31:54
# @Last Modified by:  lim
# @Last Modified time:  2018-04-09 16:11:05
import psycopg2 
from tools import get_logger, error_record
from config import PG_DB ,PG_USER, PG_PWD, PG_HOST, PG_PORT 

pd_db_log = get_logger('pgsql_db')


class PgSql(object):


    def __init__(self):
        self.conn = self.get_conn()
        self.cursor = self.get_cursor()
        #self.table_1 = self.create_table_1()
        #self.table_2 = self.create_table_2()
        #self.table_3 = self.create_table_3()


    def get_conn(self):
        try:
            return psycopg2.connect(database=PG_DB, user=PG_USER, password=PG_PWD,
                 host=PG_HOST, port=PG_PORT) 
        except Exception as e:
            error_record('200')
            pd_db_log.warning('200:Can not establish a connection to guangzhou pg DB: {}'.format(e.message))


    def get_cursor(self):
        try:
            return self.conn.cursor()
        except Exception as e:
            error_record('200')
            pd_db_log.warning('200:Can not establish a connection to guangzhou pg DB :{}'.format(e.message))


    def create_table_1(self):
        sql = '''CREATE TABLE if not exists rc_cp_mi_info_contact_phone
           (id SERIAL PRIMARY KEY NOT NULL,
           idnumber       CHAR(100) ,
           c_phone        CHAR(100) ,
           relation       CHAR(100) ,
           c_name         CHAR(100) ,
           comment        CHAR(100) ,
           level          CHAR(100) ,
           creatd_time    CHAR(100));'''
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            pd_db_log.warning('204:Can not creatd table 1:{}'.format(e.message))


    def create_table_2(self):
        sql = '''CREATE TABLE if not exists rc_cp_mi_info_phone_company
           (id SERIAL PRIMARY KEY NOT NULL,
           phone          CHAR(100) ,
           company        CHAR(100) ,
           creatd_time    CHAR(100));'''
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            pd_db_log.warning('205:Can not creatd table 2')


    def create_table_3(self):
        sql = '''CREATE TABLE if not exists rc_cp_mi_info_address
           (id SERIAL PRIMARY KEY NOT NULL,
           idnumber       CHAR(100) ,
           relation       CHAR(100) ,
           comment        CHAR(100) ,
           c_name         CHAR(100) ,
           c_phone        CHAR(100) ,
           address        CHAR(300) ,
           level          CHAR(100) ,
           creatd_time    CHAR(100));'''
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            pd_db_log.warning('206:Can not creatd table 3')


    def channel_1(self,data_list):
        """ for table rc_cp_mi_info_contact_phone"""
        if not isinstance(data_list,list):
            return
        try:
            self.cursor.execute("INSERT INTO hadoop.rc_cp_mi_info_contact_phone"
             "(idnumber,c_phone,relation,c_name,comment,level"
             ",create_time)VALUES(%s,%s,%s,%s,%s,%s,%s)",tuple(data_list))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            pd_db_log.warning('201:arror in insert data to pg table one:{}'.format(e.message))
            if 'duplicate' not in e.message:
                error_record('201')            


    def channel_2(self,data_list):
        """ for table rc_cp_mi_info_phone_company"""
        if not isinstance(data_list,list):
            return
        try:
            self.cursor.execute("INSERT INTO hadoop.rc_cp_mi_info_phone_company"
            "(phone,company,create_time)VALUES(%s,%s,%s)",tuple(data_list))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            pd_db_log.warning('202:Error in insert data to pg table two:{}'.format(e.message))
            if 'duplicate' not in e.message:
                error_record('202')


    def channel_3(self,data_list):
        """for table rc_cp_mi_info_address"""
        if not isinstance(data_list,list):
            return
        try:
            self.cursor.execute("INSERT INTO hadoop.rc_cp_mi_info_address"
             "(idnumber,relation,comment,c_name,c_phone,"
             "type,address,level,create_time)VALUES"
             "(%s,%s,%s,%s,%s,%s,%s,%s,%s)",tuple(data_list))
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            pd_db_log.warning('203:Error in insert data to pg table three:{}'.format(e.message))
            if 'duplicate' not in e.message:
                error_record('203')

