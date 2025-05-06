# -*- coding:utf-8 -*-

"""
说明： DJANGO直接使用原生SQL处理
"""
import datetime
import time
from django.db import connection, transaction
import logging
logger = logging.getLogger("")


def datetime_to_timestamp(datetime_obj):
    """将本地(local) datetime 格式的时间 (含毫秒) 转为毫秒时间戳
    :param datetime_obj: {datetime}2016-02-25 20:21:04.242000
    :return: 13 位的毫秒时间戳  1456402864242
    """
    local_timestamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
    return local_timestamp


class DjMysqlHandleInfo:
    """
    Oracle 相关信息, 便于扩展需要的信息
    """
    def __init__(self, result, message, elapsed_time, affect_rows):
        self.result = result
        self.message = message
        self.session_connect_elapsed_time = 0
        self.elapsed_time = elapsed_time
        self.affect_rows = affect_rows
        self.data = None

    def set_query_data(self, data):
        self.data = data

    def set_connect_elapsed_time(self, elapsed_time):
        self.session_connect_elapsed_time = elapsed_time

class DjMysqlHandle:
    """
    Mysql
    """
    def __init__(self):

        self._connect_time = 0  # 链接消耗时间，毫秒
        self.last_error_message = ""

        self._db_cursor = None  # 游标
        self._db_cursor_is_open = False     # cursor status
        self.data_fetch_is_finished = True    # 数据抽取已经完成

    def mysql_execute_query_get_all_data(self, sql_text, buffer_size=1000):
        """
        mysql execute sql , select; 只适合小数据量提取数据
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = connection.cursor()
            self._db_cursor_is_open = True
            self._db_cursor.arraysize = buffer_size
            self._db_cursor.execute(sql_text)
            data = self._db_cursor.fetchall()
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            mysql_info = DjMysqlHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
            mysql_info.set_query_data(data)
            mysql_info.set_connect_elapsed_time(self._connect_time)

        except Exception as e:
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("mysql query error: {0}".format(e))
            mysql_info = DjMysqlHandleInfo(False,
                                         "mysql query error: {0}".format(e),
                                         sql_end_time - sql_start_time,
                                         0)
            mysql_info.set_connect_elapsed_time(0)
        # finally:
        #     if self._db_cursor_is_open:
        #         self._db_cursor.close()
        #         self._db_cursor_is_open = False
        return mysql_info

    def mysql_execute_dml_sql(self, sql_text, auto_commit=True):
        """
        mysql execute sql , insert, update, delete ...
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = connection.cursor()    # 获取cursor
            self._db_cursor_is_open = True
            # result = self._db_cursor.execute(sql_text, multi=True)  # , multi=True

            affect_rows = 0
            self._db_cursor.execute(sql_text)

            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            if auto_commit:
                connection.commit()
            my_info = DjMysqlHandleInfo(True, "", sql_end_time - sql_start_time, self._db_cursor.rowcount)
            my_info.set_connect_elapsed_time(self._connect_time)
            my_info.set_query_data("")
            my_info.affect_rows = affect_rows

        except Exception as e:
            connection.rollback()
            affect_rows = 0
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("oracle connect error: {0}".format(e))
            my_info = DjMysqlHandleInfo(False, "mysql execute error: {0}".format(e), sql_end_time - sql_start_time, 0)
            my_info.set_connect_elapsed_time(0)
            my_info.set_query_data("")
        # finally:
        #     if self._db_cursor_is_open:
        #         self._db_cursor.close()
        #         self._db_cursor_is_open = False
        return my_info
