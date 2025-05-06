# -*- coding:utf-8 -*-

"""
说明： Oracle 处理
"""

import datetime
import time
import os
import logging
logger = logging.getLogger("")
import cx_Oracle


def datetime_to_timestamp(datetime_obj):
    """将本地(local) datetime 格式的时间 (含毫秒) 转为毫秒时间戳
    :param datetime_obj: {datetime}2016-02-25 20:21:04.242000
    :return: 13 位的毫秒时间戳  1456402864242
    """
    local_timestamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
    return local_timestamp


class OracleHandleInfo:
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

    def __call__(self):
        pass

    def set_query_data(self, data):
        self.data = data

    def set_connect_elapsed_time(self, elapsed_time):
        self.session_connect_elapsed_time = elapsed_time


class OracleHandle:
    """
    Oracle
    """
    def __init__(self, username, passwd, instance_name, address, port=1521):
        os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
        self._db_address = address
        self._db_port = port
        self._instance_name = instance_name
        self._username = username
        self._passwd = passwd

        self._connect_time = 0  # 链接消耗时间，毫秒
        self.last_error_message = ""
        self.connection = self._ora_connect()

        self._db_cursor = None  # 游标
        self._db_cursor_is_open = False     # cursor status
        self.data_fetch_is_finished = True    # 数据抽取已经完成

    def get_instance_name(self):
        return self._instance_name

    def get_address(self):
        return self._db_address

    def get_port(self):
        return self._db_port

    def get_username(self):
        return self._username

    def get_passwd(self):
        return self._passwd

    def _ora_connect(self):
        """
        oracle connect
        :return:
        """
        connect_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            ora_url = "{0}:{1}/{2}".format(self._db_address, self._db_port, self._instance_name)
            conn = cx_Oracle.Connection(self._username, self._passwd, ora_url, events=True)
            connect_end_time = datetime_to_timestamp(datetime.datetime.now())
            conn.ping()
            self._connect_time = connect_end_time - connect_start_time
            return conn
        except Exception as e:
            logger.error("oracle connect error: {0}".format(e))
            self.last_error_message = "oracle connect error: {0}".format(e)
            raise

    def ora_execute_dml_sql(self, sql_text, auto_comit=True):
        """
        oracle execute sql , insert, update, delete ...
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = self.connection.cursor()    # 获取cursor
            self._db_cursor_is_open = True
            exec_result = self._db_cursor.execute(sql_text)
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            if auto_comit:
                self.connection.commit()
            ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
            ora_info.set_connect_elapsed_time(self._connect_time)
            ora_info.set_query_data("")

        except Exception as e:
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("oracle execute error: {0}".format(e))
            ora_info = OracleHandleInfo(False,
                                        "oracle execute error: {0}".format(e),
                                        sql_end_time-sql_start_time,
                                        0)
            ora_info.set_connect_elapsed_time(0)
            ora_info.set_query_data("")
        finally:
            if self._db_cursor_is_open:
                self._db_cursor.close()
                self._db_cursor_is_open = False

        return ora_info

    def ora_execute_query_get_count(self, sql_text, buffer_size=1000):
        """
        获取数据总数
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = self.connection.cursor()  # 获取cursor
            self._db_cursor_is_open = True
            self._db_cursor.arraysize = buffer_size
            exec_result = self._db_cursor.execute(sql_text)
            data = self._db_cursor.fetchone()
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            ora_info = OracleHandleInfo(True, "", sql_end_time - sql_start_time, exec_result.rowcount)
            ora_info.set_query_data(data)
            ora_info.set_connect_elapsed_time(self._connect_time)

        except Exception as e:
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("oracle query error: {0}".format(e))
            ora_info = OracleHandleInfo(False,
                                        "oracle query error: {0}".format(e),
                                        sql_end_time - sql_start_time,
                                        0)
            ora_info.set_query_data("")
            ora_info.set_connect_elapsed_time(0)
        finally:
            if self._db_cursor_is_open:
                self._db_cursor.close()
                self._db_cursor_is_open = False
        return ora_info

    def ora_execute_query_get_all_data(self, sql_text, buffer_size=1000):
        """
        oracle execute sql , select; 只适合小数据量提取数据
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = self.connection.cursor()    # 获取cursor
            self._db_cursor_is_open = True
            self._db_cursor.arraysize = buffer_size
            exec_result = self._db_cursor.execute(sql_text)
            data = self._db_cursor.fetchall()
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, exec_result.rowcount)
            ora_info.set_query_data(data)
            ora_info.set_connect_elapsed_time(self._connect_time)

        except Exception as e:
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("oracle query error: {0}".format(e))
            ora_info = OracleHandleInfo(False,
                                        "oracle query error: {0}".format(e),
                                        sql_end_time-sql_start_time,
                                        0)
            ora_info.set_query_data("")
            ora_info.set_connect_elapsed_time(0)
        finally:
            if self._db_cursor_is_open:
                self._db_cursor.close()
                self._db_cursor_is_open = False
        return ora_info

    def ora_execute_query_get_data_loop(self, sql_text, row_number, buffer_size=1000):
        """
        oracle execute sql , select; 循环提取数据
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        if self._db_cursor_is_open:
            try:
                data = self._db_cursor.fetchmany(row_number)
                sql_end_time = datetime_to_timestamp(datetime.datetime.now())

                if data:
                    self.data_fetch_is_finished = False
                    ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
                    ora_info.set_query_data(data)
                    ora_info.set_connect_elapsed_time(self._connect_time)
                else:
                    self.data_fetch_is_finished = True
                    ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
                    ora_info.set_query_data(data)
                    ora_info.set_connect_elapsed_time(self._connect_time)
            except Exception as e:
                self.data_fetch_is_finished = True
                sql_end_time = datetime_to_timestamp(datetime.datetime.now())
                logger.error("oracle query error: {0}".format(e))
                ora_info = OracleHandleInfo(False,
                                            "oracle query error: {0}".format(e),
                                            sql_end_time-sql_start_time,
                                            0)

        else:
            try:
                self._db_cursor = self.connection.cursor()    # 获取cursor
                self._db_cursor_is_open = True
                self._db_cursor.arraysize = buffer_size
                exec_result = self._db_cursor.execute(sql_text)
                data = self._db_cursor.fetchmany(row_number)
                sql_end_time = datetime_to_timestamp(datetime.datetime.now())

                if data:
                    self.data_fetch_is_finished = False
                    ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
                    ora_info.set_query_data(data)
                    ora_info.set_connect_elapsed_time(self._connect_time)
                else:
                    self.data_fetch_is_finished = True
                    ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
                    ora_info.set_query_data(data)
                    ora_info.set_connect_elapsed_time(self._connect_time)

            except Exception as e:
                self.data_fetch_is_finished = True
                sql_end_time = datetime_to_timestamp(datetime.datetime.now())
                logger.error("oracle query error: {0}".format(e))
                ora_info = OracleHandleInfo(False,
                                            "oracle query error: {0}".format(e),
                                            sql_end_time-sql_start_time,
                                            0)
        if self.data_fetch_is_finished:
            if self._db_cursor:
                self._db_cursor.close()
        return ora_info


if __name__ == "__main__":
    import json
    import re
    ora = OracleHandle("system", "oracle1231", "lumeta", "172.168.71.55")
    sql = "select owner,table_name from all_tables where rownum<5"

    orainfo = ora.ora_execute_query_get_all_data(sql)

    result = json.dumps(orainfo.data)
    # p = re.split(r'[;,\s]\s*', line)
    print(result)

    ss = json.loads(result)

    # while True:
    #     orainfo = ora.ora_execute_query_get_data_loop(sql, 100)
    #     if ora.data_fetch_is_finished:
    #         break
    #
    #     print len(orainfo.data)

    # result = ora.ora_execute_dml_sql(sql)
    # cursor = ora.connection.cursor()
    #
    # cursor.arraysize = 10000
    # cursor.execute(sql)
    # result = cursor.fetchall()

    # print len(result)
    # for row in cursor.fetchall():
    #     print row[0]
    # while True:
    #     results = cursor.fetchmany(1000)
    #     if not results:
    #         break
    #
    #
    #     print results[1]
    # print result
