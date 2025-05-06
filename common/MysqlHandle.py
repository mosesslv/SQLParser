# -*- coding:utf-8 -*-

"""
说明： Mysql 处理
"""

import time
import datetime
import os
import logging
logger = logging.getLogger("")
import mysql.connector


def datetime_to_timestamp(datetime_obj):
    """将本地(local) datetime 格式的时间 (含毫秒) 转为毫秒时间戳
    :param datetime_obj: {datetime}2016-02-25 20:21:04.242000
    :return: 13 位的毫秒时间戳  1456402864242
    """
    local_timestamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
    return local_timestamp


class MysqlHandleInfo:
    """
    Mysql 相关信息, 便于扩展需要的信息
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


class MysqlHandle:
    """
    Mysql
    """
    def __init__(self, username, passwd, host, port=3306, database="mysql"):
        os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
        self._db_host = host
        self._db_port = port
        self._database = database
        self._username = username
        self._passwd = passwd

        self._connect_time = 0  # 链接消耗时间，毫秒
        self.last_error_message = ""
        self.connection = self._mysql_connect()

        self._db_cursor = None  # 游标
        self._db_cursor_is_open = False     # cursor status
        self.data_fetch_is_finished = True    # 数据抽取已经完成

    def _mysql_connect(self):
        """
        mysql connect
        :return:
        """
        connect_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            conn = mysql.connector.connect(user=self._username,
                                           password=self._passwd,
                                           host=self._db_host,
                                           port=self._db_port,
                                           database=self._database)
            connect_end_time = datetime_to_timestamp(datetime.datetime.now())
            conn.ping()
            self._connect_time = connect_end_time - connect_start_time
            return conn
        except mysql.connector.Error as e:
            logger.error("mysql connect error: {0}".format(e))
            self.last_error_message = "mysql connect error: {0}".format(e)
            raise
        except Exception as e:
            logger.error("mysql error: {0}".format(e))
            self.last_error_message = "mysql connect error: {0}".format(e)
            raise

    def mysql_execute_dml_sql(self, sql_text, auto_commit=True):
        """
        mysql execute sql , insert, update, delete ...
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = self.connection.cursor()    # 获取cursor
            self._db_cursor_is_open = True
            # result = self._db_cursor.execute(sql_text, multi=True)  # , multi=True

            affect_rows = 0
            for exec_result in self._db_cursor.execute(sql_text, multi=True):
                affect_rows += exec_result.rowcount

            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            if auto_commit:
                self.connection.commit()
            my_info = MysqlHandleInfo(True, "", sql_end_time - sql_start_time, self._db_cursor.rowcount)
            my_info.set_connect_elapsed_time(self._connect_time)
            my_info.set_query_data("")
            my_info.affect_rows = affect_rows

        except Exception as e:
            self.connection.rollback()
            affect_rows = 0
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("mysql connect error: {0}".format(e))
            my_info = MysqlHandleInfo(False,
                                      "mysql execute error: {0}".format(e),
                                      sql_end_time - sql_start_time,
                                      0)
            my_info.set_connect_elapsed_time(0)
            my_info.set_query_data("")
        finally:
            if self._db_cursor_is_open:
                self._db_cursor.close()
                self._db_cursor_is_open = False
        return my_info

    def mysql_execute_query_get_all_data(self, sql_text, buffer_size=1000):
        """
        mysql execute sql , select; 只适合小数据量提取数据
        :return:
        """
        sql_start_time = datetime_to_timestamp(datetime.datetime.now())
        try:
            self._db_cursor = self.connection.cursor()    # 获取cursor
            self._db_cursor_is_open = True
            self._db_cursor.arraysize = buffer_size
            self._db_cursor.execute(sql_text)
            data = self._db_cursor.fetchall()
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            mysql_info = MysqlHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
            mysql_info.set_query_data(data)
            mysql_info.set_connect_elapsed_time(self._connect_time)

        except Exception as e:
            sql_end_time = datetime_to_timestamp(datetime.datetime.now())
            logger.error("mysql query error: {0}".format(e))
            mysql_info = MysqlHandleInfo(False,
                                         "mysql query error: {0}".format(e),
                                         sql_end_time - sql_start_time,
                                         0)
            mysql_info.set_connect_elapsed_time(0)
        finally:
            if self._db_cursor_is_open:
                self._db_cursor.close()
                self._db_cursor_is_open = False
        return mysql_info

    def get_host(self):
        return self._db_host

    def get_port(self):
        return self._db_port

    def get_username(self):
        return self._username

    def get_passwd(self):
        return self._passwd

    def get_sql_display_column_names(self):
        """
        获取 SQL 查询的表头信息
        :return: tuple
        """
        if self._db_cursor is None:
            return None
        else:
            return self._db_cursor.column_names

    # def ora_execute_query_get_data_loop(self, sql_text, row_number, buffer_size=1000):
    #     """
    #     oracle execute sql , select; 循环提取数据
    #     :return:
    #     """
    #     sql_start_time = common_time.datetime_to_timestamp(datetime.datetime.now())
    #     if self._db_cursor_is_open:
    #         try:
    #             data = self._db_cursor.fetchmany(row_number)
    #             sql_end_time = common_time.datetime_to_timestamp(datetime.datetime.now())
    #
    #             if data:
    #                 self.data_fetch_is_finished = False
    #                 ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
    #                 ora_info.set_query_data(data)
    #             else:
    #                 self.data_fetch_is_finished = True
    #                 ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
    #                 ora_info.set_query_data(data)
    #         except Exception, e:
    #             self.data_fetch_is_finished = True
    #             sql_end_time = common_time.datetime_to_timestamp(datetime.datetime.now())
    #             logger.error("oracle connect error: {0}".format(e.message))
    #             ora_info = OracleHandleInfo(False,
    #                                         "oracle connect error: {0}".format(e.message),
    #                                         sql_end_time-sql_start_time,
    #                                         0)
    #
    #     else:
    #         try:
    #             self._db_cursor = self.connection.cursor()    # 获取cursor
    #             self._db_cursor_is_open = True
    #             self._db_cursor.arraysize = buffer_size
    #             exec_result = self._db_cursor.execute(sql_text)
    #             data = self._db_cursor.fetchmany(row_number)
    #             sql_end_time = common_time.datetime_to_timestamp(datetime.datetime.now())
    #
    #             if data:
    #                 self.data_fetch_is_finished = False
    #                 ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
    #                 ora_info.set_query_data(data)
    #             else:
    #                 self.data_fetch_is_finished = True
    #                 ora_info = OracleHandleInfo(True, "", sql_end_time-sql_start_time, self._db_cursor.rowcount)
    #                 ora_info.set_query_data(data)
    #
    #         except Exception, e:
    #             self.data_fetch_is_finished = True
    #             sql_end_time = common_time.datetime_to_timestamp(datetime.datetime.now())
    #             logger.error("oracle connect error: {0}".format(e.message))
    #             ora_info = OracleHandleInfo(False,
    #                                         "oracle connect error: {0}".format(e.message),
    #                                         sql_end_time-sql_start_time,
    #                                         0)
    #     if self.data_fetch_is_finished:
    #         if self._db_cursor:
    #             self._db_cursor.close()
    #     return ora_info


if __name__ == "__main__":
    myconn = MysqlHandle("mysqladmin", "mysqladmin", "172.168.71.55")

    # sql = "select Host,user from user"
    sql = "use abidata"
    session_timeout_sql = "set session lock_wait_timeout=0.1"
    # sql = "insert into ta (id) values (1)"
    # while True:
    #     orainfo = ora.ora_execute_query_get_data_loop(sql, 100)
    #     if ora.data_fetch_is_finished:
    #         break
    #
    #     print len(orainfo.data)

    # result = myconn.mysql_execute_query_get_all_data(sql)
    result = myconn.mysql_execute_dml_sql(session_timeout_sql)

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
