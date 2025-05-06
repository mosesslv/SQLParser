# -*- coding: UTF-8 -*-


def test_mysql_common():
    from common.MysqlHandle import MysqlHandle
    from service.mysql_opt.mysql_common import MysqlCommon
    my_handle = MysqlHandle("mysqladmin", "mysqladmin", "172.168.71.55", port=3307)

    result = my_handle.mysql_execute_query_get_all_data("select * from information_schema.INNODB_LOCK_WAITS")
    my_common = MysqlCommon(my_handle)
    result = my_common.get_meta_tables("information_schema", "CHARACTER_SETS")
    print("hello")


def test_mysql_ai_process():
    from service.AISQLReview.mysql_ai_process import MysqlAISQLReview
    from service.AISQLReview.sql_abstract import MysqlSQLStruct
    from common.MysqlHandle import MysqlHandle

    my_handle = MysqlHandle("mysqladmin", "mysqladmin", "172.168.71.55", port=3307)

    my_sql_struct = MysqlSQLStruct()
    my_sql_struct.tenant_code = "lufax"
    my_sql_struct.schema_name = "acclogdata"
    my_sql_struct.sql_text = "select * from acc_log"
    my_sql_struct.mysql_conn = my_handle

    my_ai_sql = MysqlAISQLReview(my_sql_struct)
    result = my_ai_sql.sql_predict()

    print("hello")


def test_parser():
    from service.lu_parser_graph.LUSQLParser import LuSQLParser

    sql_text1 = "select * from ( select a from tab_a) x join b from select( x from tab_b) on x.key1 = b.key2"
    with open('pingan_bank.txt', 'r') as check:
        sql_text2 = check.read()
    lup = LuSQLParser(sql_text2)
    tbs = lup.get_table_name()
    print(tbs)


import datetime
def datetime_to_timestamp(datetime_obj):
    """将本地(local) datetime 格式的时间 (含毫秒) 转为毫秒时间戳
    :param datetime_obj: {datetime}2016-02-25 20:21:04.242000
    :return: 13 位的毫秒时间戳  1456402864242
    """
    local_timestamp = int(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
    return local_timestamp


if __name__ == "__main__":
    import re
    import time
    import json

    test_parser()
    # pass