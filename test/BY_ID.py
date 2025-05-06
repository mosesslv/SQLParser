# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/10/8
# LAST MODIFIED ON:
# AIM:

import sys

program_path = sys.path[0][0:-5]
if program_path not in sys.path:
    sys.path.append(program_path)
print('program_path',sys.path[0])
import json
from typing import Optional, List
import pandas

SERVICE_ADD = 3306
# DB_TYPE = '\'MYSQL\''
DB_TYPE = '\'ORACLE\''
OVER_WRITE = 1

if DB_TYPE == '\'ORACLE\'':
    from service.AISQLReview.sql_abstract import OracleSQLStruct as SQLStruct
    from service.oracle_meta.oracle_meta_abstract import OracleTableMeta as TableMeta
    from service.predict_sql_review_oracle.InfoHandler import InfoHandler
    from service.predict_sql_review_oracle.P_DISCRIMINATOR import AI
    from service.predict_sql_review_oracle.P_RECOMMENDATION import RECOMM
    from service.predict_sql_review_oracle.AIException import SQLHandleError
if DB_TYPE == '\'MYSQL\'':
    from service.AISQLReview.sql_abstract import MysqlSQLStruct as SQLStruct
    from service.mysql_meta.mysql_meta_abstract import MysqlTableMeta as TableMeta
    from service.predict_sql_review_mysql.InfoHandler import InfoHandler
    from service.predict_sql_review_mysql.P_DISCRIMINATOR import AI
    from service.predict_sql_review_mysql.P_RECOMMENDATION import RECOMM
    from service.predict_sql_review_mysql.AIException import SQLHandleError

from test.test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
from test.test_Utility.Utility_Data_get_from_sql import GetSQL
# from test.Utility.Write_to_error_sql_Database import WriteToDataBase
from test.test_Utility.Utility_Data_get_from_sql import GetSQL
from termcolor import colored
import timeit
import datetime
import copy
import pandas
import os


def to_OrackeSQKSTtruct(input: pandas.Series) -> SQLStruct:
    v = SQLStruct()
    v.sql_text = input.sql_text
    v.plan_text = input.plan_text
    if input.plan_raw == '':
        raise SQLHandleError('执行计划缺失')
    if DB_TYPE == '\'ORACLE\'':
        v.plan_raw = json.loads(input.plan_raw)
    else:
        v.plan_raw = pandas.DataFrame(json.loads(input.plan_raw))
    sql_data = json.loads(input.sql_data)
    ti = sql_data['TABLE_INFO']
    tab_info = []
    for o in ti:
        oracle_meta_info = TableMeta()
        oracle_meta_info.__dict__ = o
        tab_info.append(oracle_meta_info)
    v.tab_info = tab_info
    v.view_histogram = sql_data['HISTOGRAM']
    v.addition = sql_data['ADDITION']
    return v


def Run_by_id(id: int) -> None:
    MOLDE = AI()
    RECOM = RECOMM()
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    sql = f' /*+index(l,IFK_LOAN_LOANEE)*/ ' \
          f'select id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail' \
          f' where id = {id:d}'
    pd_data = GetSQL(sql, server).get_data()

    value = to_OrackeSQKSTtruct(pd_data.loc[0])
    print(colored(value.sql_text, 'yellow'))
    print(colored(value.plan_text, 'yellow'))
    handle = InfoHandler(value)
    data = handle.getData()

    out = MOLDE.predict(data)
    if DB_TYPE == '\'MYSQL\'':
        print('PASS' if out[0] == 1 else 'FAIL')
    if DB_TYPE == '\'ORACLE\'':
        print('PASS' if out == 1 else 'FAIL')

    print(RECOM.predict(data))


if __name__ == '__main__':
    Run_by_id(144)
