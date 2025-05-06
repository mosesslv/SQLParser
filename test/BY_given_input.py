# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/10/8
# LAST MODIFIED ON:
# AIM:

import sys

program_path = sys.path[0][0:-5]
if program_path not in sys.path:
    sys.path.append(program_path)

import json
from typing import Optional, List
import pandas

SERVICE_ADD = 3306
DB_TYPE = '\'MYSQL\''
#DB_TYPE = '\'ORACLE\''
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
#from test.Utility.Write_to_error_sql_Database import WriteToDataBase
from test.test_Utility.Utility_Data_get_from_sql import GetSQL
from termcolor import colored
import timeit
import datetime
import copy
import pandas
import os


#####################################
#             load Data             #
#####################################
# ---- sql text --- #
with open('INPUT_SQL','r') as SQL:
    sql_text = SQL.read()


#----- plan raw --- #
with open('INPUT_PLAN_INFO','r') as PLAIN:
    txt = PLAIN.read()
    plan_raw = json.loads(txt)

#---- table info ---- #
with open('INPUT_TAB_INFO','r') as TAB_INFO:
    txt = TAB_INFO.read()
    tab_info_ = json.loads(txt)

# --------------------------------#
#    save data into database     #
# --------------------------------#
# -- check difference -- #
write_to_data_base = False
try:
    with open('check_file', 'r') as check:
        temp_sql = check.read()
    if temp_sql != sql_text:
        write_to_data_base = True
except:
    write_to_data_base = True
    with open('check_file', 'w') as check:
        check.write(sql_text)

#wtb = WriteToDataBase()
try:
    sql = 'select max(id) from ai_review_back_test'
    id = GetSQL(sql).get_data().values[0, 0] + 1
except:
    id = 0

# if write_to_data_base:
#     df = pandas.DataFrame()
#     df['id'] = [id]
#     df['tab_info'] = [json.dumps(tab_info_)]
#     df['plan_raw'] = [json.dumps(plan_raw)]
#     df['sql_text'] = [sql_text]
#
#     df = df.set_index('id')
#     wtb.write(df, 'ai_review_back_test', if_exists='append')


#####################################
#         process function          #
#####################################
def to_OrackeSQKSTtruct() -> SQLStruct:
    v = SQLStruct()
    v.sql_text = sql_text
    if plan_raw == '':
        raise SQLHandleError('执行计划缺失')
    if DB_TYPE == '\'ORACLE\'':
        v.plan_raw = plan_raw
    else:
        v.plan_raw = pandas.DataFrame(plan_raw)
    print(tab_info_)
    table_info = tab_info_
    # table_info = eval(tab_info_)
    ti = table_info['TABLE_INFO']
    #ti = table_info['table_info']
    tab_info = []
    for o in ti:
        oracle_meta_info = TableMeta()
        oracle_meta_info.__dict__ = o
        tab_info.append(oracle_meta_info)
    v.tab_info = tab_info
    #v.view_histogram = table_info['HISTOGRAM']
    #v.addition = table_info['ADDITION']
    return v


def Run_by_id() -> None:
    MOLDE = AI()
    RECOM = RECOMM()
    value = to_OrackeSQKSTtruct()
    print(value.sql_text)
    print(value.plan_text)
    handle = InfoHandler(value)
    data = handle.getData()

    out = MOLDE.predict(data)
    print(out)
    if DB_TYPE == '\'ORACLE\'':
        out, sql = RECOM.predict(data)
    else:
        out = RECOM.predict(data)
    print(out)



if __name__ == '__main__':
    Run_by_id()

