# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019 /9/29
# LAST MODIFIED ON:
# AIM: test single sql
import sys

program_path = sys.path[0][0:-5]
if program_path not in sys.path:
    sys.path.append(program_path)

import json
from typing import Optional, List
import pandas

SERVICE_ADD = 3306
DB_TYPE = '\'MYSQL\''
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
    from service.predict_sql_review_mysql.SQL_REVIEW import REVIEW
    from service.predict_sql_review_mysql.AIException import SQLHandleError

from test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
from test_Utility.Utility_Data_get_from_sql import GetSQL
from termcolor import colored
import timeit
import datetime
import copy
import json


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


def Run_by_id(id: int):
    buffer = ''
    MOLDE = AI()
    RECOM = RECOMM()
    # if DB_TYPE == '\'ORACLE\'':
    #     MOLDE = AI()
    #     RECOM = RECOMM()
    # else:
    #     MODEL = REVIEW()
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    sql = f'select id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail' \
          f' where id = {id:d}'
    pd_data = GetSQL(sql, server).get_data()

    value = to_OrackeSQKSTtruct(pd_data.loc[0])
    buffer += value.sql_text + '\n'
    buffer += value.plan_text + '\n'
    handle = InfoHandler(value)
    data = handle.getData()

    out1 = MOLDE.predict(data)
    buffer += json.dumps(out1) + '\n'
    out2 = RECOM.predict(data)
    prob_tag = RECOM.get_problem_tag(out2)
    buffer += out2 + '\n'

    # if DB_TYPE == '\'ORACLE\'':
    #     out1 = MOLDE.predict(data)
    #     print(out1)
    #     out2,_ = RECOM.predict(data)
    #     print(out2)
    # else:
    #     grand, recom = MODEL.predict(data)
    #     print(grand)
    #     print(recom)
    return buffer, ' '.join(v for v in prob_tag)


if __name__ == '__main__':
    # Run_by_id(857)

    sql = """
     SELECT * FROM dbcm.ai_sr_sql_detail where db_type = 'MYSQL' and ai_result like "%NOPASS%";
         """
    # sql = """
    # SELECT * FROM dbcm.ai_sr_sql_detail where db_type = 'ORACLE' and sql_text like '%rownum%';
    # """
    #    sql = """
    # select id from ai_sr_sql_detail_jbh where db_type = 'ORACLE' and tab_num = 1
    #   """

    data = GetSQL(sql, SQL_SERVER_ADDRESS[3306]).get_data()
    m = data.shape[0]
    m_i = 0
    check_list = []
    cnt = 0
    while m_i < m:
        #m_i = m_i %  m
        id = data.iloc[m_i, :][0]
        print(f'id:{id:d}[{m_i:d}|{m:d}]')
        # m_i += 1
        # try:
        #     Run_by_id(id)
        # except Exception as e:
        #     print(e)
        try:
            out, prob_tag = Run_by_id(id)
        except:
            m_i += 1
            continue

        # print(out)
        if prob_tag not in check_list:
            with open('sample.txt', 'a') as f:
                f.write(out)
            #check_list.append(prob_tag)
            cnt +=1
            print(out)
        if cnt > 100:
            break
        m_i += 1
        # v = input()
        # if v.upper() == 'N':
        #     m_i += 1
        # if v.upper() == 'P':
        #     m_i -= 1
        # if v.upper() == 'Q':
        #     break
