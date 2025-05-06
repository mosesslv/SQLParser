# CREATED BY: bohuai jiang
# CREATED ON: 2019/9/3
# LAST MODIFIED ON:
# AIM: write ai result to sql
# -*- coding:utf-8 -*-

import sqlalchemy
from test.test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
import json

SQL = f'select * from dbcm.ai_sr_sql_detial_jbh'
# id = 2975
# SQL= f'select * from ai_sr_sql_detail where id = {id:d}'
AI_recommend = []

if __name__ == '__main__':
    info = SQL_SERVER_ADDRESS[3306]
    data = GetSQL(SQL, connect_info=info)
    result = data.get_data()
    result = result.set_index('id')
    for i in result.index:
        print(i)
        value = result.loc[i]
        if type(value.ai_result) == tuple:
            recom = json.loads(value.ai_result[0])['AI_RECOMMEND']
        elif value.ai_result == '':
            recom = ''
        else:
            recom = json.loads(value.ai_result)['AI_RECOMMEND']
        print(recom)
        AI_recommend.append(recom)
    result['AI_RECOMMEND'] = AI_recommend

    database_connection = \
        sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                 format(info['username'], info['passwd'],
                                        info['db_host'], info['database']))
    result.to_sql(con = database_connection,
                  name = 'ai_result_need_dba_review',
                  if_exists='replace')