# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/9/10
# LAST MODIFIED ON:
# AIM: test sql by id
from test.test_Utility.Utility_Data_get_from_sql import GetSQL
from test.test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.oracle_meta.oracle_meta_abstract import OracleTableMeta
from service.lu_parser_graph.SQLmodifier import SQLmodifier
import pandas

import json


def to_OrackeSQKSTtruct(input: pandas.Series) -> OracleSQLStruct:
    v = OracleSQLStruct()
    v.sql_text = input.sql_text
    v.plan_text = input.plan_text
    v.plan_raw = json.loads(input.plan_raw)
    sql_data = json.loads(input.sql_data)
    ti = sql_data['TABLE_INFO']
    tab_info = []
    for o in ti:
        oracle_meta_info = OracleTableMeta()
        oracle_meta_info.__dict__ = o
        tab_info.append(oracle_meta_info)
    v.tab_info = tab_info
    v.view_histogram = sql_data['HISTOGRAM']
    v.addition = sql_data['ADDITION']
    return v

if __name__ == '__main__':
    id = 1
    server = SQL_SERVER_ADDRESS[3306]
    sql = f'select id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail where id = {id:d}'
    pd_data = GetSQL(sql, server).get_data()
    value =  pd_data.iloc[0]

    value = to_OrackeSQKSTtruct(value)
    sql = '''
/*@ files=CollectionRecord.xml namespace=CollectionRecord id=getCollectionRecordByCollectionPLanId  @*/
        select /*+index(CR,COLLECT_REC_PLANID_IND)*/
        CR.COLLECTION_PLAN_ID as collectionPlanId,
        CR.RECORD_NUMBER as recordNumber,
        CR.AMOUNT as amount,
        CR.PRINCIPAL as principal,
        CR.INTEREST as interest,
        CR.PENAL_VALUE as penalValue,
        CR.OVERDUE_PENAL_VALUE as overduePenalValue,
        CR.STATUS as status,
        CR.CREATED_AT as createdAt,
        CR.ID as id,
        CR.MANAGEMENT_FEE as managementFee,
        CR.COLLECTION_SOURCE as collectionSource,
        CR.INVESTMENT_FEE as investmentFee
        from COLLECTION_RECORDS CR
        where CR.COLLECTION_PLAN_ID = :P_collectionPLanId and CR.XXXX = 10
    '''
    sql = '''
/*@ files=CollectionRecord.xml namespace=CollectionRecord id=getCollectionRecordByCollectionPLanId  @*/
        select /*+index(CR,COLLECT_REC_PLANID_IND)*/
        CR.XXXX
        from COLLECTION_RECORDS CR
    '''
    value.sql_text = sql
    print(value.sql_text)


    sp = SQLmodifier(value)
    sp.display_elements()
    sql = sp.check_sqlmap_missing()

    print(sql)


