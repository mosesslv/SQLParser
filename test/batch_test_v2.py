# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/10/28
# LAST MODIFIED ON: 2019/10/28 13:34
# AIM: over all analysis
import sys

program_path = sys.path[0][0:-5]
if program_path not in sys.path:
    sys.path.append(program_path)

import json
from typing import Optional, List
import pandas

SERVICE_ADD = 3306
OVER_WRITE = 1
from service.AISQLReview.sql_abstract import OracleSQLStruct as SQLStruct
from service.oracle_meta.oracle_meta_abstract import OracleTableMeta as TableMeta
from service.predict_sql_review_oracle.InfoHandler import InfoHandler
from service.predict_sql_review_oracle.SQL_REVIEW import REVIEW
from service.predict_sql_review_oracle.AIException import SQLHandleError

from test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
from test_Utility.Utility_Data_get_from_sql import GetSQL
from termcolor import colored
from multiprocessing import Pool
import time
from functools import partial
import os
import timeit
import datetime
import copy
from typing import Tuple

def to_OrackeSQKSTtruct(input: pandas.Series) -> SQLStruct:
    v = SQLStruct()
    v.sql_text = input.sql_text
    v.plan_text = input.plan_text
    if input.plan_raw == '':
        raise SQLHandleError('执行计划缺失')

    v.plan_raw = json.loads(input.plan_raw)
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


def get_tab_info_index_info(string):
    tab_info = []
    index_info = dict()
    for v in string:
        # -- get table name -- #
        name = v['table_name']
        index_info[name] = copy.copy(v['idx_info'])
        # -- remove idx_info from table info --#
        del v['idx_info']
        tab_info.append(v)
    return tab_info, index_info


def add_to_dict(dict_name: dict, key: object, value: object):
    try:
        dict_name[key].append(value)
    except:
        dict_name[key] = [value]


def Run_by_id(id: int) -> None:
    MOLDE = REVIEW()
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    sql = f'select id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail' \
          f' where id = {id:d}'
    pd_data = GetSQL(sql, server).get_data()

    value = to_OrackeSQKSTtruct(pd_data.loc[0])
    handle = InfoHandler(value)
    data = handle.getData()

    out = MOLDE.predict(data)

    print(out)


def Run_test(num_process: int = 8) -> dict:
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    sql = f'select id from ai_sr_sql_detail where db_type="ORACLE"'
    id_list = GetSQL(sql, server).get_data().values[:, 0]
    result = dict()
    total_size = len(id_list)
    result['total_data_size'] = total_size

    #if DB_TYPE == '\'ORACLE\'':
    MODEL = REVIEW()
    # else:
    #     MODEL = REVIEW()
    # --- run multiprocess --- #
    pool = Pool(num_process)

    process_start = timeit.default_timer()
    for cnt,id in enumerate(id_list):
        #if DB_TYPE == '\'ORACLE\'':
        pool.apply_async(partial(Run_single_thread, MODEL=MODEL), (id,),
                         callback=partial(save_to_dict, result=result, cnt=cnt, total_size=total_size,
                                          process_start=process_start))
        # else:
        #     pool.apply_async(partial(Run_single_thread_mysql, MODEL=MODEL), (id,),
        #                      callback=partial(save_to_dict, result=result, cnt=cnt, total_size=total_size,
        #                                       process_start=process_start))

    pool.close()
    pool.join()
    # analysis(result)
    return result


#################################
#      concurrent function      #
#################################
def save_to_dict(input: Tuple[dict, bool], result: dict, cnt: int, total_size: int, process_start: float):
    input, statu = input
    statu = colored('success','green') if statu else colored('fail', 'red')
    for key in input.keys():
        value_list = input[key]
        for v in value_list:
            add_to_dict(result, key, v)
    np.save(f'result.npy', result)
    process_end = timeit.default_timer()
    process_lapses = process_end - process_start
    average_cost = process_lapses / float(cnt+1)
    process_remand_time = round((total_size - cnt) * average_cost, 2)
    print(f'{cnt:d}|{total_size:d},remand time:',
          str(datetime.timedelta(seconds=process_remand_time)),
          'average time consume: ', str(datetime.timedelta(seconds=average_cost)), statu)

def Run_single_thread(id: int, MODEL: "REVIEW") -> Tuple[dict, bool]:
    result = dict()
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    sql = f'select id,sql_text,plan_text,plan_raw,sql_data,db_type from ai_sr_sql_detail' \
          f' where id = {id:d}'
    pd_data = GetSQL(sql, server).get_data()
    success = True
    for ii in pd_data.index:
        value = pd_data.iloc[ii]
        id = value.id
        try:
            value = to_OrackeSQKSTtruct(value)
            handle = InfoHandler(value)
            data = handle.getData()
            try:
                out = MODEL.predict(data)
                if type(out) == tuple:
                    message = out[1]
                    out = out[0]
                if out == 0:
                    add_to_dict(dict_name=result, key='FAIL', value=id)
                else:
                    add_to_dict(dict_name=result, key='PASS', value=id)
            except Exception as e:
                ex_type = str(type(e)).replace('>', '').replace('\'', '').split('.')
                add_to_dict(dict_name=result, key='DISC-' + ex_type[-1], value=id)
                success = False
            # try:
            #     RECOM.predict(data)
            #     add_to_dict(dict_name=result, key='RECOM', value=id)
            # except Exception as e:
            #     ex_type = str(type(e)).replace('>', '').replace('\'', '').split('.')
            #     add_to_dict(dict_name=result, key='RECOM-' + ex_type[-1], value=id)
            #     success = False

        except Exception as e:
            ex_type = str(type(e)).replace('>', '').replace('<', '').replace('\'', '').split('.')
            if isinstance(e, SQLHandleError):
                add_to_dict(dict_name=result, key=str(e), value=id)
            else:
                add_to_dict(dict_name=result, key='DATA-' + ex_type[-1], value=id)
            success = False
    return result,success

# def Run_single_thread_mysql(id: int, MODEL: "REVIEW") -> dict:
#     result = dict()
#     server = SQL_SERVER_ADDRESS[SERVICE_ADD]
#     sql = f'select id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail' \
#           f' where id = {id:d}'
#     pd_data = GetSQL(sql, server).get_data()
#     for ii in pd_data.index:
#         value = pd_data.iloc[ii]
#         id = value.id
#         try:
#             value = to_OrackeSQKSTtruct(value)
#             handle = InfoHandler(value)
#             data = handle.getData()
#             try:
#                 out, _ = MODEL.predict(data)
#                 if type(out) == tuple:
#                     message = out[1]
#                     out = out[0]
#                 if out == 0:
#                     add_to_dict(dict_name=result, key='FAIL', value=id)
#                 else:
#                     add_to_dict(dict_name=result, key='PASS', value=id)
#                 add_to_dict(dict_name=result, key='RECOM', value=id)
#             except Exception as e:
#                 ex_type = str(type(e)).replace('>', '').replace('\'', '').split('.')
#                 add_to_dict(dict_name=result, key='MODEL-' + ex_type[-1], value=id)
#
#
#         except Exception as e:
#             ex_type = str(type(e)).replace('>', '').replace('<', '').replace('\'', '').split('.')
#             if isinstance(e, SQLHandleError):
#                 add_to_dict(dict_name=result, key=str(e), value=id)
#             else:
#                 add_to_dict(dict_name=result, key='DATA-' + ex_type[-1], value=id)
#     return result



################################

def report():
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    sql = 'select db_type, count(*) from ai_sr_sql_detail group by db_type'
    pd_data = GetSQL(sql, server).get_data()
    print('=====================')
    print(pd_data)
    print('=====================')


def analysis(result: dict):
    result_ = copy.copy(result)
    print('==========================')
    print('         analysis         ')
    print('==========================')
    total_size = result_['total_data_size']
    print('total_data_size: ', total_size)
    try:
        print('PASS', f' {len(result_["PASS"]) / float(total_size) * 100:0.2f}%')
    except:
        pass
    try:
        print('FAIL', f' {len(result_["FAIL"]) / float(total_size) * 100:0.2f}%')
    except:
        pass
    try:
        print('RECOM', f' {len(result_["RECOM"]) / float(total_size) * 100:0.2f}%')
    except:
        pass
    for k in result_.keys():
        value = result_[k]
        if type(value) == list and k not in ['PASS', 'FAIL', 'RECOM']:
            print(k, f' {len(value) / float(total_size) * 100:0.3f}%')


def Run_by_id_list(id_list: List[int]):
    server = SQL_SERVER_ADDRESS[SERVICE_ADD]
    # if DB_TYPE == '\'ORACLE\'':
    #MOLDE = AI()
    #RECOM = RECOMM()
    # else:
    #     MODEL = REVIEW()

    m = len(id_list)
    m_i = 0
    while m_i < m:
        #m_i = m_i % m
        sql = f'select id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail' \
              f' where id = {id_list[m_i]:d}'
        pd_data = GetSQL(sql, server).get_data()
        value = pd_data.iloc[0]
        # -- start server --- #
        print(colored(value.sql_text, 'yellow'))
        print(colored(value.plan_text, 'yellow'))
        print('index:{}, {}|{}'.format(id_list[m_i], m_i, m))

        Run_by_id(id_list[m_i])
        m_i += 1


if __name__ == '__main__':
    import numpy as np

    run_analysis = 1   # False
    report()
    if run_analysis:
        result = Run_test(4)
        analysis(result)
    else:
        result = np.load(f'result.npy', allow_pickle=True).item()
        analysis(result)
        value_list = result['DISC-<class TypeError']

        Run_by_id_list(value_list)
        #Run_by_id_list([29214])
        # Run_by_id_list([9288])
