# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/27 上午10:39
# LAST MODIFIED ON:
# AIM: simulate our environment

from test.test_Utility.Utility_Data_get_from_sql import GetSQL
from test.test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
from service.AISQLReview.sql_abstract import OracleSQLStruct as SQLStruct
from service.oracle_meta.oracle_meta_abstract import OracleTableMeta as TableMeta
from service.predict_sql_review_oracle.InfoHandler import InfoHandler
from service.predict_sql_review_oracle.AIException import SQLHandleError
from service.sql_parser_graph.units import ParseUnit, ParseUnitList
from service.predict_sql_review_oracle.Utility.TableInfoAnalysis import TabelInfoAnaylsis
from service.sql_parser_graph.SQLParser import SQLParser
import pandas
import json
from test.test_Utility.Write_to_error_sql_Database import WriteToDataBase
from typing import Dict, List


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


def get_tab_col(handle_data: InfoHandler) -> Dict[str, List[str]]:
    '''
    get tab_name and col_name from a sql
    :return: [tab_name, [col_names]]
    '''
    sematic_info = handle_data.getData()['sql_text']
    tab_info = handle_data.getData()['tab_info']

    out = dict()
    prsr_unt_lst = sematic_info.elements
    for opt in prsr_unt_lst.by_type['OPT']:
        if opt.in_statement == 'WHERE' and \
                not (has_func_child(opt, prsr_unt_lst) and opt.name in ['IS', 'LIKE']):
            chldrn_id = prsr_unt_lst.find_root(opt)
            for id in chldrn_id:
                child = prsr_unt_lst.by_id[id]
                if child.type == 'COL':
                    col_name = child.name
                    tab_units = [prsr_unt_lst.by_id[i] for i in prsr_unt_lst.find_tab(child, tab_only=True)]
                    tab_names = [ele.name for ele in tab_units]
                    col_tab = tab_info.chck_col_vld(col_name, tab_names)
                    if col_tab is None:
                        continue
                    _, t_id = col_tab
                    tab_name = tab_names[t_id]
                    try:
                        if col_name not in out[tab_name]:
                            out[tab_name].append(col_name)
                    except:
                        out[tab_name] = [col_name]
    return out


def has_func_child(unit: ParseUnit, sem_info: ParseUnitList) -> bool:
    '''
    check if the given OPT unit has a FUNC child on its right
    :param unit:
    :param sem_info:
    :return:
    '''
    for c in unit.edges:
        c_unit = sem_info.by_id[c]
        if c_unit.id < unit.id:
            return False
        if c_unit.type == 'FUNC':
            return True
    return False

def find_in_list(data_list:List[str], value: str) -> int:
    for i, v in enumerate(data_list):
        if v == value:
            return i
    return -1

def update_comb_col_freq(comb_col_freq:str, comb:List[str]) -> str:
    comb_col_freq = json.loads(comb_col_freq)
    comb = ' '.join(v for v in sorted(comb))
    out = []
    found = False
    for comb_col, freq in comb_col_freq:
        if comb_col == comb:
            freq += 1
            found = True
        out.append((comb_col, freq))
    if not found :
        out.append((comb, 1))
    return json.dumps(out)

if __name__ == "__main__":
    sql = 'select id from ai_sr_sql_detail where db_type="ORACLE"'
    server = SQL_SERVER_ADDRESS[3306]
    id_df = GetSQL(sql_text=sql, connect_info=server).get_data()
    id_df = id_df.set_index('id')

    # --- columns --- #
    tab_name = []
    columns = []
    col_distinct = []
    comb_col_freq = []
    numrow = []
    appearance = []
    # --- #
    length = len(id_df)
    cnt = 0
    for id in id_df.index:
        cnt += 1
        sql = f'select  id,sql_text,plan_text,plan_raw,sql_data from ai_sr_sql_detail where id = {id:d}'
        pd_data = GetSQL(sql, server).get_data()
        try:
            value = to_OrackeSQKSTtruct(pd_data.loc[0])
            handle = InfoHandler(value)
        except:
            continue

        tab_col = get_tab_col(handle)
        tab_info = handle.getData()['tab_info']
        print(f'{cnt:d}|{length:d} tab_size:{len(tab_name):d}')
        for tab in tab_col:
            index = find_in_list(tab_name, tab)
            if index == -1:
                tab_name.append(tab)
                columns.append(json.dumps(tab_info.get_all_col_name(tab).tolist()))
                col_distinct.append(json.dumps(tab_info.get_all_col_distinct(tab).tolist()))
                comb_col_freq.append(json.dumps([(' '.join(v for v in sorted(tab_col[tab])),1)]))
                numrow.append(str(tab_info.get_tab_numrows(tab)))
                appearance.append('1')
            else:
                comb_col_freq[index]  = update_comb_col_freq(comb_col_freq[index],tab_col[tab])
                appearance[index] = str(int(appearance[index]) + 1)

    write2sql = WriteToDataBase()
    df = pandas.read_csv('ai_sr_tab_strategy_temp.csv')
    df['tab_name'] = tab_name
    df['columns'] = columns
    df['col_distinct'] = col_distinct
    df['comb_col_freq'] = comb_col_freq
    df['numrow'] = numrow
    df['appearance'] = appearance
    df = df.set_index('tab_name')
    df.to_csv('ai_sr_tab_strategy_temp.csv')
    write2sql.write(dataframe=df, database_name='ai_sr_tab_strategy')

    # from service.predict_table_strategy_oracle.StrategyBuilder import StrategyBuilder
    #
    # builder = StrategyBuilder(csv_address='ai_sr_tab_strategy_temp.csv')
    # builder.load_from_csv()
    # builder.report()
    # builder.build_index_strategy()
    #
    # print('done')
