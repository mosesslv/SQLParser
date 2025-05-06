# -*- coding:utf-8 -*-
# created by bohuai jiang
# on 2019/8/13
# last modified on: 2019/9/9 15:28

from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnit
import re
from typing import List
from itertools import chain, combinations
from typing import Optional, Tuple, List, Dict, Tuple, List, Union
from sqlparse.keywords import is_keyword
import numpy as np
import sqlparse
import datetime

THRESHOLD = 1000
DIFF_THETA = 0.4


class LuSQLParser(SQLParser):
    def __init__(self, sql, **kwargs):
        super().__init__(sql, **kwargs)

    def sql_has_dynamic_mosaicking(self): # 检测动态拼接
        empty_sql_regular = "\/\*isNotEmpty.*\*\/"
        empty_match_list = re.findall(re.compile(empty_sql_regular, re.I), self.origin_sql)

        sql_regular = "\/\*isNotNull.*\*\/"
        null_match_list = re.findall(re.compile(sql_regular, re.I), self.origin_sql)

        if (empty_match_list is not None and len(empty_match_list) > 0) or \
                (null_match_list is not None and len(null_match_list) > 0):
            return True
        else:
            return False

    def has_star_in_SELECT(self) -> bool:
        for t in self.tokens:
            if '*' in t.value:
                return True
        return False

    #######################################
    #         for data transmit           #
    #######################################
    def get_addition(self, table_info: dict) -> List[dict]:
        out = []
        tab_col = dict()
        for opt in self.elements.by_type['OPT']:
            col_ids = self.elements.find_root(opt, roots=[])
            for id in col_ids:
                each_col = self.elements.by_id[id]
                col_name = each_col.name
                tab_name = [self.elements.by_id[i].name for i in
                            self.elements.find_tab(each_col, tabs=[], tab_only=True)]
                values = self.__check_col_valid(col_name, tab_name, table_info)
                if values is None: continue
                col_name, tab_name = values
                if tab_name in tab_col.keys():
                    tab_col[tab_name].append(col_name)
                else:
                    tab_col[tab_name] = [col_name]

        for tab in tab_col.keys():
            cols = tab_col[tab]
            m = len(cols)
            if m > 1:
                all_comb = chain.from_iterable(combinations(cols, r) for r in range(2, m + 1))
                for comb in all_comb:
                    container = {'col_name': comb, 'sql_text': self.__to_sql(cols, tab)}
                    out.append(container)
        return out

    def __check_col_valid(self, col: str, tabs: List[str], table_info: dict) -> Optional[Tuple[str, str]]:
        for info in table_info:
            if info['table_name'].upper() in tabs:
                for v in info['table_columns']:
                    if col.upper() == v['col_name'].upper():
                        return col.upper(), info['table_name'].upper()
        return None

    def __to_sql(self, col_list: List[str], tab: str):
        out = 'select count(*) from ( select '
        col_verbose = ''
        for col in col_list:
            col_verbose += col + ','
        out += col_verbose[0:-1] + ' from ' + tab + ' group by '
        out += col_verbose[0:-1] + ') a where rownum <= 100000'
        return out

    #######################################
    #           get table name            #
    #######################################
    def get_table_name(self, *args, alias_on=False, view_on=False) -> Union[Tuple[List[str], List[str]], List[str]]:
        out = super().get_table_name(alias_on=alias_on, view_on=view_on)
        try:
            return [v.replace('\"', "") for v in out]
        except:
            return out

    #######################################
    #      searching bind variables       #
    #######################################
    # ---- fetch bind variables ---#
    def _get_bindVariable_in(self, Unit: ParseUnit) -> Optional[List[str]]:
        place_holder = []
        roots = self.elements.find_root(graph=Unit, col_only=False)
        for r in roots:
            unit = self.elements.by_id[r]
            type = unit.type
            name = unit.name
            if type in ["STRUCT", "VALUE"] and name not in '(,)[]':
                place_holder.append(name)

        return place_holder

    def _get_bindVariable_value(self, Unit: ParseUnit) -> Optional[Dict[int, List[str]]]:
        '''

        :param Unit:
        :return: (level , values[])
        '''
        place_holder = dict()
        bd_egdes = sorted(list(Unit.edges)[1::])
        count = 0
        for id in bd_egdes:
            unit = self.elements.by_id[id]
            type = unit.type
            if type == "SUB":
                roots = sorted(unit.edges)
                bvs = []
                for r in roots:
                    r_unit = self.elements.by_id[r]
                    r_type = r_unit.type
                    r_name = r_unit.name
                    if r_type in ["STRUCT", "VALUE", "FUNC"] and r_name not in '(,)[]':
                        bvs.append(r_name)
                place_holder[count] = bvs
                count += 1

        return place_holder

    def _get_bindVariable_opt(self, Unit: ParseUnit) -> Optional[List[str]]:
        place_holder = []
        for id in Unit.edges:
            unit = self.elements.by_id[id]
            type = unit.type
            name = unit.name
            if type in ["STRUCT", "VALUE"] and name not in '(,)[]':
                place_holder.append(name)
            if type == 'FUNC':
                place_holder.append([v.value for v in unit.token.parent.flatten()])

        return place_holder

    def _get_bindVariable_Name(self, Unit: ParseUnit) -> Optional[List[str]]:
        name = Unit.name
        if name == 'IN':
            return self._get_bindVariable_in(Unit)
        elif name == 'VALUES':
            return self._get_bindVariable_value(Unit)
        else:
            return self._get_bindVariable_opt(Unit)

    def _add_bindVariable_output(self, col_ids: List[int], bv: List[str]) -> \
            Optional[List[Dict[str, str]]]:
        if len(col_ids) == len(bv):
            result = []
            col_ids = np.sort(list(col_ids))

            for c_id, bv_name in zip(col_ids, bv):
                col_name = self.elements.by_id[c_id].name
                tab_name = []
                for i in self.elements.find_tab(self.elements.by_id[c_id], tab_only=True):
                    a_name = self.elements.by_id[i].name
                    if '(' not in a_name:
                        tab_name.append(a_name)
                result.append({
                    'tab_name': tab_name,
                    'col_name': col_name,
                    'bv_name': bv_name
                })
            return result
        else:
            result = []
            if len(col_ids) == 1:
                # case, len(col_ids) == 1  < len(bv)
                col = col_ids[0]
                tab_name = []
                for i in self.elements.find_tab(self.elements.by_id[col], tab_only=True):
                    a_name = self.elements.by_id[i].name
                    if '(' not in a_name:
                        tab_name.append(a_name)
                for e_bv in bv:
                    result.append({
                        'tab_name': tab_name,
                        'col_name': self.elements.by_id[col].name,
                        'bv_name': e_bv,
                    })
                return result
        return []

    def get_bindVariable_info(self) -> List[dict]:
        result = []
        for opt in self.elements.by_type['OPT']:
            bv = self._get_bindVariable_Name(opt)
            if bv:
                col_ids = self.elements.find_root(opt, col_only=True)
                if type(bv) == list:
                    result.extend(self._add_bindVariable_output(col_ids, bv))
                if type(bv) == dict:
                    for key in bv:
                        e_bv = bv[key]
                        result.extend(self._add_bindVariable_output(col_ids, e_bv))
        return result

    # --- reconstruct sql --- #
    def sql_replace_bind_variable(self, table_metas: List[dict], data_map: dict) -> str:
        map_table = self.get_bindVariable_info()
        view_map_done = False
        if len(map_table) == 0:
            view_map_done = True
        sql = ''
        count = 0
        tokens = sqlparse.parse(self.origin_sql)
        sql_tokens = [t for t in self.token_walk(tokens, True, False)]
        i = 0
        while i < len(sql_tokens):
            token = sql_tokens[i]
            if not view_map_done:
                tab_name = map_table[count]['tab_name']
                col_name = map_table[count]['col_name']
                bv_name = map_table[count]['bv_name']
                chck_value = bv_name[0] if type(bv_name) == list else bv_name
            if not view_map_done and token.value.upper() == chck_value:
                is_ph = False
                if isinstance(bv_name, list):
                    for v in bv_name:
                        if ':P' in v:
                            is_ph = True
                            break
                else:
                    if ':P' in bv_name:
                        is_ph = True
                if is_ph:
                    value = self._get_bind_variable_values(table_metas, data_map, col_name, tab_name)
                    if value:
                        sql += self._get_bind_variable_values(table_metas, data_map, col_name, tab_name)
                    else:
                        sql += 'dummy'
                    if type(bv_name) == list:
                        i += len(bv_name)
                    else:
                        i += 1
                else:
                    sql += token.value
                    i += 1
                count += 1
                if count >= len(map_table):
                    view_map_done = True
            else:
                # capture rest bv
                value = token.value
                if ':P' in value and 'Comment' not in str(token.ttype):
                    sql += '100'
                else:
                    sql += value
                i += 1
        return sql

    def _get_bind_variable_values(self, table_metas: List['TableMeta'], data_map: dict, col_name: str,
                                  tab_name_list: List[str]) -> str:
        for metas in table_metas:
            if metas.table_name in tab_name_list:
                for id, line in enumerate(metas.table_columns):
                    if col_name.upper() == line['col_name'].upper():
                        found_error = False
                        real_data = None
                        try:
                            real_data = data_map[metas.table_name][id]
                            if real_data is None:
                                found_error = True
                        except:
                            found_error = True
                        col_type = line["col_type"].upper()
                        if col_type.startswith("VARCHAR"):
                            value = "'{0}'".format(real_data if not found_error else 'dummy')
                        elif col_type.startswith("NUMBER") or col_type.startswith("INT") or \
                                col_type.startswith("DECIMAL") or col_type.startswith("BIGINT"):
                            value = str(real_data) if not found_error else '1'
                        elif col_type.startswith("DATETIME"):
                            value = "'{0}'".format(
                                real_data if not found_error else datetime.datetime.now().strftime("%Y-%m-%d %H:%m"))
                        else:
                            value = "'{0}'".format(real_data if not found_error else 'dummy')
                        return value


def get_expection_list(table_metas):
    # --- get exception_list --- #
    exception_list = []
    for meta in table_metas:
        for col_info in meta.table_columns:
            col_name = col_info['col_name'].upper()
            if str(is_keyword(col_name)[0]) == 'Token.Keyword':
                exception_list.append(col_name)

    return exception_list


class TableMeta:
    """
    META 信息, 表的元数据信息都在这里定义
    """
    db_type = ""  # 数据库类型 ORACLE MYSQL HIVE
    instance_name = ""  # ORACLE - instance name; MYSQL - ip:port;
    schema_name = ""  # schema name
    table_name = ""  # table name
    table_comments = ""  # comments
    table_seq_name = ""  # table sequence name for oracle
    tab_synonym_created = 0  # table synonym created for oracle
    seq_synonym_created = 0  # sequence synonym created for oracle
    opr_select_granted = 0  # opr user select granted
    opr_insert_granted = 0  # opr user insert granted
    opr_update_granted = 0  # opr user update granted
    rnd_select_granted = 0  # rnd user select granted
    seq_opr_select_granted = 0  # opr user sqeuqnce select granted for oracle
    table_is_partitioned = 0  # table is partitioned
    table_partition_methord = ""  # partition methord
    table_partition_keys = []  # partition columns
    table_partition_interval = ""  # interval
    table_columns = []  # table columns list :
    # {'col_name':'','col_type':'', 'col_null':'', 'default_value':'', 'col_comments':''}
    table_indexes = []  # table index list
    # {'idx_name':'','idx_cols':'', 'idx_part':'' ,'idx_constraint_name':'','idx_constraint_type':''}
    table_privileges = []  # table special grant list :{'priv_grantee':'','priv_privilege':''}

    def __init__(self):
        pass


if __name__ == '__main__':
    import json
    import pandas

    sql = """
/*@ files=be_IcashWithdrawalsRec_mysql.xml namespace=IcashWithdrawalsRec id=updateToExportBatch @*/
update WMC_ICASH_WITHDRAWALS_REC
set is_export = '1',
export_time = now(),
lu_bank_account_code = :P_luBankAccountCode,
lu_bank_account_no = :P_luBankAccountNo,
vendor_code = :P_vendorCode,
export_batch_id = :P_exportBatchId,
updated_at = now(),
updated_by =  :P_userType
where is_export =  :P_userType
and status =  :P_userType
and aml_status =  :P_userType
and withdraw_status=  :P_userType
and user_type = :P_userType
and created_at >= str_to_date(:P_applyDateStart , '%Y-%m-%d %H:%i:%s')
and created_at <= str_to_date(:P_applyDateEnd , '%Y-%m-%d %H:%i:%s')
limit :P_limitCount

       """

    table_metas = """
[{"db_type": "MYSQL", "table_name": 
"WMC_ICASH_WITHDRAWALS_REC", "table_columns": 
[{"default_value": "", "col_name": "ID", "col_type": "BIGINT(20)",
 "col_null": "NOT NULL", "col_comments": "\u4e3b\u952e"}, 
 {"default_value": "CURRENT_TIMESTAMP", "col_name": "CREATED_AT",
  "col_type": "DATETIME", "col_null": "NOT NULL",
   "col_comments": "\u521b\u5efa\u65f6\u95f4"},
    {"default_value": "SYS", "col_name": "CREATED_BY", "col_type": "VARCHAR(128)", 
    "col_null": "NOT NULL", "col_comments": "\u521b\u5efa\u4eba"}, 
    {"default_value": "CURRENT_TIMESTAMP", "col_name": "UPDATED_AT", 
    "col_type": "DATETIME", "col_null": "NOT NULL", "col_comments": "\u4fee\u6539\u65f6\u95f4"},
     {"default_value": "SYS", "col_name": "UPDATED_BY", "col_type": "VARCHAR(128)", 
     "col_null": "NOT NULL", "col_comments": "\u4fee\u6539\u4eba"}, {"default_value": "", 
     "col_name": "USER_ID", "col_type": "BIGINT(20)", "col_null": "NOT NULL",
      "col_comments": "\u53d6\u73b0\u7528\u6237id"}, {"default_value": "", 
      "col_name": "CURRENCY", "col_type": "VARCHAR(32)", "col_null": "NOT NULL", 
      "col_comments": "\u5e01\u79cd"}, {"default_value": "",
       "col_name": "USER_BANK_ACCOUNT_NAME", "col_type": "VARCHAR(512)", 
       "col_null": "NOT NULL", "col_comments": "\u7528\u6237\u94f6\u884c\u5361\u59d3\u540d"}, 
       {"default_value": "", "col_name": "STATUS", "col_type": "VARCHAR(32)",
        "col_null": "NOT NULL", "col_comments": "\u8bf7\u6c42\u8bb0\u5f55\u72b6\u6001 1\uff1a\u5904\u7406\u4e2d 2 \u6210\u529f 3 \u5931\u8d25 4 \u5931\u6548"}, 
        {"default_value": "", "col_name": "AMOUNT", "col_type": "DECIMAL(19,2)", "col_null": "NOT NULL",
         "col_comments": "\u53d6\u73b0\u91d1\u989d"}, {"default_value": "", "col_name": "FEE_AMOUNT", 
         "col_type": "DECIMAL(19,2)", "col_null": "NOT NULL", "col_comments": "\u624b\u7eed\u8d39\uff08\u603b\u989d\uff09"},
          {"default_value": "", "col_name": "REMARK", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u5907\u6ce8"},
           {"default_value": "", "col_name": "ICASH_BANK_TRANSACTION_ID", "col_type": "BIGINT(20)", "col_null": "",
            "col_comments": "\u7f51\u94f6\u6d41\u6c34\u8868id"}, {"default_value": "", "col_name": "BANK_TRANSACTION_NO", 
            "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u94f6\u884c\u6d41\u6c34\u53f7"},
             {"default_value": "", "col_name": "USER_BANK_ACCOUNT_NO", 
             "col_type": "VARCHAR(128)", "col_null": "NOT NULL",
              "col_comments": "\u7528\u6237\u94f6\u884c\u5361\u53f7"},
               {"default_value": "", "col_name": "LU_BANK_ACCOUNT_NO", "col_type": "VARCHAR(128)",
                "col_null": "", "col_comments": "\u9646\u56fd\u9645\u94f6\u884c\u5361\u53f7"}, 
                {"default_value": "", "col_name": "SWIFT_CODE", 
                "col_type": "VARCHAR(128)", "col_null": "NOT NULL", "col_comments": "\u7528\u6237swiftcode"},
                 {"default_value": "", "col_name": "FINISH_TIME", "col_type": "DATETIME", "col_null": "", 
                 "col_comments": "\u5339\u914d\u5b8c\u6210\u65f6\u95f4"}, {"default_value": "",
                  "col_name": "REMOTE_STATUS", "col_type": "VARCHAR(32)", "col_null": "",
                   "col_comments": "\u8fdc\u7a0b\u8c03\u7528\u72b6\u6001"}, 
                   {"default_value": "", "col_name": "REMOTE_MSG", 
                   "col_type": "VARCHAR(4000)", "col_null": "", 
                   "col_comments": "\u8fdc\u7a0b\u8fd4\u56de\u4fe1\u606f"},
                    {"default_value": "", "col_name": "FROZEN_NO", "col_type": "VARCHAR(32)",
                     "col_null": "", "col_comments": "\u5e9f\u5f03\u5b57\u6bb5"}, 
                     {"default_value": "", "col_name": "IS_EXPORT", 
                     "col_type": "VARCHAR(1)", "col_null": "NOT NULL",
                      "col_comments": "\u662f\u5426\u5df2\u5bfc\u51fa\uff0c1\uff1a\u662f\uff0c0\uff1a\u5426"}, 
                      {"default_value": "", "col_name": "BANK_TRANSACTION_TIME",
                       "col_type": "VARCHAR(32)", "col_null": "",
                        "col_comments": "\u94f6\u884c\u6d41\u6c34\u8f6c\u8d26\u65f6\u95f4"}, 
                        {"default_value": "", 
                        "col_name": "EXPORT_TIME", "col_type": "DATETIME",
                         "col_null": "", "col_comments": "\u8bb0\u5f55\u5bfc\u51fa\u65f6\u95f4"},
                          {"default_value": "", "col_name": "ACTUAL_AMOUNT", "col_type": "DECIMAL(19,2)", 
                          "col_null": "", "col_comments": "\u5b9e\u9645\u8f6c\u8d26\u91d1\u989d"},
                           {"default_value": "", "col_name": "CHANNEL_ID", "col_type": "VARCHAR(128)",
                            "col_null": "NOT NULL", "col_comments": "\u8c03\u7528\u65b9\u6807\u8bc6"}, 
                            {"default_value": "", "col_name": "AML_STATUS", "col_type": "VARCHAR(1)", 
                            "col_null": "NOT NULL", "col_comments": 
                            "AML\u5ba1\u6838\u72b6\u6001\uff1b0\uff1a\u65b0\u5efa\uff1b1\uff1a\u5f85\u5ba1\u6838\uff1b2\uff1a\u5ba1\u6838\u4e2d\uff1b3\uff1a\u901a\u8fc7\uff1b4\uff1a\u672a\u901a\u8fc7"},
                             {"default_value": "", "col_name": "AML_MSG", "col_type": "VARCHAR(4000)", "col_null": "", "col_comments": "AML\u8fd4\u56de\u6d88\u606f\u62a5\u6587"},
                              {"default_value": "", "col_name": "LU_BANK_ACCOUNT_CODE", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u9646\u56fd\u9645\u94f6\u884c\u5361\u6240\u5728\u94f6\u884c\u4ee3\u7801"}, 
                              {"default_value": "", "col_name": "RESPONSE_NO", "col_type": "BIGINT(20)", "col_null": "", "col_comments": "\u8fdc\u7a0b\u8c03\u7528\u53d6\u73b0\u51c6\u5907\u8fd4\u56de\u7684response_no"},
                               {"default_value": "", "col_name": "USER_ALIAS", "col_type": "VARCHAR(512)", "col_null": "NOT NULL", "col_comments": "\u7528\u6237\u767b\u5f55\u540d"},
                                {"default_value": "", "col_name": "VENDOR_CODE", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u8f6c\u8d26\u670d\u52a1\u4f9b\u5e94\u5546\u4ee3\u7801"}, 
                                {"default_value": "", "col_name": "REQ_RECORDS_ID", "col_type": "BIGINT(20)", "col_null": "NOT NULL", "col_comments": "\u5e42\u7b49\u8868id"}, 
                                {"default_value": "", "col_name": "EXPORT_BATCH_ID", "col_type": "BIGINT(20)", "col_null": "", "col_comments": "\u5bfc\u51fa\u6279\u6b21\u53f7\uff0cbe\u5bfc\u51fa\u7533\u8bf7\u8868\u7684id"}, 
                                {"default_value": "", "col_name": "MOBILE_NO", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u624b\u673a\u53f7"},
                                 {"default_value": "", "col_name": "IDENTITY_NO", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u8bc1\u4ef6\u53f7"}, 
                                 {"default_value": "", "col_name": "FAIL_CODE", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u53d6\u73b0\u5931\u8d25\u539f\u56e0code"}, 
                                 {"default_value": "", "col_name": "FAIL_MSG", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u53d6\u73b0\u5931\u8d25\u539f\u56e0msg"}, 
                                 {"default_value": "", "col_name": "WITHDRAW_STATUS", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u53d6\u73b0\u72b6\u6001\uff1a 1 \u65b0\u5efa 2 \u53d6\u73b0\u51c6\u5907 3 \u53d6\u73b0\u51c6\u5907\u56de\u76d8 4 AML\u7ed3\u679c\u540c\u6b65\u5b8c\u6210 5 \u53d6\u73b0\u5b8c\u6210 6\u53d6\u73b0\u5b8c\u6210\u56de\u76d8"},
                                  {"default_value": "", "col_name": "BANK_COUNTRY", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u94f6\u884c\u56fd\u5bb6"}, {"default_value": "", "col_name": "RESPONSE_NO2", "col_type": "BIGINT(20)", "col_null": "", "col_comments": "\u8fdc\u7a0b\u8c03\u7528payment\u5ba1\u6838\u63a5\u53e3\u8fd4\u56de\u7684response_no"}, 
                                  {"default_value": "", "col_name": "MATCH_REMARK", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u5339\u914d\u5907\u6ce8\uff1aW_ID\u768432\u8fdb\u5236"}, {"default_value": "", "col_name": "AML_FAILED_CODE", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "AML\u5ba1\u6838\u5931\u8d25\u9519\u8bef\u4ee3\u7801"}, {"default_value": "", "col_name": "AML_FAILED_DESC", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "AML\u5ba1\u6838\u5931\u8d25\u9519\u8bef\u63cf\u8ff0"}, {"default_value": "", "col_name": "EXPORT_TYPE", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u5bfc\u51fa\u7c7b\u578b: \u7cfb\u7edf-SYS\uff0cBE-NULL"}, {"default_value": "", "col_name": "SECURITY_VERIFY_STATUS", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u5b89\u5168\u9a8c\u8bc1\u72b6\u6001\uff1b1\uff1a\u5f85\u8fdb\u884c\u5b89\u5168\u9a8c\u8bc1\uff1b2\uff1a\u5b89\u5168\u9a8c\u8bc1\u4e2d\uff1b3\uff1a\u5b89\u5168\u9a8c\u8bc1\u901a\u8fc7\uff1b4\uff1a\u5b89\u5168\u9a8c\u8bc1\u672a\u901a\u8fc7"}, {"default_value": "", "col_name": "SECURITY_VERIFY_FAIL_CODE", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u5b89\u5168\u9a8c\u8bc1\u5931\u8d25\u9519\u8bef\u4ee3\u7801"}, {"default_value": "", "col_name": "SECURITY_VERIFY_FAIL_DESC", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u5b89\u5168\u9a8c\u8bc1\u5931\u8d25\u9519\u8bef\u63cf\u8ff0"}, {"default_value": "", "col_name": "BANK_NAME", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u94f6\u884c\u540d\u79f0\uff08App\u7aef\u5c55\u793a\uff09"}, {"default_value": "", "col_name": "BANK_ACCOUNT_NO", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u94f6\u884c\u8d26\u53f7\uff08App\u7aef\u5c55\u793a\uff09"}, {"default_value": "", "col_name": "TYPE", "col_type": "VARCHAR(1)", "col_null": "", "col_comments": "\u53d6\u73b0\u7c7b\u578b\uff1a1.\u53d6\u73b02.\u4ea4\u6613\uff08\u7cfb\u7edf\u81ea\u52a8\u53d6\u73b0\uff09"}, {"default_value": "", "col_name": "USER_TYPE", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u7528\u6237\u7c7b\u578b\uff1a\u4e2a\u4f53\u6237(0)\uff0c\u673a\u6784\u6237(1)\uff0c\u4ea7\u54c1\u6237(2)"}], "schema_name": "cfwmccshmydata", "table_comments": "\u53d6\u73b0\u8bb0\u5f55\u8868"}]
                                  {"default_value": "", "col_name": "MATCH_REMARK", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u5339\u914d\u5907\u6ce8\uff1aW_ID\u768432\u8fdb\u5236"}, {"default_value": "", "col_name": "AML_FAILED_CODE", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "AML\u5ba1\u6838\u5931\u8d25\u9519\u8bef\u4ee3\u7801"}, {"default_value": "", "col_name": "AML_FAILED_DESC", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "AML\u5ba1\u6838\u5931\u8d25\u9519\u8bef\u63cf\u8ff0"}, {"default_value": "", "col_name": "EXPORT_TYPE", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u5bfc\u51fa\u7c7b\u578b: \u7cfb\u7edf-SYS\uff0cBE-NULL"}, {"default_value": "", "col_name": "SECURITY_VERIFY_STATUS", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u5b89\u5168\u9a8c\u8bc1\u72b6\u6001\uff1b1\uff1a\u5f85\u8fdb\u884c\u5b89\u5168\u9a8c\u8bc1\uff1b2\uff1a\u5b89\u5168\u9a8c\u8bc1\u4e2d\uff1b3\uff1a\u5b89\u5168\u9a8c\u8bc1\u901a\u8fc7\uff1b4\uff1a\u5b89\u5168\u9a8c\u8bc1\u672a\u901a\u8fc7"}, {"default_value": "", "col_name": "SECURITY_VERIFY_FAIL_CODE", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u5b89\u5168\u9a8c\u8bc1\u5931\u8d25\u9519\u8bef\u4ee3\u7801"}, {"default_value": "", "col_name": "SECURITY_VERIFY_FAIL_DESC", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u5b89\u5168\u9a8c\u8bc1\u5931\u8d25\u9519\u8bef\u63cf\u8ff0"}, {"default_value": "", "col_name": "BANK_NAME", "col_type": "VARCHAR(512)", "col_null": "", "col_comments": "\u94f6\u884c\u540d\u79f0\uff08App\u7aef\u5c55\u793a\uff09"}, {"default_value": "", "col_name": "BANK_ACCOUNT_NO", "col_type": "VARCHAR(128)", "col_null": "", "col_comments": "\u94f6\u884c\u8d26\u53f7\uff08App\u7aef\u5c55\u793a\uff09"}, {"default_value": "", "col_name": "TYPE", "col_type": "VARCHAR(1)", "col_null": "", "col_comments": "\u53d6\u73b0\u7c7b\u578b\uff1a1.\u53d6\u73b02.\u4ea4\u6613\uff08\u7cfb\u7edf\u81ea\u52a8\u53d6\u73b0\uff09"}, {"default_value": "", "col_name": "USER_TYPE", "col_type": "VARCHAR(32)", "col_null": "", "col_comments": "\u7528\u6237\u7c7b\u578b\uff1a\u4e2a\u4f53\u6237(0)\uff0c\u673a\u6784\u6237(1)\uff0c\u4ea7\u54c1\u6237(2)"}], "schema_name": "cfwmccshmydata", "table_comments": "\u53d6\u73b0\u8bb0\u5f55\u8868"}]
"""
    data_map = """
{"WMC_ICASH_WITHDRAWALS_REC": []}
"""
    test = 'select a from "b",c,d'
    pr = LuSQLParser(sql)
    print(pr.get_table_name())
    # table_metas = json.loads(table_metas)
    # tm = []
    # for v in table_metas:
    #     t = TableMeta()
    #     t.__dict__ = v
    #     tm.append(t)
    #
    # data_map = json.loads(data_map)
    #
    # # with open('sql', 'r') as f:
    # #     sql = f.read()
    #
    # expect_list = get_expection_list(tm)
    # pr = LuSQLParser(sql, exception=expect_list)
    #
    # pr.display_elements(in_detial=True)
    # # print pr.get_table_name()
    # a = pr.sql_replace_bind_variable(tm, data_map)
    # # a = pr.sql_replace_bind_variable([tm], data_map)
    # print(a)
    #
    # print('done')
