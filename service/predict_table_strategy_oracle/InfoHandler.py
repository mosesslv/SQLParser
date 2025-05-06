# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/26 下午4:36
# LAST MODIFIED ON:
# AIM: Another Information handler , service for our table strategy input
# WARNING: only accept single schema case
from sqlparse.keywords import is_keyword
from service.sql_parser_graph.SQLParser import SQLParser
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.predict_sql_review_oracle.AIException import SQLHandleError, AIparserError, AnaylsisInfoNOTAccurate
from common.OracleHandle import OracleHandle
from service.predict_sql_review_oracle.Utility.TableInfoAnalysis import TabelInfoAnaylsis
from typing import Optional, List
import pandas


class Handler_pingan:

    def __init__(self, sql_struct: OracleSQLStruct, **kwargs):
        self._init_attribute()
        self.__value_chec(sql_struct.sql_text, 'sql_text')
        self.oracle_conn = self.__value_chec(sql_struct.oracle_conn, 'oracle_conn')
        assert isinstance(self.oracle_conn, OracleHandle)
        self.schema_name = self.__value_chec(sql_struct.schema_name, 'schema_name')
        self.fatal_error_check()
        self.instance_name = self.oracle_conn.get_instance_name()
        exception_list = self.add_tab_info(sql_struct)
        self.sematic_info = SQLParser(sql=sql_struct.sql_text, case_sensitive_on=False, exception_list=exception_list)
        self.index_info = self.tab_info.get_all_index()
        # --- add args --- #
        if 'sql_freq' in kwargs.keys():
            self.sql_freq = kwargs['sql_freq']
        if 'sql_id' in kwargs.keys():
            self.sql_id = kwargs['sql_id']

    # --------------------- #
    #        utility        #
    # --------------------- #
    def _init_attribute(self):
        self.oracle_conn = None
        self.schema_name = None
        self.instance_name = None
        self.tab_info = None
        self.index_info = None
        self.sematic_info = None
        self.sql_freq = 1
        self.error = ''
        self.sql_id = ''

    #def add_index_info(self, i):
    def add_tab_info(self, oracle_struct: OracleSQLStruct) -> List[str]:
        '''
        assign value to self.tab_info
        :param oracle_struct:
        :return:  LU parser Exception List
        '''
        # ---- check tab_info ---- #
        try:
            check_table_name = [v.upper() for v in oracle_struct.sql_ast.get_table_name()]
        except:
            check_table_name = SQLParser(oracle_struct.sql_text, case_sensitive_on=False).get_table_name()

        check_table_name = list(set(check_table_name))
        tab_info = self.__value_chec(oracle_struct.tab_info, 'tab_info')
        self.fatal_error_check()
        # --- add tab_info --- #
        tab_info_ = dict()
        exception_list = []
        for each_tab_info in tab_info:
            meta_info = each_tab_info.__dict__
            tab_name = meta_info['table_name'].upper()
            check_table_name.remove(tab_name)
            self.__completely_traverse_chec(meta_info, 'tab_info-' + tab_name)
            for keys in meta_info:
                value = meta_info[keys]
                if type(value) is list:
                    if value and type(value[0]) is dict:
                        meta_info[keys] = pandas.DataFrame(value)
                    if keys == 'table_columns':
                        for col_name in meta_info[keys].COLUMN_NAME.values:
                            col_name = col_name.upper()
                            if str(is_keyword(col_name)[0]) == 'Token.Keyword':
                                exception_list.append(col_name)
            tab_info_[tab_name] = meta_info

        if len(check_table_name) != 0:
            self.error += '[tab_info] 缺失表 <'
            for v in check_table_name:
                self.error += v + '> 和 <'
            self.error = self.error[0:-4] + '的信息\n'
        self.fatal_error_check()
        self.tab_info = TabelInfoAnaylsis(tab_info_)
        return exception_list

    def __completely_traverse_chec(self, value: object, where: str, defalue_value: object = None) -> None:
        if type(value) is dict or type(value) is list:
            for v in value:
                if type(value) is list:
                    self.__completely_traverse_chec(v, where, defalue_value)
                else:
                    self.__completely_traverse_chec(value[v], where + '-' + v, defalue_value)
        else:
            self.__value_chec(value, where, defalue_value)

    def __value_chec(self, value: object, where: str, default_value: object = None) -> Optional[object]:
        if value:
            return value
        elif type(value) is str and value == '':
            self.error += '[' + where + '] 是空值 \n'
            if default_value is not None:
                return None
            else:
                return default_value
        else:
            self.error += '[' + where + '] 缺失信息 \n'
            if default_value is not None:
                return None
            else:
                return default_value

    def fatal_error_check(self):
        error = self.error.split('\n')
        for v in error:
            if '[sql_text]' in v:
                raise SQLHandleError('AIErr_Oracle_20000')
            if '[oracle_conn]' in v:
                raise SQLHandleError('AIErr_Oracle_20000')
            if '[tab_info]' in v:
                raise SQLHandleError('AIErr_Oracle_20000')

