# created by bohuai jiang
# on 2019/8/15
# -*- coding:utf-8 -*-
from sqlparse.keywords import is_keyword
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.sql_parser_graph.SQLParser import SQLParser
from service.predict_sql_review_oracle.AIException import SQLHandleError, AIparserError, AnaylsisInfoNOTAccurate
from service.predict_sql_review_oracle.Utility.OracleExplainAnalysis import OracleExplainAnalysis, to_Dataframe
from service.predict_sql_review_oracle.Utility.TableInfoAnalysis import TabelInfoAnaylsis
from typing import Optional
import pandas
import copy


class InfoHandler:
    def __init__(self, information_: OracleSQLStruct):
        '''
        check validity of give info
        :param information: inofrmation
        :return: true -> legal false -> illegal
        '''
        self.error = ''
        # -- avoid been overwrite
        information = information_.copy()
        # set default
        self.input_data = {
            "sql_text": None,
            "plan_raw": None,
            "tab_info": None,
            "tab_name": None,
            "OracleSQLStruct": information,
            "exception_list": []
        }
        # ---- checked data valid ----#
        self.input_data['sql_text'] = self.__value_chec(information.sql_text, 'sql_text')
        self.input_data['plan_raw'] = self.__value_chec(information.plan_raw, 'plan_raw')
        self.fatal_error_check()
        # ---- check parser --- #
        try:
            SQLParser(information.sql_text, case_sensitive_on=False)
        except:
            raise AIparserError("AIErr_Oracle_20002")

        # ---- check tab_info ---- #
        try:
            check_table_name = [v.upper() for v in information.sql_ast.get_table_name()]
        except:
            check_table_name = SQLParser(information.sql_text, case_sensitive_on=False).get_table_name()
        tab_name = OracleExplainAnalysis(to_Dataframe(information.plan_raw)).get_tab_name()
        check_table_name = list(set(check_table_name))
        tab_name = list(set(tab_name))
        if len(tab_name) > len(check_table_name):
            self.error += '[tab_info] information inconsistency with [plan_raw] in table names\n'
        tab_info = self.__value_chec(information.tab_info, 'tab_info')
        self.fatal_error_check()

        # ---- add tab_info ---- #
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

        tab_info = TabelInfoAnaylsis(tab_info_)
        # ---- add tab_info , tab_name, exception_list ----- #
        self.input_data['tab_info'] = tab_info
        self.input_data['exception_list'] = exception_list
        self.input_data['tab_name'] = copy.copy(tab_name)

        # --- table size check --- #
        for tab in tab_info.table_info:
            if tab_info.get_tab_numrows(tab) < 1 and SQLParser(self.input_data['sql_text'],
                                                               case_sensitive_on=False).has_where:
                raise AnaylsisInfoNOTAccurate('AIErr_Oracle_20005')

        # ---- add sql_text, plan_raw ----- #
        self.input_data['sql_text'] = SQLParser(information.sql_text, exception=exception_list, case_sensitive_on=False)
        self.input_data['plan_raw'] = OracleExplainAnalysis(to_Dataframe(information.plan_raw))

    def getData(self) -> dict:
        self.fatal_error_check()
        return self.input_data  # message

    def fatal_error_check(self):
        error = self.error.split('\n')
        for v in error:
            if '[tab_info]' in v:
                # fatal_errors += v + '\n'
                raise SQLHandleError('AIErr_Oracle_20000')
            if '[plan_raw]' in v + '\n':
                raise SQLHandleError('AIErr_Oracle_20001')
            if '关键字' in v:
                print(v)
                # fatal_errors += v + '\n'
        # if fatal_errors != '':
        #     raise SQLHandleError(fatal_errors[0:-1])

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
