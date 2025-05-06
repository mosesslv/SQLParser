# created by bohuai jiang
# on 2019/8/15
# -*- coding:utf-8 -*-
from sqlparse.keywords import is_keyword
from service.AISQLReview.sql_abstract import MysqlSQLStruct
from service.sql_parser_graph.SQLParser import SQLParser
from service.predict_sql_review_mysql.AIException import SQLHandleError, AIparserError, AnaylsisInfoNOTAccurate, \
    KeyWordsAsAlias
from service.predict_sql_review_mysql.Utility.MysqlExplainAnalysis import MysqlExplainAnalysis
from service.predict_sql_review_mysql.Utility.TableInfoAnalysis import TabelInfoAnaylsis
from typing import Optional
import pandas
import copy

KEY_WORD = ['POSITION', 'TYPE', 'BLOCK', '']


class InfoHandler:
    def __init__(self, information: MysqlSQLStruct):

        # -- avoid been overwrite
        '''
        check validity of give info
        :param information: inofrmation
        :return: true -> legal false -> illegal
        '''
        self.error = ''
        # set default
        self.input_data = {
            "sql_text": None,
            "plan_raw": None,
            "tab_info": None,
            "tab_name": None,
            "MysqlSQLStruct": information,
            "exception_list": []
        }
        # ---- check sql_text, plan_raw  ----#
        self.__value_chec(information.sql_text, 'sql_text')
        self.__dataframe_chec(information.plan_raw, 'plan_raw')
        self.fatal_error_check()

        try:
            SQLParser(information.sql_text)
        except:
            raise AIparserError("AIErr_Mysql_20002")

        # ---- check tab_info ---- #
        try:
            check_table_name = information.sql_ast.get_table_name()
        except:
            check_table_name = SQLParser(information.sql_text).get_table_name(alias_on=False)
        check_table_name = list(set(check_table_name))
        tab_info = self.__value_chec(information.tab_info, 'tab_info')
        self.fatal_error_check()

        # ---- add tab_info ---- #
        tab_info_ = dict()
        exception_list = []
        for each_tab_info in tab_info:
            meta_info = each_tab_info.__dict__
            tab_name = meta_info['table_name']
            # TODO:remove here when you pull
            try:
                check_table_name.remove(tab_name)
            except:
                check_table_name.remove(tab_name.lower())
                tab_name = tab_name.lower()
            #check_table_name.remove(tab_name)
            self.__completely_traverse_chec(meta_info, 'tab_info-' + tab_name)
            for keys in meta_info:
                value = meta_info[keys]
                if type(value) is list:
                    if value and type(value[0]) is dict:
                        meta_info[keys] =  pandas.DataFrame(value)
                    if keys == 'table_columns':
                        for col_name in meta_info[keys].COLUMN_NAME.values:
                            col_name = col_name
                            if str(is_keyword(col_name)[0]) == 'Token.Keyword':
                                exception_list.append(col_name)
            tab_info_[tab_name] = meta_info
        if len(check_table_name) != 0:
            self.error += '[tab_info] 缺失表 <'
            for v in check_table_name:
                self.error += v + '> 和 <'
            self.error = self.error[0:-4] + '的信息\n'
        self.fatal_error_check()

        # ---- add sql_text, plan_raw ----- #
        self.input_data['sql_text'] = SQLParser(information.sql_text, exception=exception_list)
        self.input_data['plan_raw'] = MysqlExplainAnalysis(information.plan_raw)

        tab_info = TabelInfoAnaylsis(tab_info_)
        # --- table size check --- #
        for tab in tab_info.table_info: # 前面的条件是要去查统计信息，和ai_sql_Reivew模块关系不大
            if tab_info.get_tab_numrows(tab) < 1 and SQLParser(information.sql_text).has_where: # jbh的条件逻辑
                raise AnaylsisInfoNOTAccurate('AIErr_Mysql_20005')

        # --- check if keywords is tab alias name --- #
        # tab_alias_tabInfo = tab_info.get_table_name()
        # tab_name_parser, ta_alias_parser = SQLParser(information.sql_text).get_table_name(alias_on=True)
        # for names, alias in zip(tab_name_parser, ta_alias_parser):
        #     if alias not in tab_alias_tabInfo:
        #         raise  KeyWordsAsAlias('AIErr_Mysql_20006')
        # ---- add tab_info , tab_name, exception_list ----- #
        self.input_data['tab_info'] = tab_info
        self.input_data['exception_list'] = exception_list
        self.input_data['tab_name'] = copy.copy(check_table_name)

    def getData(self) -> dict:
        return self.input_data  # message

    def fatal_error_check(self):
        error = self.error.split('\n')
        for v in error:
            if '[tab_info]' in v:
                # fatal_errors += v + '\n'
                raise SQLHandleError('AIErr_Mysql_20000')
            if '[plan_raw]' in v + '\n':
                raise SQLHandleError('AIErr_Mysql_20001')
            if '关键字' in v:
                print(v)

    def __dataframe_chec(self, value: object, where: str) -> Optional[object]:
        if type(value) is pandas.DataFrame:
            if value.empty:
                self.error += '[' + where + '] 是空值 \n'
                return value
            else:
                return value.apply(lambda x: x.astype(str).str)
        else:
            self.error += '[' + where + '] 是缺失 \n'
            return None

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
