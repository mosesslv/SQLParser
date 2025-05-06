# -*- coding:utf-8 -*-
# title = ''
# CREATED BY: 'lvao513'
# CREATED ON: '2020/6/15'
# LAST MODIFIED ON: '2020/6/15'
# GOAL: ......
from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnitList, ParseUnit
from typing import Union, List, Optional, Tuple, Dict
from test.SQL_Parser_2.parseutils import extract_tables
import re

class SQLParserNew(SQLParser):
    #
    # def __new__(cls, *args, **kwargs):
    #     print("new %s" % cls)
    #     return SQLParser.__new__(cls, *args, **kwargs)

    def get_table_name(self, alias_on = False, view_on = False) -> Union[Tuple[List[str], List[str]], List[str]]:
        tab_names = []
        as_names = []
        TAB_LIST = self.elements.by_type['TAB']
        VIEW_LIST = self.elements.by_type['VIEW'] if view_on else []
        SUB_LIST = self.elements.by_type['SUB']
        for unit in TAB_LIST + VIEW_LIST + SUB_LIST:
            if unit.type == 'TAB' and '(' not in unit.name and 'DUAL' not in unit.name:
                tab_names.append(re.sub(r'[\"\']', "", unit.name))
                if unit.as_name != 'DUMMY':
                    as_names.append(re.sub(r'[\"\']', "", unit.as_name))
                else:
                    as_names.append(re.sub(r'[\"\']', "", unit.name))
            if unit.type == 'VIEW':
                tab_names.append(re.sub(r'[\"\']', "", unit.as_name))
            as_names.append(re.sub(r'[\"\']', "", unit.as_name))
            # if unit.type == 'SUB' :
            #     # print('当前子语句: ',unit.name)
            #     sub_parser = self.__class__(unit.name)
            #     for tk in sub_parser.tokens.flatten():
            #         if tk.ttype == ''
            #     sub_tables = sub_parser.get_table_name()
            #     tab_names.extend(sub_tables)
        if alias_on:
            return tab_names, as_names
        else:
            return tab_names

    def addSchema(self,sql:str,schema:str) ->str:
        idx = 0
        tk_list = list(sql.split())
        out = []
        for token in tk_list:
            if 'FROM' == token.upper():

                tk_list[idx+1] = schema +'.'+ tk_list[idx+1]
                out.append(token)
                idx +=1
            else:
                out.append(token)
                idx +=1

        return ' '.join(out)

if __name__ == "__main__":
    sql_text1 = "(select * from ( select xxx + select (select a from tab_a) from xxx) x join b from select( x from tab_b) on x.key1 = b.key2)"
    with open('/Users/lvao513/Desktop/陆金所/代码仓库/sqlreview/test/pingan_bank.txt', 'r') as check:
        sql_text2 = check.read()
    new_parser = SQLParserNew(sql_text2)
    print('get elements\t',new_parser.display_elements())
    print('get tb names\t', new_parser.get_table_name())
    # print('\n')
    # for tk in new_parser.tokens.flatten():
    #     print('token',tk, tk.ttype)

    print('tables extract:',extract_tables(sql_text2))

    # print('add schema:',new_parser.addSchema(sql_text2,'schema1'))
    print('table name grep:', new_parser.table_name_grep(sql_text2))