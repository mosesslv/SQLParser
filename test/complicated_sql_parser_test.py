# -*- coding:utf-8 -*-
# title = ''
# CREATED BY: 'lvao513'
# CREATED ON: '2020/6/15'
# LAST MODIFIED ON: '2020/6/15'
# GOAL: ......

try:
    with open('pingan_bank.txt', 'r') as check:
        temp_sql = check.read()
except:
    print('sql reading error')

from service.sql_parser_graph.SQLParser import SQLParser

sql_parsed = SQLParser(temp_sql)

# print('original parsed tokens:\t',sql_parsed.origin_tokens,'\n')

print('sql elements:\n',sql_parsed.display_elements(),'\n')

print('type sql parsed:\t',sql_parsed.sql_statement(),'\n')

print('table names:\t',sql_parsed.get_table_name(),'\n')

print('*'*50)
modifed_sql = sql_parsed.sql_interpreter(temp_sql)
sql_parsed2 = SQLParser(modifed_sql)
print('new table names get:\t',sql_parsed2.get_table_name(),'\n')

# print('sql interpret:\t',sql_parsed.sql_interpreter(temp_sql),'\n')