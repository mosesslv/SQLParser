# -*- coding:utf-8 -*-
# title = ''
# CREATED BY: 'lvao513'
# CREATED ON: '2020/6/2'
# LAST MODIFIED ON: '2020/6/2'
# GOAL: ......

import sqlparse as parser
# parser = sqlparse()

try:
    with open('check_file', 'r') as check:
        temp_sql = check.read()
except:
    print('报错:')
sql_stmt = parser.format(temp_sql)
print('SQL读出:',sql_stmt)

# sql_stmt = sqlparse.formatter(temp_sql)