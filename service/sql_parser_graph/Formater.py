# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/3/19 下午1:09
# LAST MODIFIED ON:
# AIM:

import sqlparse
import re
from sqlparse.sql import Where, IdentifierList, Identifier, TokenList, Token, Parenthesis, Comment, Case, Operation, \
    Function, Values


class Formater:
    def __init__(self, sql: str):
        pattern = re.compile('(\n|\t)')
        sql = pattern.sub(' ', sql).strip()
        sql = re.sub(' +',' ',sql)
        sql = ' '.join(v for v in sql.split(' '))
        self.token = sqlparse.parse(sql)[0]

    def format(self, statement: TokenList, header: str) -> str:
        out = ''

        for token in statement:
            if not isinstance(token, TokenList):
                token = token.value
                try:
                    out += header if out[-1] == '\n' else ''
                except:
                    pass
                if token.upper() in ['SELECT', 'INSERT', 'UPDATE','FROM', 'WHERE', 'SET', 'ORDER BY', 'GROUP BY', 'WITH', 'MERGE INTO', 'AND','OR']:
                    try:
                        out += '\n' if out[-1] != '\n' else ''
                    except:
                        pass
                    out += header[0:-1] if isinstance(statement,Where) else header
                    out += token
                else:
                    out += token
            else:
                if isinstance(token, IdentifierList):
                    out += self.format_identifierList(token, header + '\t')
                elif isinstance(token, Function):
                    out += token.value
                else:
                    out += '\n' if isinstance(token,Where) else ''
                    out += self.format(token, header + '\t')
        return out

    def format_identifierList(self, ilist: IdentifierList, header: str) -> str:
        out = ''
        for token in ilist:
            if token.value not in [',']:
                out += header + token.value + '\n'
        return out

    def run(self) -> str:
        return self.format(self.token, '')


if __name__ == '__main__':
    sql_text = """
select * from (select * from tab where id in (1,2,3,4)) where a > 10 
    """
    # sql_text = """
    # select a from b where i > 10 and
    # """
    print(Formater(sql_text).run())
