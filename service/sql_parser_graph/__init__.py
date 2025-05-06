# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/3/19 下午3:03
# LAST MODIFIED ON:
# AIM:

from service.sql_parser_graph.Formater import Formater


def reformat(sql:str) -> str:
    return Formater(sql).run()

if __name__ == '__main__':
    sql_text = """
      select * from ( select * from tab where id in (select c from tab2 where i > 10 ) ) left join (select b from c)
      """

    print(reformat(sql_text))