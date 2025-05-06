# created by bohuai jiang
# on 2019/8/1
# -*- coding: utf-8 -*-

from typing import List
import json
import random
from typing import Tuple
from service.AISQLReview.sql_abstract import MysqlSQLStruct
from common.MysqlHandle import MysqlHandle
from service.AISQLReview.mysql_addition import MysqlAddition


class ComBuffer:
    def __init__(self, param: dict):
        self.param = param
        # --- prepare communication environments --- #
        ora_sql = self.param['MysqlSQLStruct']
        self.ora_add = MysqlAddition(ora_sql)
        # --- #

    def __to_sql(self, col_list: List[str], tab: str):
        out = 'select count(*) from ( select '
        col_verbose = ''
        for col in col_list:
            col_verbose += col + ','
        out += col_verbose[0:-1] + ' from ' + tab + ' group by '
        out += col_verbose[0:-1] + ') a where rownum <= 100000'
        return out

    def fetch_combine_distinct(self, col_list: list, tab: str) -> float:
        sql = self.__to_sql(col_list, tab)
        data_dict = {"columns": col_list,
                     "sql_text": sql}

        #distinc = self.ora_add.get_column_discrimination_data(data_dict)
        return len(col_list)*0.01

    def fetch_col_distinct(self, col:str, tab:str)-> float:
        return self.fetch_combine_distinct([col],tab)


if __name__ == '__main__':
    c = ComBuffer(None)
    print(c.fetch_combine_distinct(['aaa', 'MKT_SMS_REALATION'], 'tab1'))
