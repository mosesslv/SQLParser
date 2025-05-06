# CREATED BY: bohuai jiang
# CREATED ON: 2020/2/5 12:33
# LAST MODIFIED ON:
# AIM: encapsulate data
# -*- coding:utf-8 -*-

from typing import List
import pandas
from typing import Tuple


class Package:
    def __init__(self):
        self.container = dict()
        self.html = ''

    def insert(self, **kwargs):
        pass

    def __iter__(self):
        return self.container.__iter__()

    def get(self, key: object):
        pass

    def is_empty(self) -> bool:
        return False if self.container else True


class BuilderPackage(Package):
    def __init__(self):
        super().__init__()

    def insert(self, instnt_schm_tb_name: str, col_combs: str, index_name: str, sqlid_list: List[str]):
        n_value = (col_combs, index_name)
        try:
            value_list, sqlid_list = self.container[instnt_schm_tb_name]
            value_list.append(n_value)
            self.container[instnt_schm_tb_name] = (value_list, sqlid_list)
        except:
            self.container[instnt_schm_tb_name] = ([n_value], sqlid_list)

    def get(self, key: str):  # -> Tuple[List[(str, str)],List[str]]:
        col_index, sqlid_list = self.container[key]
        return col_index, sqlid_list

    def get_instance_name(self) -> str:
        key = list(self.container.keys())[0]
        return key.split('.')[0]
    

class EvaluatorPackage(Package):
    def __init__(self):
        super().__init__()
        self.container = []

    def insert(self, instnt_schm_tb_name: str, sql_id: str, sql_text: str, plan_before: str, plan_after: str,
               improvement: float):
        value = {'instant_schema_tab_name': instnt_schm_tb_name,
                 'sql_id': sql_id,
                 'sql_text': sql_text,
                 'plan_berfore': plan_before,
                 'plan_after': plan_after,
                 'improvement': improvement,
                 'schema_name': instnt_schm_tb_name.split('.')[1]}
        self.container.append(value)

    def toDataFrame(self):
        return pandas.DataFrame(self.container)
