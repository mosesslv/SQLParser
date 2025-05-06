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

    def insert(self, instance_name: str, schema_name: str, tab_name: str, col_combs: str, index_name: str, sqlid_list: List[str]):
        if schema_name is None:
            schema_name = 'DUMMY'
        
        instnt_schm_tb_name = '.'.join([instance_name, schema_name, tab_name])
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

    def to_ddl_given_instnt_schm_tb_name(self, instnt_schm_tb_name:List[str]) -> List[str]:
        ddl_list = []
        for name in self:
            if name not in instnt_schm_tb_name:
                continue
            
            instance_name, schema_name, tab_name = name.split('.')
            col_index, sqlid_list = self.get(name)
            for col_combs, index_name in col_index:
                ddl_list.append(self._build_DDL(col_combs, tab_name, index_name))
        
        return ddl_list


    def _build_DDL(self, col_list: str, tab_name: str, index_name: str) -> str:
        out = f'CREATE INDEX {index_name:s} ON {tab_name}('
        for col in col_list.split(' '):
            out += col + ','
        out = out[0:-1] + ') ONLINE COMPUTE STATISTICS;'
        return out

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
                 'improvement': improvement}
        self.container.append(value)

    def toDataFrame(self):
        return pandas.DataFrame(self.container)
