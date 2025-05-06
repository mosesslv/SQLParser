# CREATED BY: bohuai jiang 
# CREATED ON: 2019/8/23
# LAST MODIFIED ON:
# AIM: Explain Oracle explain
# -*- coding:utf-8 -*-


import pandas as pd
from typing import Optional, List, Tuple, Dict, Union
import numpy as np
from service.predict_sql_review_mysql.AIException import DBTypeError


class MysqlExplainAnalysis:
    def __init__(self, Explain: Union[pd.DataFrame, pd.Series], build_grap_off: bool = False):
        if isinstance(Explain, list):
            raise DBTypeError('AIErr_Mysql_20004')
        if isinstance(Explain, pd.Series):
            Explain = Explain.to_frame()
        if 'rows' not in Explain.columns.values:
            raise DBTypeError('AIErr_Mysql_20004')
        self.explain = Explain

        self.used_index = []
        for index in self.explain[self.explain['key'].notnull()]['key']:
            self.used_index.extend(index.split(','))

        used_table = self.explain[self.explain['table'].notnull()]['table']
        self.used_table = []
        for tab in used_table:
            if '<' not in tab:
                self.used_table.append(tab)

    def Extra_verbose(self) -> list:
        verbose = []
        for id, extra in enumerate(self.explain['Extra'].values):
            # if extra == None:
            #     verbose.append('MYQL执行计划错误，请联系dba')
            if extra == 'No tables used':
                verbose.append('未使用任何表，请检查sql语句是否正确')
            elif extra == 'Impossible HAVING':
                verbose.append('HAVING子句始终为false，请检查sql语句是否正确')
            elif extra == 'Impossible WHERE':
                verbose.append('WHERE子句始终为false，请检查sql语句是否正确')
            elif extra == 'Impossible WHERE noticed after reading const tables':
                verbose.append('MySQL已读取所有const（和系统）表，并注意到WHERE子句始终为false。(WHERE所查找的字段在数据之外？)')
            elif extra == 'No matching min/max row':
                verbose.append('找不到满足查询的条件（SELECT MIN（…）FROM…WHERE）的行，请修改sql语句')
            elif extra == 'no matching row in const table':
                verbose.append('发现存在空表或不满足唯一索引条件的表')
        return verbose

    def is_valid_Explain(self) -> bool:
        if np.sum(self.explain["rows"]) == 0:
            return False
        return True

    def get_Cardinality(self) -> int:
        rows = self.explain["rows"].fillna(-1)
        return np.sum(rows)

    def get_tab_name(self) -> List[str]:
        '''
        :return: all accessed tables
        '''
        return self.used_table

    def get_index_name(self) -> List[str]:
        return self.used_index

    def has(self, value: str, where: str) -> List[pd.Series]:
        out = []
        for id in self.explain.index:
            if self.explain.loc[id, where] is not None:
                if value in self.explain.loc[id, where] or value.lower() in self.explain.loc[id, where] :
                    out.append(self.explain.loc[id])
        return out

    def contains(self, value: str, where: str) -> List[pd.Series]:
        out = []
        for i in self.explain.index:
            elem = self.explain.loc[i][where]
            if type(elem) == str:
                if value in elem or value.lower() in elem :
                    out.append(self.explain.loc[i])
        return out
