# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/9/10
# LAST MODIFIED ON:
# AIM: modified SQL, berfore it been send to AI review

from service.sql_parser_graph_2.SQLParser import SQLParser
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.predict_sql_review_oracle.Utility.TableInfoAnalysis import TabelInfoAnaylsis
from service.sql_parser_graph_2.units import ParseUnit
import pandas
from typing import List, Optional
from sqlparse.sql import Comparison


class SQLmodifier(SQLParser):
    def __init__(self, oracle_sql_struct: OracleSQLStruct):
        super().__init__(oracle_sql_struct.sql_text)

        # -- get tab info -- #
        tableInfo = oracle_sql_struct.tab_info
        self.tableInfo = dict()
        for each_tab_info in tableInfo:
            meta_info = each_tab_info.__dict__
            tab_name = meta_info['table_name']
            for keys in meta_info:
                value = meta_info[keys]
                if type(value) is list:
                    if value and type(value[0]) is dict:
                        meta_info[keys] = pandas.DataFrame(value)
            self.tableInfo[tab_name] = meta_info

        # --- #
        self.tableInfo = TabelInfoAnaylsis(self.tableInfo)

    def _get_remove_trunk(self, unit: ParseUnit) -> List[str]:
        unit_list = []
        # --- step 1: search --- #
        path = []
        q = [unit.id]
        while q:
            v = q.pop(0)
            if not v in path:
                if type(v) == int:
                    path = path + [v]
                    # --- #
                    units_ = self.elements.by_id[v]
                    if units_.type != 'TAB':
                        unit_list.append(units_)
                        q = q + list(units_.parent) + list(units_.edges)
        # --- step 2: reconstruct --- #
        # -- 2.1 sort --#
        unit_list = sorted(unit_list, key=lambda each_unit: each_unit.id)
        print('done')
        out = []
        if len(unit_list) == 1 or isinstance(unit_list[0].token, Comparison):
            for v in self.token_walk(unit_list[0].token, True, False):
                if not v.is_whitespace:
                    out.append(v.value)
        elif unit_list[1].name == 'IN':
            for unit in unit_list[0:-1]:
                for v in self.token_walk(unit.token, True, False):
                    if not v.is_whitespace:
                        out.append(v.value)
                out.append('in')
            out = out[0:-1]
        elif unit_list[1].name == 'BETWEEN':
            pass
        return out

