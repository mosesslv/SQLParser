# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/9/20
# LAST MODIFIED ON:　2019/9/20 18:06 CHECKED
# AIM: analysis table info

from typing import Optional, List, Tuple
from pandas import DataFrame
import numpy as np

class TabelInfoAnaylsis:
    def __init__(self, table_info: dict):
        self.table_info = table_info

        for tab_name in table_info:
            # -- rebuild index info --#
            tab_name = tab_name
            col_distinc = dict()
            index_info = self.table_info[tab_name]['table_indexes']
            index_col = dict()
            index_distinc = dict()
            index_non_unique = dict()
            for i in index_info.index:
                col_name = index_info.loc[i, 'COLUMN_NAME']
                cardinality = index_info.loc[i, 'CARDINALITY']
                col_distinc[col_name] = cardinality
                index_name = index_info.loc[i, 'INDEX_NAME']
                if index_name in index_col.keys():
                    index_col[index_name].append(col_name)
                    index_car = index_distinc[index_name]
                    if cardinality > index_car:
                        index_distinc[index_name] = cardinality
                else:
                    index_col[index_name] = [col_name]
                    index_distinc[index_name] = cardinality
                index_non_unique[index_name] = index_info.loc[i, 'NON_UNIQUE']
            index_info_ = DataFrame()
            index_info_['INDEX_NAME'] = list(index_distinc.keys())
            index_info_['INDEX_COLUMNS'] = list(index_col.values())
            index_info_['DISTINCT_KEYS'] = list(index_distinc.values())
            index_info_['NON_UNIQUE'] = list(index_non_unique.values())
            self.table_info[tab_name]['table_indexes'] = index_info_
            # -- rebuild col info --#
            # -- add cardinality to COL --#
            col_info = self.table_info[tab_name]['table_columns']
            col_names = col_info.loc[:, 'COLUMN_NAME']
            CARDINALITY = []
            for col in col_names:
                try:
                    CARDINALITY.append(col_distinc[col])
                except:
                    CARDINALITY.append(-1)
            col_info['NUM_DISTINCT'] = CARDINALITY
            self.table_info[tab_name]['table_columns'] = col_info
        self.__numrow_amended()

    def __numrow_amended(self):
        for tab in self.table_info:
            info = self.table_info[tab]
            numrows = info['table_numrows']
            mx_idx_dstnct = np.max(info['table_indexes']['DISTINCT_KEYS'])
            if mx_idx_dstnct > numrows:
                self.table_info[tab]['table_numrows'] = mx_idx_dstnct

    def get_table_name(self):
        return self.table_info.keys()

    def get_IndexInfo(self, tab: str) -> DataFrame:
        return self.table_info[tab]['table_indexes']

    def get_colInfo(self, tab_name: str):
        return self.table_info[tab_name]['table_columns']

    def get_col_distinc_by_colInfo(self, col: str, tab_name: str):
        col_info = self.get_colInfo(tab_name)
        return col_info[col_info['COLUMN_NAME'] == col]['NUM_DISTINCT'].iloc[0]

    def get_col_from_indexInfo(self, idx_name: str, tab_name: str) -> List[str]:
        index_info = self.get_IndexInfo(tab_name)
        return index_info[index_info['INDEX_NAME'] == idx_name].INDEX_COLUMNS

    def update_col_distinc(self, col: str, tab_name: str, value: float):
        col_info = self.table_info[tab_name]['table_columns']
        id = col_info[col_info['COLUMN_NAME'] == col].index[0]
        col_info.loc[id, 'NUM_DISTINCT'] = value

    def get_plan_info_get_tab_name(self, index_name: str) -> Optional[str]:
        for tab in self.table_info:
            index_info = self.table_info[tab]['table_indexes']
            if max(index_info['INDEX_NAME'] == index_name):
                return tab
        return None

    def get_tab_numrows(self, tab_name: str) -> float:
        index_info = self.table_info[tab_name]['table_indexes']
        row_num = self.table_info[tab_name]['table_numrows']
        if row_num is None:
            row_num = max(index_info['DISTINCT_KEYS'])
        return row_num + 0.000001

    def find_indexName_by_colName(self, table_name: str, col_name: str) -> List[str]:
        index_info = self.table_info[table_name]['table_indexes']
        index_names = []
        for i in index_info.index:
            columns = index_info.loc[i]['INDEX_COLUMNS']
            index_name = index_info.loc[i]['INDEX_NAME']
            if col_name in columns:
                index_names.append(index_name)
        return index_names

    def chck_col_vld(self, col: str, tabs: List[str]) -> Optional[Tuple[str, int]]:
        '''

              :param col: col name
              :param tabs: list of tabs
              :return:  conl name and tabs id (easy for converting into alias_on name)
              '''
        table_info = self.table_info
        for tab_name in table_info:
            for id, name in enumerate(tabs):
                if tab_name == name:
                    tab_col = table_info[tab_name]['table_columns']
                    value = tab_col[tab_col['COLUMN_NAME'] == col]['COLUMN_NAME']
                    if not value.empty:
                        return col, id
        return None

    def get_tabName_by_idxName(self, index_name: str) -> Optional[str]:
        for tab in self.table_info:
            index_info = self.table_info[tab]['table_indexes']
            if max(index_info['INDEX_NAME'] == index_name):
                return tab
        return None