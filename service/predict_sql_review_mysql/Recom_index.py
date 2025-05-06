# -*- coding: UTF-8 -*-
# created by bohuai jiang
# on 2019/9/20
# last modified on :

from service.predict_sql_review_mysql.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit
from service.predict_sql_review_mysql.Utility.Commu_buffer import ComBuffer
from typing import Dict, Tuple, List, Optional, Union
from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnitList, ParseUnit
from service.predict_sql_review_mysql.AIException import SQLConflict
from service.predict_sql_review_mysql.Utility.SORT import TabTree
import service.sql_parser_graph.ParserUtiltiy as pu
import numpy as np
import copy

class RecomIndex(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_alias = dict()

    def __get_tab_alias(self, sem_info: SQLParser):
        for tab in sem_info.elements.by_type['TAB']:
            if '(' and 'DUAL' not in tab.name:
                self.table_alias[tab.get_name()] = tab.name

    def __has_func_child(self, unit: ParseUnit, sem_info: ParseUnitList) -> bool:
        for c in unit.edges:
            c_unit = sem_info.by_id[c]
            if c_unit.id < unit.id:
                return False
            if c_unit.type == 'FUNC':
                return True
        return False

    # --- core function --- #
    def __get_tab_col_relation(self, sem_info: SQLParser) -> Dict[str, np.array]:
        '''
        :param sem_info: sql semantic information
        :return: {tab_nam:[col_names, is_bindVariable, operation, level]}
        '''
        sem_info = sem_info.elements
        out = dict()
        tabInfo = self.param['tab_info']

        for opt in sem_info.by_type['OPT']:
            if opt.in_statement == 'WHERE' and opt.name != 'IS':
                is_placeholder = False
                if self.__has_func_child(opt, sem_info) or opt.name in ['IS', 'LIKE']:
                    continue
                children_ids = sem_info.find_root(opt)

                for id in children_ids:
                    child = sem_info.by_id[id]
                    if child.type == 'VALUE':
                        if ':P' in child.name:
                            is_placeholder = True
                            break
                for id in children_ids:
                    child = sem_info.by_id[id]
                    if child.type == 'COL':
                        col_name = child.name
                        tab_list = [sem_info.by_id[i] for i in sem_info.find_tab(child, tab_only=True)]
                        tab_alias = [ele.get_name() for ele in tab_list]
                        tab_names = [ele.name for ele in tab_list]
                        values = tabInfo.chck_col_vld(col_name, tab_names)
                        if values is None:
                            continue
                        col_name, tab_idx = values
                        tab_name = tab_alias[tab_idx]
                        if tab_name not in out.keys():
                            out[tab_name] = [
                                [col_name, str(is_placeholder), opt.name, pu.find_add_hint_loc(opt, sem_info)]]
                        else:
                            out[tab_name].append(
                                [col_name, str(is_placeholder), opt.name, pu.find_add_hint_loc(opt, sem_info)])
        for k in out.keys():
            out[k] = np.vstack(out[k])
        return out

    def __get_used_indexes(self, col_list: Dict[str, np.array]) -> Dict[str, np.array]:
        '''
        :return: tab- name :  (key-index_name, value-distinct key, cardinality, Options)
        '''

        Explain = self.param['plan_info']
        indexes = Explain.get_index_name()
        tabInfo = self.param['tab_info']
        tab_index = []
        for index_name in indexes:
            tab_name = tabInfo.get_tabName_by_idxName(index_name)
            if not tab_name:
                raise SQLConflict('AIErr_Oracle_20003')
            tab_index.append((tab_name, index_name))

        out = dict()
        for pair in tab_index:
            idx_name = pair[1]
            for key in col_list:
                if key in self.table_alias.keys():
                    tab_name = self.table_alias[key]
                    index_explain = Explain.has(idx_name, 'key')[0]
                    index_info = tabInfo.get_IndexInfo(tab_name)
                    row_num = tabInfo.get_tab_numrows(tab_name)
                    distinc = index_info[index_info['INDEX_NAME'] == idx_name].DISTINCT_KEYS
                    corr_key = index_info[index_info['INDEX_NAME'] == idx_name].INDEX_COLUMNS
                    cardinality = index_explain.rows
                    option = index_explain.type
                    is_in_index = False
                    if not distinc.empty:
                        for col_name in col_list[key][:, 0]:
                            if col_name in corr_key.values[0]:
                                is_in_index = True
                        if is_in_index:
                            if key in out.keys():
                                out[key].append(
                                    [idx_name, str(float(distinc.values[0]) / (row_num + 0.00001)), str(cardinality),
                                     option, ','.join(corr_key.values[0])])
                            else:
                                out[key] = [
                                    [idx_name, str(float(distinc.values[0]) / (row_num + 0.00001)), str(cardinality),
                                     option, ','.join(corr_key.values[0])]]
        for key in out.keys():
            out[key] = np.vstack(out[key])
        return out

    def __find_alternatives(self, col_list: np.array, tab_alias: str) -> Optional[
        Tuple[str, float, int]]:
        '''

        :param col_list: [[col, location ]]
        :param tab: tab_name
        :return: index_name, distinc, col_index
        '''
        if tab_alias in self.table_alias.keys():
            tab = self.table_alias[tab_alias]
            tabInfo = self.param['tab_info']
            max_value = -1
            best_index = None
            max_id = -1
            row_num = tabInfo.get_tab_numrows(tab)
            index_info = tabInfo.get_IndexInfo(tab)
            level = -1
            # -- caculate matching rate --#
            for id in index_info.index:
                # print(id)
                index_cols = copy.copy(index_info.loc[id, 'INDEX_COLUMNS'])
                index_name = index_info.loc[id, 'INDEX_NAME']
                base = copy.copy(len(index_cols))
                match_count = 0
                while len(index_cols) > 0:
                    # print('in dead loop')
                    v = index_cols.pop(0)
                    found = False
                    for col in col_list:
                        if v == col[0]:
                            match_count += 1
                            found = True
                            level = col[1]
                            break
                    if not found:
                        break
                if index_name != 'PRIMARY':
                    match_rate = 1.0 / 10.0 ** (base - match_count)
                else:
                    match_rate = match_count / float(base)
                index_distinc = float(index_info.loc[id, 'DISTINCT_KEYS']) / (row_num + 0.000001) * match_rate
                if index_distinc > max_value and match_count > 0:
                    max_value = index_distinc
                    best_index = index_name
                    # -- @change here -- #
                    max_id = level

            if best_index is not None and level != -1:
                return best_index, max_value, max_id
            else:
                return ('', 0, -1)
        else:
            # -- handle view -- #
            sem_info = self.param['semantic_info'].elements
            tabInfo = self.param['tab_info']
            tab_col = dict()  # - {tab1:([col],[col_idx])}
            view_id = pu.get_unit_id(tab_alias, is_alias=True, graph=sem_info)
            for col in col_list:
                _tab_names, col = pu.find_col_tab_from_view(col[0],
                                                            view_id,
                                                            sem_info)
                values = tabInfo.chck_col_vld(col, _tab_names)
                if values is None:
                    continue
                col_name, tab_idx = values
                tab_name = _tab_names[tab_idx]
                col_id = pu.get_unit_id(col_name, is_alias=False, graph=sem_info, left_most=view_id)
                try:
                    tab_col[tab_name].append([col, str(col_id)])
                except:
                    tab_col[tab_name] = [[col, str(col_id)]]
            max_distinc = -1
            max_index = ''
            max_col = -1
            for tab in tab_col.keys():
                col_list = tab_col[tab]
                index_name, distinc, col_index = self.__find_alternatives(np.array(col_list), self.get_alias_name(tab))
                if distinc > max_distinc:
                    max_distinc = distinc
                    max_index = index_name
                    max_col = col_index

            return max_index, max_distinc, max_col

    def get_alias_name(self, tab_name: str) -> str:
        out = tab_name
        for k, v in self.table_alias.items():
            if tab_name == v:
                return k
        return out

    def check_comba_valid(self, comb: Tuple[str], level_map: Dict[str, int]) -> bool:
        elements = self.param['semantic_info'].elements
        init_id = level_map[comb[0]]
        init_level = elements.by_id[init_id].level
        for i in range(1, len(comb)):
            c_id = level_map[comb[i]]
            c_level = elements.by_id[c_id].level
            if init_level != c_level:
                return False
        return True

    def __index_builder(self, col_list_: np.array, tab_alias: str) -> Optional[Tuple[List[str], float, int]]:
        '''
        :param col_list: column_name list
        :param tab:  tab
        :return: (best_combination, distinct_key)
        '''
        if tab_alias in self.table_alias.keys():
            tab = self.table_alias[tab_alias]
            tabInfo = self.param['tab_info']
            row_num = tabInfo.get_tab_numrows(tab)
            col_level = dict()
            best_combo = None
            max_distinct = -1
            best_level = None
            for v in col_list_:
                level = v[1]
                col_name = v[0]
                try:
                    if col_name not in col_level[level]:
                        col_level[level].append(col_name)
                except:
                    col_level[level] = [col_name]

            for level in col_level:
                comb = col_level[level]
                comb_distinct = tabInfo.get_col_distinc_by_colInfo(comb[0], tab) / (row_num + 0.00000001)
                if comb_distinct > max_distinct:
                    max_distinct = comb_distinct
                    best_combo = comb
                    best_level = level
            if best_combo:
                temp_ = []
                # -- format best_combo --#
                for e_comb in best_combo:
                    temp_.append(np.array([e_comb, level]))
                temp_ = np.vstack(temp_)

                v = self.__find_alternatives(temp_, tab_alias)
                if v[0] != '':
                    index_info = tabInfo.get_IndexInfo(tab)
                    index_cols = index_info[index_info['INDEX_NAME'] == v[0]]['INDEX_COLUMNS'].values[0]
                    if len(index_cols) < len(best_combo):
                        return best_combo, (v[1] + 0.1), best_level
                    else:
                        return None
                else:
                    return best_combo, max_distinct, best_level
            else:
                return None

        else:
            sem_info = self.param['semantic_info'].elements
            tabInfo = self.param['tab_info']
            tab_col = dict()  # - {tab1:([col],[col_idx])}
            view_id = pu.get_unit_id(tab_alias, is_alias=True, graph=sem_info)
            for col in col_list_:
                _tab_names, col = pu.find_col_tab_from_view(col[0],
                                                            view_id,
                                                            sem_info)
                values = tabInfo.chck_col_vld(col, _tab_names)
                if values is None:
                    continue
                col_name, tab_idx = values
                tab_name = _tab_names[tab_idx]
                col_id = pu.get_unit_id(col_name, is_alias=False, graph=sem_info, left_most=view_id)
                try:
                    tab_col[tab_name].append([col, str(col_id)])
                except:
                    tab_col[tab_name] = [[col, str(col_id)]]
            max_distinc = -1
            max_comb = []
            max_idx = -1
            for tab in tab_col.keys():
                col_list = np.array(tab_col[tab])
                new_index = self.__index_builder(col_list, self.get_alias_name(tab))
                if new_index:
                    col_comb, distinc, idx = new_index
                    if distinc > max_distinc:
                        max_distinc = distinc
                        max_idx = idx
                        max_comb = col_comb
            if max_comb == []:
                return None
            return max_comb, max_distinc, max_idx

    def sort_colList_by_distinc(self, col_list_: Tuple[List[str], List[str]], tab_alias: str) -> np.array:
        '''

        :param col_list_:
        :param tab_alais:
        :return: [col and its location ]
        '''
        if tab_alias in self.table_alias.keys():
            tab_name = self.table_alias[tab_alias]
            tabInfo = self.param['tab_info']
            map_table = dict()
            col_list = []
            for col, level in zip(col_list_[0], col_list_[1]):
                col_list.append(col)
                map_table[col] = str(level)

            tree = TabTree()
            row_num = tabInfo.get_tab_numrows(tab_name)
            dividend = 100000 if row_num > 100000 else row_num
            for col in col_list:
                try:
                    col_dist = tabInfo.get_col_distinc_by_colInfo(col, tab_name) / float(row_num)
                except:
                    continue
                if col_dist < 0:
                    try:
                        col_dist = self.com.fetch_col_distinct(col, tab_name) / float(dividend)
                    except:
                        col_dist = 0.01
                    # --- update tabInfo -- #
                    tabInfo.update_col_distinc(col, tab_name, int(col_dist * float(dividend)))
                if col_dist >= 0.0001:
                    tree.insert(name=col, distinct=col_dist)
            sorted_col = tree.tree_traverse(increase=False, out=[])
            del tree
            out = []
            if sorted_col:
                for col in sorted_col:
                    out.append([col, map_table[col]])
                return np.array(out)
            else:
                return []
        # -- if it is a view -- #
        else:
            sem_info = self.param['semantic_info'].elements
            tabInfo = self.param['tab_info']
            tab_col = dict()  # - { tab: ([col1], [col_idx])]}
            view_id = pu.get_unit_id(tab_alias, is_alias=True, graph=sem_info)
            for col in col_list_[0]:
                _tab_names, col = pu.find_col_tab_from_view(col,
                                                            view_id,
                                                            sem_info)
                values = tabInfo.chck_col_vld(col, _tab_names)
                if values is None:
                    continue
                col_name, tab_idx = values
                tab_name = _tab_names[tab_idx]
                col_id = pu.get_unit_id(col_name, is_alias=False, graph=sem_info, left_most=view_id)
                try:
                    tab_col[tab_name][0].append(col)
                    tab_col[tab_name][1].append(str(col_id))
                except:
                    tab_col[tab_name] = ([col], [str(col_id)])
                # col_id = pu.get_unit_id(col_name, is_alias=False, graph=sem_info, left_most=view_id)
            out = []
            for tab in tab_col:
                col_pair_list = tab_col[tab]
                out.extend(self.sort_colList_by_distinc(col_pair_list, self.get_alias_name(tab)))
            return np.array(out)

    def __verbose(self, key: str, **kwargs) -> Union[str, Dict[str, str]]:
        translate = {'ALL': '全表扫描', 'INDEX': '全索引扫描', 'RANGE': '索引范围扫描', 'REF': '非唯一索引扫描',
                     'EQ_REF': '唯一索引扫描'}
        if key.upper() == 'WHERE':
            used_equal = kwargs['used_equal']
            pattens = kwargs['info']
            opt = ''
            for token in pattens.split(' '):
                opt += translate[token] if token in translate.keys() else token
            tab_name = kwargs['tab_name']
            index = kwargs['index_name']
            col_names = self.param['tab_info'].get_col_from_indexInfo(index, self.table_alias[tab_name]).values[0]
            col_list = '('
            for col in col_names:
                col_list += col.lower() + ','
            if used_equal:
                return '扫描表' + tab_name + '在索引' + index + col_list[0:-1] + ')上,使用' + opt + '但参数区分度可能不高'
            else:
                return '扫描表' + tab_name + '在索引' + index + col_list[0:-1] + ')上,使用' + opt + '但参数范围可能过大'
        if key.upper() == 'NONE':
            return '请加入WHERE限定条件'
        if key.upper() == 'DBA':
            if 'index_name' in kwargs.keys():
                tab_name = kwargs['tab_name']
                index = kwargs['index_name']
                col_names = self.param['tab_info'].get_col_from_indexInfo(index, self.table_alias[tab_name]).values[0]
                col_list = '('
                for col in col_names:
                    col_list += col.lower() + ','
                return '表' + tab_name + '中,' + index + col_list[0:-1] + ')已是最优索引，请评估是否还有效率更高的扫描方式'
            else:
                return '请评估是否还有效率更高的扫描方式'
        if key.upper() == 'BUILD_INDEX':
            recom = '请在'
            index_list = ''
            for col in set(kwargs['combine']):
                recom += col + '和'
                index_list += col + ','
            if len(col) > 1:
                recom = '可以尝试在' + recom[0:-1] + '字段上加组合索引\n'
            else:
                recom = '可以尝试在' + recom[0:-1] + '字段上加索引\n'
            # CREATE INDEX [index name] ON [table name]([column name]);
            recom += '\t建立索引的DDL语句为： CREATED INDEX xxx(索引名) ON ' + kwargs['tab_name'] + \
                     '([' + index_list[0:-1] + '])'

            return recom
        if key.upper() == 'OTHER_INDEX_HINT':
            return 'USE INDEX(' + kwargs['index_name'] + ')'
        if key.upper() == 'OTHER_INDEX':
            tab_name = kwargs['tab_name']
            index = kwargs['index_name']
            col_names = self.param['tab_info'].get_col_from_indexInfo(index, self.table_alias[tab_name]).values[0]
            col_list = '('
            for col in col_names:
                col_list += col.lower() + ','
            return '表' + tab_name + '中，可以使用索引' + index + col_list[0:-1] + ')'
        if key.upper() == 'PLACE_HOLDER':
            return kwargs['index_name'] + '使用绑定变量，请确定绑定变量取值范围'
        if key.upper() == 'FUZZY_SEARCH':
            tab_name = kwargs['tab_name']
            col_name = kwargs['col_name']
            return '表' + tab_name + '的字段' + col_name + '上使用前置模糊匹配,去掉前置模糊匹配'
        return ''

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[Optional[TransmitContainer], VerboseUnit]:
        # -- initialize output fromate --#
        self.com = ComBuffer(self.param)
        transmit = TransmitContainer()
        recom = VerboseUnit()
        self.view_names = self.param['semantic_info'].get_view_name(view_id=True)
        # --#
        if not self.param['plan_info'].is_valid_Explain():
            recom.add_problems(self.param['plan_info'].Extra_verbose())
            return transmit, recom
        theta = min(np.percentile(self.param['plan_info'].explain['rows'].values, 50), self.param['threshold'])
        TabelInfo = self.param['tab_info']
        self.__get_tab_alias(self.param['semantic_info'])

        if info is None:
            col_in_sql = self.__get_tab_col_relation(self.param['semantic_info'])
        else:
            col_in_sql = info.index_receive

        if len(col_in_sql) == 0:
            if self.param['semantic_info'].has_where:
                recom.add_solve(self.__verbose(key='DBA'))
            else:
                recom.add_solve(self.__verbose(key='NONE'))
            return transmit, recom

        used_indexes = self.__get_used_indexes(col_in_sql)

        #############################
        #        add problems       #
        #############################

        for tab in col_in_sql.keys():
            # -- sort col_names --#
            col_names = self.sort_colList_by_distinc((col_in_sql[tab][:, 0], col_in_sql[tab][:, 3]), tab)
            col_names = col_names[0:5]
            if len(col_names) == 0:
                recom.add_solve(self.__verbose(key='DBA', tab_name=tab))
                transmit.index_transmit = (None, 0, False)
            else:
                if tab in used_indexes.keys():
                    for indexes in used_indexes[tab]:
                        # -- assume tab has one index ONLY --#
                        index_name = indexes[0]
                        cardinality = float(indexes[2])
                        index_distinc = min(1 - cardinality / TabelInfo.get_tab_numrows(self.table_alias[tab]),
                                            float(indexes[1]))

                        if index_distinc > self.param['distinct_theta']:
                            recom.add_solve(self.__verbose(key='DBA', index_name=index_name, tab_name=tab))
                            transmit.index_transmit = (index_name, index_distinc, True)

                        else:
                            alt_index = self.__find_alternatives(col_names, tab)
                            if alt_index[1] > index_distinc and alt_index[0] != index_name:
                                recom.add_hint(hint=
                                               self.__verbose(key='OTHER_INDEX_HINT', index_name=alt_index[0],
                                                              tab_name=tab), level=int(alt_index[2]))
                                recom.add_solve(self.__verbose(key='OTHER_INDEX', index_name=alt_index[0],
                                                               tab_name=tab))
                                transmit.index_transmit = (alt_index[0], alt_index[1], True)
                            else:
                                new_index = self.__index_builder(col_names, tab)
                                if new_index is None:
                                    recom.add_solve(self.__verbose(key='DBA', index_name=index_name, tab_name=tab))
                                    transmit.index_transmit = (index_name, index_distinc, True)

                                elif new_index[1] > index_distinc:
                                    recom.add_solve(
                                        self.__verbose(key='BUILD_INDEX', combine=new_index[0],
                                                       tab_name=tab))
                                    transmit.index_transmit = (new_index[0], new_index[1], False)

                                else:
                                    recom.add_solve(self.__verbose(key='DBA', index_name=index_name, tab_name=tab))
                                    transmit.index_transmit = (index_name, index_distinc, True)
                else:  # if not used_indexes
                    alt_index = self.__find_alternatives(col_names, tab)
                    if alt_index[0] != '':
                        recom.add_hint(
                            hint=self.__verbose(key='OTHER_INDEX_HINT', index_name=alt_index[0], tab_name=tab),
                            level=int(alt_index[2]))
                        recom.add_solve(
                            self.__verbose(key='OTHER_INDEX', index_name=alt_index[0], tab_name=tab))
                        transmit.index_transmit = (alt_index[0], alt_index[1], True)
                        # return transmit, recom
                    else:
                        new_index = self.__index_builder(col_names, tab)
                        if new_index is None:
                            new_index = [-1, -1]
                        if new_index[1] > self.param['distinct_theta']:
                            recom.add_solve(
                                self.__verbose(key='BUILD_INDEX', combine=new_index[0], tab_name=tab))
                            transmit.index_transmit = (new_index[0], new_index[1], False)

                        else:
                            recom.add_solve(self.__verbose(key='DBA', tab_name=tab))
                            transmit.index_transmit = (new_index[0], new_index[1], False)

            # ------ end logic ----------- #
        return transmit, recom
