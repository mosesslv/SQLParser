# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/11/15
# LAST MODIFIED ON: 2019/11/27
# AIM: improve architecture

from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit, \
    AnalysisPackage
from typing import Dict, Tuple, List, Optional, Union
from service.sql_parser_graph.units import ParseUnitList, ParseUnit
from service.predict_sql_review_oracle.AIException import SQLConflict
from service.predict_sql_review_oracle.Utility.SORT import TabTree
import pandas as pd
import service.sql_parser_graph.ParserUtility_v2 as pu
import copy

DEFAULT_RECOM_IDX_CONFIG ={'ACCEPT_COL_DISTINCT':0.0001,
                        'SELECT_TOP_N_COL':5
                          }
ACCEPT_COL_DISTINCT = DEFAULT_RECOM_IDX_CONFIG['ACCEPT_COL_DISTINCT']
SELECT_TOP_N_COL = DEFAULT_RECOM_IDX_CONFIG['SELECT_TOP_N_COL']


class TabColRelation(AnalysisPackage):
    def __init__(self):
        super().__init__()
        # {tab_id : [col_id]}
        self.value = dict()
        self.distinct = None

    def insert(self, key: int, value: int):
        try:
            self.value[key].append(value)
        except:
            self.value[key] = [value]

    def length(self):
        return len(self.value)

    def tab_id(self):
        return list(self.value.keys())

    def col_id_list(self, tab_id):
        return self.value[tab_id]


class RecomIndex(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract_analysis_data(self) -> TabColRelation:
        '''
               :param sem_info: sql semantic information
               :return: AnaylsisPackage

               '''
        sem_info = self.semantic_info.elements
        feature = TabColRelation()

        for opt in sem_info.by_type['OPT']:
            if opt.in_statement == 'WHERE' and opt.name != 'IS':
                if self.__has_func_child(opt, sem_info) or opt.name in ['IS', 'LIKE']:
                    continue

                children_ids = sem_info.find_root(opt)
                for id in children_ids:
                    child = sem_info.by_id[id]
                    if child.type == 'COL':
                        col_name = child.name
                        tab_units = [sem_info.by_id[i] for i in sem_info.find_tab(child, tab_only=True)]
                        tab_names = [ele.name for ele in tab_units]
                        col_tab = self.tab_info.chck_col_vld(col_name, tab_names)
                        if col_tab is None:
                            continue
                        _, tab_idx = col_tab
                        tab_idx = tab_units[tab_idx].id
                        feature.insert(tab_idx, id)
        return feature

    def __verbose(self, key: str, **kwargs) -> str:
        if key.upper() == 'NONE':
            return '请加入WHERE限定条件'
        if key.upper() == 'DBA':
            if 'index_name' in kwargs.keys():
                try:
                    tab_name = self.table_alias[kwargs['tab_name']]
                except:
                    tab_name = pu.get_tab_name_by_cols(kwargs['combine'], self.param['semantic_info'].elements)
                index = kwargs['index_name']
                col_names = self.param['tab_info'].get_col_from_indexInfo(index, tab_name).values[0]
                col_list = '('
                for col in col_names:
                    col_list += col.lower() + ','
                return '表' + tab_name + '中,' + index + col_list[0:-1] + ')已是最优索引，请评估是否还有效率更高的扫描方式'
            else:
                return '请评估是否还有效率更高的扫描方式'
        if key.upper() == 'BUILD_INDEX':
            recom = '请在'
            index_list = ''
            try:
                tab_name = self.table_alias[kwargs['tab_name']]
            except:
                tab_name = pu.get_tab_name_by_cols(kwargs['combine'], self.param['semantic_info'].elements)

            for col in set(kwargs['combine']):
                recom += col + '和'
                index_list += col + ','
            if len(col) > 1:
                recom = '表' + tab_name + '中，可以尝试在' + recom[0:-1] + '字段上加组合索引\n'
            else:
                recom = '表' + tab_name + '中，可以尝试在' + recom[0:-1] + '字段上加索引\n'
            recom += '\t建立索引的DDL语句为： CREATED INDEX xxx(索引名) ON ' + kwargs['tab_name'] + \
                     '(' + index_list[0:-1] + ')'
            return recom
        if key.upper() == 'OTHER_INDEX_HINT':
            return 'INDEX(' + kwargs['tab_name'] + ' ' + kwargs['index_name'] + ')'
        if key.upper() == 'OTHER_INDEX':
            tab_name = kwargs['tab_name']
            index = kwargs['index_name']
            try:
                col_names = self.param['tab_info'].get_col_from_indexInfo(index, tab_name).values[0]
            except:
                col_names = self.param['tab_info'].get_col_from_indexInfo(index, tab_name).values[0]
            col_list = '('
            for col in col_names:
                col_list += col.lower() + ','
            try:
                return '表' + self.table_alias[tab_name] + '中，可以使用索引' + index + col_list[0:-1] + ')'
            except:
                return '视图' + tab_name + '中，可以使用索引' + index + col_list[0:-1] + ')'
        return ''

    ###############################
    #        logic function       #
    ###############################
    def init_variables(self):
        # -- initialize view_name
        self.view_names = self.param['semantic_info'].get_view_name(view_id=True)
        # -- initialize tab_name
        self.table_alias = dict()
        tab_names, alias_names = self.param['semantic_info'].get_table_name(alias_on=True)
        for tab, alias in zip(tab_names, alias_names):
            self.table_alias[alias] = tab
        # -- unpack parameters -- #
        self.semantic_info = self.param['semantic_info']
        self.plan_info = self.param['plan_info']
        self.tab_info = self.param['tab_info']

    def _get_used_indexes(self) -> Dict[str, List[pd.Series]]:
        '''
        一个tab可能会对应多个index, case Union all
        :return {tab_name, [explain -- index_name]}
        '''
        out = dict()
        indexes_explain = self.plan_info.has('INDEX', 'OPERATION')
        for index in indexes_explain:
            index_name = index['OBJECT_NAME']
            tab_name = self.tab_info.get_tabName_by_idxName(index_name)
            if not tab_name:
                # - case tab_name not exist
                raise SQLConflict('AIErr_Oracle_20003')
            try:
                out[tab_name].append(index)
            except:
                out[tab_name] = [index]
        return out

    def sort_colList_by_distinct(self, col_id_list: List[int], tab_id: int) -> List[int]:
        '''
        基于COL的区分度对其排序
        :param col_id_list:
        :param tab_id:
        :return: list of col idx
        '''
        tab_unit = self.semantic_info.elements.by_id[tab_id]
        if tab_unit.type == 'VIEW':
            out = []
            tab_col_dict = self.view_handler(col_id_list, tab_id)
            for tab_id in tab_col_dict:
                sorted_col = self.sort_colList_by_distinct(tab_col_dict[tab_id], tab_id)
                out.extend(sorted_col)
            return out
        else:
            tree = TabTree()
            row_num = self.tab_info.get_tab_numrows(tab_unit.name)
            for col_i in col_id_list:
                col_unit = self.semantic_info.elements.by_id[col_i]
                try:
                    # - col may not exist in tab info
                    col_dist = self.tab_info.get_col_distinct_by_colInfo(col_unit.name, tab_unit.name) / float(row_num)
                except:
                    continue
                col_dist = float(max(0, col_dist))  # - avoid col = -1
                if col_dist >= ACCEPT_COL_DISTINCT:
                    tree.insert(name=col_unit.id, distinct=col_dist)
            sorted_col = tree.tree_traverse(increase=False, out=[])
            del tree
            return sorted_col

    def find_alternatives(self, col_id_list: List[int], tab_id: int) -> Tuple[str, float]:
        '''
        基于给定COL 找出更好的 索引建议
        :param col_id_list:
        :param tab_id:
        :return: 索引名字,对应的区分度（区分读基于匹配程度会出现不同）
        '''
        tab_unit = self.semantic_info.elements.by_id[tab_id]
        max_distinct = 0
        best_index = ''

        if tab_unit.type == 'VIEW':
            tab_col_dict = self.view_handler(col_id_list, tab_id)
            for tab_id in tab_col_dict:
                loc_bst_index, loc_distinct = self.find_alternatives(tab_col_dict[tab_id], tab_id)
                if loc_distinct > max_distinct:
                    max_distinct = loc_distinct
                    best_index = loc_bst_index
            return best_index, max_distinct
        else:
            col_level_list = self.split_cols_by_level(col_id_list)
            tab_row = self.tab_info.get_tab_numrows(tab_unit.name)
            index_info = self.tab_info.get_IndexInfo(tab_unit.name)
            for id in index_info.index:
                index_col_list = copy.copy(index_info.loc[id, 'INDEX_COLUMNS'])
                index_name = index_info.loc[id, 'INDEX_NAME']
                normalizer = float(len(index_col_list))
                match_count = 0  # - 计算相似度（match_rate），相似读越高分数越高
                # -- 基于索引字段寻找col_level_list对应的list -- #
                frst_idx_col = index_col_list.pop(0)
                col_lvl = None  # - 这里之考虑有且只有一个index
                for col_list in col_level_list:
                    if frst_idx_col in col_list:
                        col_lvl = col_list
                        match_count = 1
                        break
                while col_lvl and index_col_list:
                    idx_col = index_col_list.pop(0)
                    if idx_col in col_lvl:
                        match_count += 1
                    else:
                        break
                match_rate = match_count / normalizer
                index_distinct = float(index_info.loc[id, 'DISTINCT_KEYS']) / (tab_row + 0.000001) * match_rate
                if index_distinct > max_distinct:
                    max_distinct = index_distinct
                    best_index = index_name
            return best_index, max_distinct

    def build_new_index(self, col_id_list: List[int], tab_id: int) -> Tuple[List[str], float]:
        '''

        :param col_id_list:
        :param tab_id:
        :return:
        '''
        tab_unit = self.semantic_info.elements.by_id[tab_id]
        max_distinct = -1
        best_combo = []
        best_frst_id = -1

        if tab_unit.type == 'VIEW':
            tab_col_dict = self.view_handler(col_id_list, tab_id)
            for tab_id in tab_col_dict:
                loc_best_combo, loc_distinct = self.build_new_index(tab_col_dict[tab_id], tab_id)
                if loc_distinct > max_distinct:
                    max_distinct = loc_distinct
                    best_combo = loc_best_combo
            return best_combo, max_distinct
        else:
            tab_row = self.tab_info.get_tab_numrows(tab_unit.name)
            col_level_list, col_level_list_id = self.split_cols_by_level(col_id_list, get_id=True)
            for col_combo, id_combo in zip(col_level_list, col_level_list):
                combo_distinct = self.tab_info.get_col_distinct_by_colInfo(col_combo[0], tab_unit.name) / (
                        tab_row + 0.000001)
                if combo_distinct > max_distinct:
                    max_distinct = combo_distinct
                    best_combo = col_combo
                    best_frst_id = id_combo[0]
            if best_combo:
                v, _ = self.find_alternatives([best_frst_id], tab_id)
                if v:
                    index_info = self.tab_info.get_IndexInfo(self.semantic_info.elements.by_id[tab_id].name)
                    index_cols = index_info[index_info['INDEX_NAME'] == v]['INDEX_COLUMNS'].values[0]
                    if len(index_cols) < len(best_combo):
                        return best_combo, max_distinct
                    return [], -1
                else:
                    return best_combo, max_distinct
            else:
                return [], -1

    ###############################
    #       utility function      #
    ###############################
    def __has_func_child(self, unit: ParseUnit, sem_info: ParseUnitList) -> bool:
        '''
        检查opt是否存在个FUNCT子类
        :param unit:
        :param sem_info:
        :return:
        '''
        for c in unit.edges:
            c_unit = sem_info.by_id[c]
            if c_unit.id < unit.id:
                return False
            if c_unit.type == 'FUNC':
                return True
        return False

    def view_handler(self, view_col_id_list: List[int], view_id: id) -> Dict[int, List[int]]:
        '''
        找到view col　所对应的在真实col和tab
        :param view_col_id_list:
        :param view_id:
        :return: {tab_id:[col_id]}
        '''
        out = dict()
        sem_info = self.semantic_info.elements
        for id in view_col_id_list:
            tab_id_list, col_id = pu.find_col_tab_from_view(id, view_id, self.semantic_info.elements)
            values = self.tab_info.chck_col_vld(sem_info.by_id[col_id].name,
                                                [sem_info.by_id[id].name for id in tab_id_list])
            if values is None:
                continue
            _, tab_idx = values
            tab_id = tab_id_list[tab_idx]
            try:
                out[tab_id].append(col_id)
            except:
                out[tab_id] = [col_id]

        return out

    def split_cols_by_level(self, col_id_list: List[int], get_id: bool = False) -> Union[
        List[List[str]], Tuple[List[List[str]], List[List[int]]]]:
        '''
        基于col的level将col 划分
        :param col_id_list:
        :return: list of list of col_name, 同一曾的col_name会放在同一个list里
        '''
        lvl_col_map = dict()
        lvl_col_id_map = dict()
        for id in col_id_list:
            col_unit = self.semantic_info.elements.by_id[id]
            col_level = col_unit.level
            col_name = col_unit.name
            try:
                lvl_col_map[col_level].append(col_name)
                lvl_col_id_map[col_level].append(id)
            except:
                lvl_col_map[col_level] = [col_name]
                lvl_col_id_map[col_level] = [id]
        if not get_id:
            return list(lvl_col_map.values())
        else:
            return list(lvl_col_map.values()), list(lvl_col_id_map.values())

    def get_max_distinct(self, col_id_list: List[str], tab_id: int) -> int:
        '''
        :param col_id_list:
        :return: max distinct value over given col_id_list
        '''
        tab_name = self.semantic_info.elements.by_id[tab_id]
        max_distinct = -1
        for col_name in col_id_list:
            distinct = self.tab_info.get_col_distinct_by_colInfo(col_name, tab_name)
            if distinct > max_distinct:
                max_distinct = distinct
        return max_distinct

    ###############################
    #         main function       #
    ###############################

    def run_analysis(self, data: TabColRelation) -> Tuple[TabColRelation, VerboseUnit]:
        # --- initialize variables --- #
        used_indexes = self._get_used_indexes()

        # --- initialize output --- #
        diagnosis_result = TabColRelation()
        recom_verbose = VerboseUnit(self.semantic_info.elements)

        # ========================= #
        # case1 : 如果不存在where条件 #
        # ========================= #
        if data.length() == 0:
            if self.semantic_info.has_where:
                recom_verbose.add_solve(self.__verbose(key='DBA'))
            else:
                recom_verbose.add_solve(self.__verbose(key='NONE'))
            return diagnosis_result, recom_verbose

        # --- start search --- #
        # -- may not be a good design
        for tab_id in data.tab_id():
            tab_name = self.semantic_info.elements.by_id[tab_id]
            col_id_list = self.sort_colList_by_distinct(data.col_id_list(tab_id), tab_id)[0:SELECT_TOP_N_COL]
            # ============================ #
            # case2 : 如果没有检测到任何col  #
            # ============================ #
            if len(col_id_list) == 0:
                recom_verbose.add_solve(self.__verbose(key='DBA', tab_name=tab_name))
                # - package output - #
                diagnosis_result.value = None
                diagnosis_result.distinct = 0.0
            else:
                # ============================ #
                #      检查使用的索引是否合理     #
                # ============================ #
                explain_indexes = used_indexes[tab_name]
                for index in explain_indexes:
                    tab_row = self.tab_info.get_tab_numrows(tab_name)
                    distinct = self.tab_info.get_index_distinct(index['OBJECT_NAME'], tab_name)
                    cardinality = index['CARDINALITY']
                    distinct = min(tab_row - cardinality, distinct) / float(tab_row)  # normalization
                    if distinct > self.param['distinct_theta']:
                        # ========================== #
                        #    case 3: 如果索引达标     #
                        # ========================== #
                        recom_verbose.add_solve(
                            self.__verbose(key='DBA', index_name=index['OBJECT_NAME'], tab_name=tab_name))
                        # - package output - #
                        diagnosis_result.value = index['OBJECT_NAME']
                        diagnosis_result.distinct = distinct
                    else:
                        # ================================== #
                        #    case 4: 如果不达标寻找其他索引     #
                        # ================================== #
                        alt_index, alt_distinct = self.find_alternatives(data.col_id_list(tab_id), tab_id)
                        if alt_distinct > alt_distinct and alt_index != index:
                            recom_verbose.add_hint(hint=
                                                   self.__verbose(key='OTHER_INDEX_HINT', index_name=alt_index[0],
                                                                  tab_name=tab_name), level=int(alt_index[2]))
                            recom_verbose.add_solve(self.__verbose(key='OTHER_INDEX', index_name=alt_index[0],
                                                                   tab_name=tab_name))
                            # - package output - #
                            diagnosis_result.value = alt_index
                            diagnosis_result.distinct = alt_distinct
                        # ========================================== #
                        #     case 5: 如果没有更好索引，尝试建立索引     #
                        # ========================================== #
                        else:
                            new_index, combo_distinct = self.build_new_index(data.col_id_list(tab_id), tab_id)
                            # -- case 5.1: 如果找不到可建索引的index -- #
                            if not new_index:
                                recom_verbose.add_solve(self.__verbose(key='DBA', index_name=index, tab_name=tab_name))
                                # - package output - #
                                diagnosis_result.value = index['OBJECT_NAME']
                                diagnosis_result.distinct = distinct
                            # -- case 5.2: 所建索引达标 -- #
                            elif combo_distinct > distinct * 0.7:
                                recom_verbose.add_solve(
                                    self.__verbose(key='BUILD_INDEX', combine=new_index,
                                                   tab_name=tab_name))
                                # - package output - #
                                diagnosis_result.value = new_index
                                diagnosis_result.distinct = combo_distinct
                            # -- case 5.3: 如果索引不达标 -- #
                            else:
                                recom_verbose.add_solve(
                                    self.__verbose(key='DBA', index_name=new_index, tab_name=tab_name))
                                # - package output - #
                                diagnosis_result.value = index['OBJECT_NAME']
                                diagnosis_result.distinct = distinct
                                # update diagnosis_result
                # ====================================== #
                #     case 6: 如果不达标 尝试建立索引       #
                # ====================================== #
                else:
                    new_index, combo_distinct = self.build_new_index(data.col_id_list(tab_id), tab_id)
                    # -- case 6.1: 如果索引区分度不达标 或 无法建立索引 -- #
                    if combo_distinct > self.param['distinct_theta'] * 0.7:
                        recom_verbose.add_solve(
                            self.__verbose(key='BUILD_INDEX', combine=new_index,
                                           tab_name=tab_name))
                        # - package output - #
                        diagnosis_result.value = new_index
                        diagnosis_result.distinct = combo_distinct
                    # -- case 6.3: 所建索引达标 -- #
                    else:
                        recom_verbose.add_solve(self.__verbose(key='DBA', index_name=new_index, tab_name=tab_name))
                        # - package output - #
                        diagnosis_result.value = None
                        diagnosis_result.distinct = 0.0
                # new_index = self.(col_names, tab)
            return diagnosis_result, recom_verbose
