# -*- coding: UTF-8 -*-
# created by bohuai
# on 2019/9/20
# last modified on : 2019/10/31/13:40:51


from service.predict_sql_review_mysql.Utility.Architecture import AI_FUNC
from service.predict_sql_review_mysql.Utility.MysqlExplainAnalysis import MysqlExplainAnalysis
from service.predict_sql_review_mysql.Utility.TableInfoAnalysis import TabelInfoAnaylsis
from service.sql_parser_graph.SQLParser import SQLParser, ParseUnit
from service.sql_parser_graph.units import ParseUnitList
import service.sql_parser_graph.ParserUtiltiy as pu
from typing import Union, List, Optional, Dict, Tuple
import numpy as np
import re

# simple decision tree
THRESHOLD = 1000
VALUES_THRESHOLD = 50


class AI(AI_FUNC):

    def __init__(self, *args):
        super().__init__(*args)

    ################
    #    utility   #
    ################
    def __check_equal_only(self, sem_info: SQLParser) -> bool:
        '''
        only use "=" under where statement
        :param sem_info:
        :return:
        '''
        for opt in sem_info.elements.by_type['OPT']:
            if opt.in_statement == 'WHERE' and opt.name != '=':
                return False
        return True

    def __check_use_pk_uk_only(self, sem_info: SQLParser, explain: MysqlExplainAnalysis,
                               tabInfo: TabelInfoAnaylsis) -> bool:
        '''
        check if the sql used pk or uk only
        :param sem_info:
        :param explain:
        :param tabInfo:
        :return:
        '''
        explain_ = explain.explain
        tab_name, aliase = sem_info.get_table_name(alias_on=True)
        aliase_table = dict()
        # -- build table alise -- #
        for name, as_name in zip(tab_name, aliase):
            aliase_table[as_name] = name
        for id in explain_.index:
            try:
                index_names = explain_.loc[id, 'key'].split(',')
            except:
                continue
            for index_name in index_names:
                if explain_.loc[id, 'table'] is None:
                    continue
                tab_name = aliase_table[explain_.loc[id, 'table']]
                index_info = tabInfo.get_IndexInfo(tab_name)
                index_non_unique = index_info[index_info['INDEX_NAME'] == index_name]['NON_UNIQUE'].values[0]
                if not (index_name.upper() == 'PRIMARY' or index_non_unique == 0):
                    return False
        return True

    def __has_func_child(self, unit: ParseUnit, sem_info: ParseUnitList) -> bool:
        for c in unit.edges:
            c_unit = sem_info.by_id[c]
            if c_unit.id < unit.id:
                return False
            if c_unit.type == 'FUNC':
                return True
        return False

    def __get_tab_col_relation(self, sem_info: SQLParser, tabInfo: TabelInfoAnaylsis) -> Dict[
        str, np.array]:
        '''
             :param sem_info: sql semantic information
             :return: {tab_nam:[col_names, is_bindVariable, operation, level]}
             '''
        sem_info = sem_info.elements
        out = dict()

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
                        tab_name = tab_names[tab_idx]
                        if tab_name not in out.keys():
                            out[tab_name] = [
                                [col_name, str(is_placeholder), opt.name, pu.find_add_hint_loc(opt, sem_info)]]
                        else:
                            out[tab_name].append(
                                [col_name, str(is_placeholder), opt.name, pu.find_add_hint_loc(opt, sem_info)])
        for k in out.keys():
            out[k] = np.vstack(out[k])
        return out

    def __find_index_by_columns(self, cols: List[str], tab: str, tabInfo: TabelInfoAnaylsis) -> float:
        # cols = liset(cols))
        index_info = tabInfo.get_IndexInfo(tab)
        table_size = tabInfo.get_tab_numrows(tab)
        best_distinct = -1
        for id in index_info.index:
            relate_col = index_info.loc[id, 'INDEX_COLUMNS']
            distinc = float(index_info.loc[id, 'DISTINCT_KEYS']) / float(table_size)
            if relate_col[0] in cols and distinc > best_distinct:
                best_distinct = distinc
        return best_distinct

    def __count_insert_columns(self, sem_info: SQLParser) -> int:
        count = 0
        for unit in sem_info.elements.by_type["COL"]:
            if unit.in_statement == 'VALUES':
                count += 1
        return count

    def __count_insert_value_trunk(self, sem_info: SQLParser):
        count = 0
        for unit in sem_info.elements.by_type["SUB"]:
            if unit.in_statement == 'VALUES':
                count += 1
        return count

    ##################
    #     predict    #
    ##################

    def predict(self, features) -> Tuple[int, str]:
        df = features['plan_raw']
        semantic_info = features['sql_text']
        tab_info = features['tab_info']
        tab_col_info = self.__get_tab_col_relation(semantic_info, tab_info)
        # -- search full scan -- #
        if (df.has('ALL', 'type') or df.has('INDEX', 'type')) and semantic_info.sql_statement() != "INSERT":
            return 0, " "
        # -- search UK,PK key -- #
        if self.__check_equal_only(semantic_info) and self.__check_use_pk_uk_only(semantic_info, df, tab_info):
            return 1, " "
        # -- search range scan -- #
        for tab in tab_col_info.keys():
            cols = tab_col_info[tab][:, 0]
            dist = self.__find_index_by_columns(cols, tab, tab_info)
            if dist < 0.1:
                return 0, " "
        # -- insert handler -- #
        if semantic_info.sql_statement() == "INSERT":
            num_col = self.__count_insert_columns(semantic_info)
            if num_col == 0:
                return 0, ''
            else:
                num_value_trunk = self.__count_insert_value_trunk(semantic_info)
                if num_value_trunk > VALUES_THRESHOLD:
                    return 0, ''
                else:
                    return 1, ''
        else:
            if not df.is_valid_Explain():
                return 0, ''
        # TBD (目前无，不会run下面)
        X = df.get_Cardinality()
        # 0是拒绝，1是通过
        if X < THRESHOLD and X >= 0:
            return 1, ''
        else:
            return 0, ''
