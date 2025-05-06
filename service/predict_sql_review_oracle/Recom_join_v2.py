# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/11/27
# LAST MODIFIED ON:
# AIM:improve architecture

from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC, VerboseUnit, \
    AnalysisPackage
from service.predict_sql_review_oracle.Recom_index_v2 import RecomIndex, TabColRelation
from typing import Dict, Tuple, List, Union
from service.sql_parser_graph.units import ParseUnitList, ParseUnit
from itertools import chain, combinations
from service.predict_sql_review_oracle.AIException import SQLConflict
from service.predict_sql_review_oracle.Utility.SORT import TabTree
import pandas as pd
import service.sql_parser_graph.ParserUtility_v2 as pu
import numpy as np
import copy


class JoinColRelation(AnalysisPackage):
    def __init__(self):
        super().__init__()
        self.tab_col_relation = dict()  # {tab_id : [col_id] }
        self.join_tab_relation = dict()  # {tab_id : [{tab_id, col_id}]}
        self.join_tab_container = dict()  # {tab_id: col_id}

    def insert_tabCol(self, key: int, value: int):
        try:
            self.tab_col_relation[key].append(value)
        except:
            self.tab_col_relation[key] = [value]

    def insert_tabJoin(self, tab_id: int, col_id: int):
        self.join_tab_container[tab_id] = col_id

    def build_tabJoin(self):
        # assert (len(self.join_tab_container) == 2)
        if len(self.join_tab_container) != 2:
            self.join_tab_container = dict()
            # return

        for tab_id in self.join_tab_container:
            value = self.join_tab_container[tab_id]
            try:
                self.tab_col_relation[tab_id].append(value)
            except:
                self.tab_col_relation[tab_id] = [value]

        self.join_tab_container = dict()


class RecomJoin(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract_analysis_data(self) -> JoinColRelation:
        '''
        :return:
        '''
        feature = JoinColRelation()
        for opt in self.semantic_info.elements.by_type['OPT']:
            children_ids = self.semantic_info.elements.find_root(opt)
            cnt_col = 0

            is_join_opt = True
            if len(children_ids) >= 2:
                for id in children_ids:
                    child = self.semantic_info.elements.by_id[id]
                    if child.type == 'COL':
                        cnt_col += 1
                if cnt_col < 2:
                    is_join_opt = False
            else:
                is_join_opt = False

            # -- find index -- #
            for id in children_ids:
                child = self.semantic_info.elements.by_id[id]
                col_name = child.name
                tab_idxes = [self.semantic_info.elements.by_id[i] for i in
                             self.semantic_info.elements.find_tab(child, tab_only=True)]
                tab_names = [ele.name for ele in tab_idxes]
                col_tab = self.tab_info.chck_col_vld(col_name, tab_names)
                if col_tab is None:
                    continue
                _, tab_idx = col_tab
                tab_idx = tab_idxes[tab_idx].id
                if not is_join_opt:
                    if opt.in_statement == 'WHERE' and opt.name != 'IS':
                        feature.insert_tabCol(tab_idx, id)
                else:
                    feature.insert_tabJoin(tab_idx, id)
            feature.build_tabJoin()
        return feature

    def __verbose(self, key: str, **kwargs) -> str:
        pass

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

    def get_driven_tab_candidate(self) -> List[str]:
        '''
        Detect LEFT JOIN ,
        RULE 1. tab on the left side of LEFT JOIN could be the Driven Tab Candidate
        RULE 2. tab in Driven Tab Candidate should not on the right side of LEFT JOIN
        :return: drive tab candidate
        '''
        drvn_tb_cnddt = []
        join_type = self.__get_join_type()
        if 'LEFT JOIN' in join_type.keys():
            be_joined_list = []
            for tab_pair in join_type['LEFT JOIN']:
                left_tab = tab_pair[0]
                right_tab = tab_pair[1]
                if right_tab not in be_joined_list:
                    be_joined_list.append(right_tab)
                # RULE 1
                if left_tab not in drvn_tb_cnddt:
                    drvn_tb_cnddt.append(left_tab)
            # RULE 2
            for joined_tab in be_joined_list:
                if joined_tab in drvn_tb_cnddt:
                    drvn_tb_cnddt.remove(joined_tab)

        return drvn_tb_cnddt


    ###############################
    #       utility function      #
    ###############################
    def __get_join_type(self) -> Dict[str, (str, str)]:
        '''

        :return: e.g {'INNER JOIN': (tab_a, tab_b), 'LEFT JOIN': (tab_c, tab_d)}
        '''
        out = dict()
        sem_info = self.semantic_info.elements
        for join_unit in sem_info.by_type['JOIN']:
            child = sorted(join_unit.edges)
            join_type = join_unit.name
            tab_left = sem_info.find_tab(sem_info.by_id[child[0]], tab_only=True)[0]
            tab_right = sem_info.find_tab(sem_info.by_id[child[1]], tab_only=True)[0]
            try:
                out[join_type].append((tab_left, tab_right))
            except:
                out[join_type] = [(tab_left, tab_right)]
        return out

    ###############################
    #         main function       #
    ###############################

    def run_analysis(self, data: JoinColRelation) -> Tuple[JoinColRelation, VerboseUnit]:
        # --- initialize variables --- #

        diagnosis_result = JoinColRelation()
        recom_verbose = VerboseUnit(self.param)

        # ====================== #
        #    step 1 sort table   #
        # ====================== #
        sortTree = TabTree()
        for tab in data.tab_col_relation:
            index_data = TabColRelation()
            index_data.value = {tab: data.tab_col_relation[tab]}
            index_data, index_recom = RecomIndex().run_analysis(index_data)
            sortTree.insert(name=tab, distinct=index_data.distinct)
            recom_verbose.add(index_recom)
        #tree_ordered = sortTree.tree_traverse(increase=False, out=[])[0]
        # ====================== #
        # step 2 find driven tab #
        # ====================== #

        # Driven Tab criteria:
        # 1. relative highest Distinct Value
        # 2. Tab must from Driven Tab Candidate (if DTC is not empty)
        # 3. Tab must able to connect with at least one tab (if DTC is empty)
        driven_candidate = self.get_driven_tab_candidate()
        ordered_tab = sortTree.tree_traverse(increase=False, out=[])
        cnt = 0
        tab_length = len(ordered_tab)
        dirve_tab = None
        while cnt < tab_length:
            if driven_candidate:
                if ordered_tab[cnt] in driven_candidate:
                    #drive_tab = tab_list.pop(cnt)
                    pass


        # ====================== #
        #  apply recommendation  #
        # ====================== #
        pass
