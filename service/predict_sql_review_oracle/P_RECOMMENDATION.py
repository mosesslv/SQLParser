# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/6/20
# LAST MODIFIED ON: 2019/12/5
# AIM: test single sql

from service.predict_sql_review_oracle.Utility.Architecture import AI_FUNC
from service.predict_sql_review_oracle.Recom_explain import RecomDefault
from service.predict_sql_review_oracle.Recom_index import RecomIndex
from service.predict_sql_review_oracle.Recom_join import RecomJoin
from service.predict_sql_review_oracle.Recom_sub import RecomSub
from service.predict_sql_review_oracle.Recom_Page import RecomPage
from typing import List, Dict, Optional
from service.sql_parser_graph.SQLParser import SQLParser
from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC

THRESHOLD = 1000
DIFF_THETA = 0.01


#################
#  expressions  #
#################

#################


class RECOMM(AI_FUNC):

    def __init__(self, *args) -> None:
        super().__init__(*args)

    def __init_para(self) -> None:
        # -- feautres -- #
        self.plan_info = None
        self.semantic_info = None
        self.tab_info = None
        self.tab_name = None

    ##########################
    #   feature extraction   #
    ##########################

    def __feature_extraction(self, feature: dict) -> None:
        self.__init_para()
        self.semantic_info = feature['sql_text']
        self.plan_info = feature['plan_raw']
        self.tab_info = feature['tab_info']
        self.tab_name = feature['tab_name']

        self.oracleSQLStruct = feature['OracleSQLStruct']

    #####################################
    #        write logic here           #
    #####################################
    def number_of_union(self, sematic: SQLParser) -> int:
        """
        count number of union/union all in a sql
        :return: number
        """
        elements = sematic.elements
        cnt = 0
        for unit in elements.by_type['JOIN']:
            if unit.name in ['UNION', 'UNION ALL']:
                cnt += 1
        return cnt

    def diagnose(self) -> List[str]:
        # --------------------- #
        #  read execution plan  #
        # --------------------- #
        semantic_info = self.semantic_info.elements
        out = ['default']
        out.append('index')
        has_orderby = False
        has_rownum = False
        # ------------------ #
        #       PAGING       #
        # ------------------ #
        for opt in semantic_info.by_type['OPT']:
            if opt.name == 'ORDER BY':
                has_orderby = True
                break
        for opt in semantic_info.by_type['STRUCT']:
            if opt.name == 'ROWNUM' and opt.in_statement == "WHERE":
                has_rownum = True
                break
        if has_orderby and has_rownum:
            out.remove('index')
            out.append('page')
        # ------------------ #
        #       TABLE        #
        # ------------------ #
        _, alias_names = self.semantic_info.get_table_name(alias_on=True)
        tab_num = len(set(alias_names))
        union_num = self.number_of_union(self.semantic_info)
        sub_num = 0
        for sub in semantic_info.by_type['SUB']:
            if 'SELECT' in sub.name:
                sub_num += 1
        if sub_num >= 1:
            out.append('sub')
        if tab_num > 1 and union_num != tab_num - 1:
            out.append('join')
            if 'index' in out:
                out.remove('index')
        return out

    def __logic(self):
        # ---- add children -----#
        struct = REC_FUNC(semantic_info=self.semantic_info,
                          plan_info=self.plan_info,
                          tab_info=self.tab_info,
                          tab_name=self.tab_name,
                          distinct_theta=DIFF_THETA,
                          OracleSQLStruct=self.oracleSQLStruct,
                          threshold=THRESHOLD)

        struct.add_to_dict('default', RecomDefault())
        struct.add_to_dict('index', RecomIndex())
        struct.add_to_dict('join', RecomJoin())
        struct.add_to_dict('sub', RecomSub())
        struct.add_to_dict('page', RecomPage())
        # ----
        stat = self.diagnose()
        recom = struct.show_edges(stat, self.semantic_info.origin_sql)

        return recom

    def predict(self, feature):
        self.__feature_extraction(feature)
        return self.__logic()
