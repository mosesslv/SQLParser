# created by bohuai
# on 2019/6/20
# -*- coding: UTF-8 -*-

from service.predict_sql_review_mysql.Utility.Architecture import AI_FUNC
from service.predict_sql_review_mysql.Recom_default import RecomDefault
from service.predict_sql_review_mysql.Recom_index import RecomIndex
from service.predict_sql_review_mysql.Recom_rules import RecomRules
from typing import List, Dict
from service.predict_sql_review_mysql.Utility.Architecture import REC_FUNC
from service.predict_sql_review_mysql.Utility.Architecture import VerboseUnit

THRESHOLD = 1000
DIFF_THETA = 0.4


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

        self.mysqlSQLStruct = feature['MysqlSQLStruct']


    #####################################
    #        write logic here           #
    #####################################
    def diagnose(self) -> List[str]:
        # --------------------- #
        #  read execution plan  #
        # --------------------- #
        semantic_info = self.semantic_info.elements
        out = ['default']
        tab_num = 0
        for tab in semantic_info.by_type['TAB']:
            if '(' not in tab.name:
                tab_num += 1
        sub_num = 0
        for sub in semantic_info.by_type['SUB']:
            if 'SELECT' in sub.name:
                sub_num += 1
        #
        if sub_num >= 1:
            out.append('sub')
        if tab_num > 1:
            out.append('join')
        else:
            out.append('index')
        return out

    def __logic(self):
        # ---- add edges -----#
        struct = REC_FUNC(semantic_info=self.semantic_info,
                          plan_info=self.plan_info,
                          tab_info=self.tab_info,
                          tab_name=self.tab_name,
                          distinct_theta=DIFF_THETA,
                          MysqlSQLStruct=self.mysqlSQLStruct,
                          threshold=THRESHOLD)

        struct.add_to_dict('default', RecomDefault())
        struct.add_to_dict('index', RecomIndex())
        struct.add_to_dict('rules', RecomRules())
        # ----
        stat = self.diagnose()
        # recom = struct.show_edges(stat)
        recom = struct.show_edges(['default','index','rules'])
        return recom

    def predict(self, feature):
        self.__feature_extraction(feature)
        extra_verbose = self.plan_info.Extra_verbose()

        if extra_verbose:
            recom = VerboseUnit()
            recom.add_problems(extra_verbose)
            return recom.show()
        return self.__logic()
