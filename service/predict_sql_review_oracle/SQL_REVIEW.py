# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/23 下午4:49
# LAST MODIFIED ON:
# AIM:　将 Discriminator 和　recommendation 合并

from service.predict_sql_review_oracle.Utility.Architecture import AI_FUNC, REC_FUNC
from service.predict_sql_review_oracle.Utility.Architecture import VerboseUnit
from service.predict_sql_review_oracle.Recom_index_v2 import RecomIndex
from typing import Tuple, List
import numpy as np

THRESHOLD = 1000
VALUES_THRESHOLD = 50
DIFF_THETA = 0.4


class REVIEW(AI_FUNC):
    def __init__(self, *args):
        super().__init__(*args)

    def __init_para(self) -> None:
        # -- feautres -- #
        self.plan_info = None
        self.semantic_info = None
        self.tab_info = None
        self.tab_name = None

    def __feature_extraction(self, feature):
        self.__init_para()

        self.semantic_info = feature['sql_text']
        self.plan_info = feature['plan_raw']
        self.tab_info = feature['tab_info']
        self.tab_name = feature['tab_name']

        self.oracleSQLStruct = feature['OracleSQLStruct']

        self.recom = VerboseUnit(self.semantic_info.elements)

        tab_names, aliases = self.semantic_info.get_table_name(alias_on=True)
        self.aliase_table = dict()

        # -- build table alise -- #
        for name, as_name in zip(tab_names, aliases):
            self.aliase_table[as_name] = name

    def __feature_preprocess(self, x):
        pass

    def predict(self, feature):
        self.__feature_extraction(feature)
        grand = self.sql_grand()  # grand given sql performance
        recom = self.sql_recom()  # give sql recommendation
        return grand,recom

    # #############################
    #     utility Functions     #
    #############################

    def __verbose(self, key: str, **kwargs) -> str:
        pass

    def sql_grand(self):
        rownum_info = self.rownum_detector()
        # --- full scan --- #
        if self.plan_info.contains('FULL', 'OPTIONS'):
            return 0

        crdnlty = self.get_Crdnlty_without_rownum(rownum_info)
        if self.plan_info.explain.empty:
            return 0
        if crdnlty > THRESHOLD:
            return 0
        else:
            return 1

    def sql_recom(self):
        struct = REC_FUNC(semantic_info=self.semantic_info,
                          plan_info=self.plan_info,
                          tab_info=self.tab_info,
                          tab_name=self.tab_name,
                          distinct_theta=DIFF_THETA,
                          OracleSQLStruct=self.oracleSQLStruct,
                          threshold=THRESHOLD)

        # struct.add_to_dict('default', RecomDefault())
        struct.add_to_dict('index', RecomIndex())
        # struct.add_to_dict('join', RecomJoin())
        # struct.add_to_dict('sub', RecomSub())
        # struct.add_to_dict('page', RecomPage())
        status = self.diagnose()
        recom = struct.show_edges_V2(['index'], self.semantic_info.origin_sql)

        return recom

    # ----------------- #
    #  service utility  #
    # ----------------- #

    # --- grand --- #

    def rownum_detector(self) -> List[int]:
        '''

        :return:  rownum level , order by level and rownum size (-1 if it is
        bind variable )
        '''
        element = self.semantic_info.elements
        top_level = 10000
        out = [-1, -1, -1]
        for ele in element.by_type['OPT']:
            if ele.name == 'ORDER BY':
                out[1] = ele.level
        for ele in element.by_type['STRUCT']:
            if ele.name == 'ROWNUM':
                for idx in element.get_cousin(ele, level=1):
                    unit = element.by_id[idx]
                    if unit.name.isdigit():
                        value = int(unit.name)
                        if unit.level < top_level:
                            top_level = unit.level
                            out[0] = unit.level
                            out[2] = value
        return out

    def get_Crdnlty_without_rownum(self, rownum_info: List[int]) -> int:
        '''

        :param rownum_info: rownnum_lvl, order_by lvl,  rownum size
        :return: Cardinality
        '''
        rownum_lvl, orderBy_lvl, rownum_size = rownum_info
        if rownum_lvl == -1 or rownum_size == -1:
            return np.max(self.plan_info.explain['CARDINALITY'])
        cardinality = rownum_size
        if orderBy_lvl <= rownum_lvl:  # order by 在 rownum 外面
            for g_id in self.plan_info.graph:
                if rownum_lvl >= self.plan_info.graph[g_id]['level']:
                    cardinality += self.plan_info.explain.loc[g_id]['CARDINALITY']
            return rownum_size + cardinality

        return np.max(self.plan_info.explain['CARDINALITY'])

    def get_Selectivity(self,rownum_info: List[int],Cardinality: int)  -> float:
        """
        calculate the Selectivity based on the fomula of :
            Selectivity = cardinality / Total number of rows
        refer: https://gerardnico.com/db/oracle/selectivity
               https://stackoverflow.com/questions/22272165/oracle-selectivity-cardinality
        THIS IS A MORE PREFERED MEASUREMENT OF DISTINCT VALUE GIVEN A SQL （with index）
        """
        _, _, rownum_size = rownum_info
        selectivity = float( Cardinality / rownum_size)*100
        if 0 < selectivity < 1:
            return selectivity
        else:
            return 0.1 # just a user pre-determined number (rule-of-thumb)

    # --- recommend --- #

    def diagnose(self) -> List[str]:
        out = ['default', 'index']
        # -- join -- #
        tab_num = len(self.aliase_table)
        union_num = self.get_union_num()
        if tab_num > 1 and union_num != tab_num - 1:
            out.append('join')
            if 'index' in out:
                out.remove('index')

        # -- sub -- #
        sub = self.get_sub_num()
        if sub > 0:
            out.append('sub')

        # -- page -- #
        rownum_info = self.rownum_detector()
        if rownum_info[0] != -1 and rownum_info[1] != -1:
            out.append('page')
            if 'index' in out:
                out.remove('index')
        return out

    def get_sub_num(self) -> int:
        out = 0
        for unit in self.semantic_info.elements.by_type['SUB']:
            if unit.level > out:
                out = unit.level
        return out

    def get_union_num(self) -> int:
        cnt = 0
        for unit in self.semantic_info.elements.by_type['JOIN']:
            if unit.name in ['UNION', 'UNION ALL']:
                cnt += 1
        return cnt
