# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/6/20
# LAST MODIFIED ON: 2019/9/29
# AIM: test single sql

from service.predict_sql_review_oracle.Utility.Architecture import AI_FUNC
from service.sql_parser_graph.SQLParser import SQLParser
from service.predict_sql_review_oracle.Utility.OracleExplainAnalysis import OracleExplainAnalysis
import numpy as np
from typing import Union, Optional, Tuple, List

THRESHOLD = 1000  # 做成参数，不要写成hard-code
DIFF_THETA = 0.4


class AI(AI_FUNC):

    def __init__(self, *args):
        super().__init__(*args)

    def rownum_detector(self, semantic: SQLParser) -> Optional[Tuple[int, int, int]]:
        '''
        :param semantic:
        :return: if has rownum return (value, count-level, order by level)
        '''
        elements = semantic.elements
        top_level = 10000
        out = None
        order_by_level = -1
        for ele in elements.by_type['OPT']:
            if ele.name == 'ORDER BY':
                order_by_level = ele.level
        for ele in elements.by_type['STRUCT']:
            if ele.name == 'ROWNUM':
                for idx in elements.get_cousin(ele, level=1):
                    unit = elements.by_id[idx]
                    level = unit.level
                    if unit.type == 'VALUE':
                        try:
                            value = int(unit.name)
                            if level < top_level:
                                top_level = level
                                out = (value, level, order_by_level)
                        except:
                            continue
        return out

    def explain_modify(self, rownum_info: Tuple[int, int, int], explain: OracleExplainAnalysis) -> int:
        '''
        :param rownum_info: [rownum_size, rownum_level] 
        :param explan: 
        :return: Cardinality without rownum 
        '''
        rownum_size, rownum_level, order_by_level = rownum_info
        cardinality = rownum_size
        if order_by_level <= rownum_level: # order by 在 rownum 外面
            for g_id in explain.graph:
                if rownum_level >= explain.graph[g_id]['level']:
                    cardinality += explain.explain.loc[g_id]['CARDINALITY']
            return rownum_size + cardinality
        else:
            X = np.max(explain.explain["CARDINALITY"])
            return int(X)

    def __feature_extraction(self, feature):
        # -- extract cardinality -- #
        explain = feature['plan_raw']
        semantic = feature['sql_text']
        additive = 0

        rownum_info = self.rownum_detector(semantic)
        # if explain.contains('FULL','OPTIONS'):
        #     return 100000
        if rownum_info is not None:
            X = self.explain_modify(rownum_info, explain)
            return X
        X = np.max(explain.explain["CARDINALITY"]) + additive
        return X if not explain.explain.empty else THRESHOLD + 1

    def predict(self, features):
        X = self.__feature_extraction(features)
        if X > THRESHOLD:
            return 0  # 0是拒绝，1是通过
        else:
            return 1
