# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/1/10 下午4:48
# LAST MODIFIED ON:
# AIM: rule base sql

from service.predict_sql_review_mysql.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit
from typing import Optional, Tuple, Union, Dict


class RecomRules(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __verbose(self, key: str, **kwargs) -> Optional[str]:
        map = {'000': None,
               '001': 'SELECT里不能有子查询',
               '002': '字段左边不能有函数, 这样无法走索引'}
        return map[key]

    ##########################
    #      rules here        #
    ##########################

    def NO_sub_in_SELECT(self) -> str:
        '''
        SELECT 里不能有子查询
        :return:
        '''
        semantic = self.param['semantic_info']
        for id in semantic.elements.by_id:
            unit = semantic.elements.by_id[id]
            if unit.type in ['SUB', 'VIEW'] and unit.in_statement == 'SELECT':
                return '001'
        return '000'

    def NO_func_on_COL(self) -> str:
        '''
        字段左边不能有函数
        :return:
        '''
        semantic = self.param['semantic_info']
        for unit in semantic.elements.by_type['OPT']:
            if unit.in_statement != 'WHERE':
                continue
            edges = list(unit.edges)
            id = unit.id
            for edges_i in edges:
                e_unit = semantic.elements.by_id[edges_i]
                if edges_i >= id:
                    break
                if e_unit.type == ['FUNC']:
                    return '001'
        return '000'


    # ================ done =============== #
    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[Optional[TransmitContainer], VerboseUnit]:
        # --- initialized verbose --- #
        recom = VerboseUnit()
        transmit = TransmitContainer()

        for  func in [
            self.NO_func_on_COL,
            self.NO_func_on_COL
        ]:
            code = func()
            verbose = self.__verbose(code)
            if verbose:
                recom.add_problems(verbose)
        return transmit, recom