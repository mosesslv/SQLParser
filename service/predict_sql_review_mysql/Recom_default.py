# created by bohuai jiang
# on 2019/7/29
# -*- coding: UTF-8 -*-

from service.predict_sql_review_mysql.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit
from typing import Tuple, Optional, List
from service.predict_sql_review_mysql.Utility.MysqlExplainAnalysis import MysqlExplainAnalysis
import pandas
import numpy as np


class RecomDefault(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __verbose(self, key: str, **kwargs) -> str:
        translate = {'ALL': '全表扫描', 'INDEX': '全索引扫描', 'RANGE': '索引范围扫描', 'REF': '非唯一索引扫描',
                     'EQ_REF': '唯一索引扫描'}
        if key == 'FULL':
            tb_names = set(kwargs['tab_names'])
            reco = '表'
            for name in tb_names:
                reco += name + '和'
            return reco[0:-1] + '使用全表扫描'
        if key == 'INDEX_FULL':
            tb_names = set(kwargs['tab_names'])
            reco = '表'
            for name in tb_names:
                reco += name + '和'
            return reco[0:-1] + '使用索引全表扫描'
        if key == 'GENERAL':
            tb_num = kwargs['tab_num']
            return f'SQL 扫描了{tb_num:d}张表'

        if key == 'INDEX':
            pattens = kwargs['info']
            reco = ''
            for ele in pattens:
                opt = ''
                for token in ele[2].split(' '):
                    opt += translate[token] if token in translate.keys() else token
                col_names = self.param['tab_info'].get_col_from_indexInfo(ele[0], ele[1]).values[0]
                col_list = '('
                for col in col_names:
                    col_list += col.lower() + ','
                reco += '表' + ele[1] + '在索引' + ele[0] + col_list[0:-1] + ')上，使用' + opt
                if '全' not in opt:
                    reco += '，扫描范围过大'
                reco += ''
            return reco
        if key == 'BUILD_INDEX':
            reco_ = ''
            reco = ''
            for index in kwargs['col']:
                reco_ += index + '和'
            reco += '在表' + kwargs['tab_name'] + '的字段' + reco_[0:-1] + '上，建立组合索引'
            return reco

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[Optional[TransmitContainer], VerboseUnit]:
        recom = VerboseUnit()
        tabInfo = self.param['tab_info']
        # -- recommend form table features -- #
        explain = self.param['plan_info']  # .sort_values(by='CARDINALITY', ascending=False)
        semantic_info = self.param['semantic_info']
        tab_names, aliases = semantic_info.get_table_name(alias_on=True)
        self.aliase_table = dict()
        # -- build table alise -- #
        for name, as_name in zip(tab_names, aliases):
            self.aliase_table[as_name] = name
        # --------------- #
        # -- read data -- #
        # --------------- #

        # --- full scan --- #
        full = explain.has('ALL', 'type')
        if full:
            name_list = []
            for row in full:
                name = row['table']
                try:
                    name = self.aliase_table[name]
                except:
                    continue
                name_list.append(name)
            if name_list:
                recom.add_problems('' + self.__verbose(key='FULL', tab_names=name_list) + '')

        # --- index full scan --- #
        idx_full = explain.has('INDEX', 'type')
        if idx_full:
            name_list = []
            for row in idx_full:
                name = row['table']
                try:
                    name = self.aliase_table[name]
                except:
                    continue
                name_list.append(name)
            if name_list:
                recom.add_problems('' + self.__verbose(key='INDEX_FULL', tab_names=name_list) + '')

        # --- check numb of used index pre tab -- #
        # for id in explain.explain.index:
        #     if  explain.explain.loc[id, 'key'] is None:
        #         continue
        #     index_names = explain.explain.loc[id, 'key'].split(',')
        #     tab_name = explain.explain.loc[id,'table']
        #     if len(index_names) > 1:
        #         col_names = []
        #         for idx_name in index_names:
        #             cols = tabInfo.get_col_from_indexInfo(idx_name.upper(), self.aliase_table[tab_name.upper()]).values[0]
        #             for c in cols:
        #                 if c not in col_names:
        #                     col_names.append(c)
        #         recom.add_solve(''+ self.__verbose(key='BUILD_INDEX', col = col_names, tab_name = tab_name))

        return None, recom
