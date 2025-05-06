# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/10/31
# LAST MODIFIED ON:
# AIM: main function

from service.predict_sql_review_mysql.Utility.Architecture import AI_FUNC
from service.predict_sql_review_mysql.Recom_default import RecomDefault
from service.predict_sql_review_mysql.Recom_index import RecomIndex
from typing import List, Dict, Tuple
from service.predict_sql_review_mysql.Utility.Architecture import REC_FUNC
from service.predict_sql_review_mysql.Utility.Architecture import VerboseUnit
from service.sql_parser_graph.SQLParser import SQLParser
from service.predict_sql_review_mysql.Utility.MysqlExplainAnalysis import MysqlExplainAnalysis
from service.predict_sql_review_mysql.Utility.TableInfoAnalysis import TabelInfoAnaylsis
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

        self.recom = VerboseUnit()

    def __feature_extraction(self, feature):
        self.__init_para()

        self.semantic_info = feature['sql_text']
        self.plan_info = feature['plan_raw']
        self.tab_info = feature['tab_info']
        self.tab_name = feature['tab_name']

        self.mysqlSQLStruct = feature['MysqlSQLStruct']

        tab_names, aliases = self.semantic_info.get_table_name(alias_on=True)
        self.aliase_table = dict()
        # -- build table alise -- #
        for name, as_name in zip(tab_names, aliases):
            self.aliase_table[as_name] = name

    def __feature_preprocess(self, x):
        pass

    def predict(self, feature) -> Tuple[int, str]:
        '''

        :param feature:
        :return: score and recommendation
        '''
        self.__feature_extraction(feature)
        grand = self.sql_grade()
        recom = self.sql_recom()
        return grand, recom

    #############################
    #     utility Functions     #
    #############################
    def __verbose(self, key: str, **kwargs) -> str:
        if key == 'INSERT':
            return '发现VALUE后插入数据太多,可能会影响到sql性能'
        if key == 'FULL':
            tb_names = set(kwargs['tab_names'])
            reco = '表'
            for name in tb_names:
                reco += self.aliase_table[name.upper()] + '和'
            return reco[0:-1] + '使用全表扫描'

    def __check_equal_only(self) -> bool:
        '''
        only use "=" under where statement
        :param sem_info:
        :return:
        '''
        sem_info = self.semantic_info
        for opt in sem_info.elements.by_type['OPT']:
            if opt.in_statement == 'WHERE' and opt.name != '=':
                return False
        return True

    def __check_use_pk_uk_only(self) -> bool:
        '''
        check if the sql used pk or uk only
        :param sem_info:
        :param explain:
        :param tabInfo:
        :return:
        '''
        sem_info = self.semantic_info
        explain = self.plan_info
        tabInfo = self.tab_info
        explain_ = explain.explain
        tab_name, aliase = sem_info.get_table_name(alias_on=True)
        aliase_table = dict()
        # -- build table alise -- #
        for name, as_name in zip(tab_name, aliase):
            aliase_table[as_name.upper()] = name.upper()
        for id in explain_.index:
            index_names = explain_.loc[id, 'key'].split(',')
            for index_name in index_names:
                if explain_.loc[id, 'table'] is None:
                    continue
                tab_name = aliase_table[explain_.loc[id, 'table'].upper()]
                index_info = tabInfo.get_IndexInfo(tab_name)
                index_non_unique = index_info[index_info['INDEX_NAME'] == index_name]['NON_UNIQUE'].values[0]
                if not (index_name.upper() == 'PRIMARY' or index_non_unique == 0):
                    return False
        return True

    def __get_tab_col_relation(self, sem_info: SQLParser, tabInfo: TabelInfoAnaylsis) -> Dict[
        str, np.array]:
        '''
        :param sem_info: sql semantic information
        :return: {tab_nam:[col_names, is_bindVariable, operation]}
        '''
        sem_info = sem_info.elements
        out = dict()

        for opt in sem_info.by_type['OPT']:
            if opt.in_statement == 'WHERE' and opt.name in ['>', '<', '>=', '<=', 'IN']:
                is_placeholder = False
                for ele in opt.edges:
                    if type(ele) == str:
                        if 'Token.Name.Placeholder' in ele.split('-'):
                            is_placeholder = True
                col_ids = sem_info.find_root(opt, col_only=True)
                for id in col_ids:
                    each_col = sem_info.by_id[id]
                    col_name = each_col.name
                    tab_name = [sem_info.by_id[i].name for i in sem_info.find_tab(each_col, tab_only=True)]
                    values = tabInfo.check_col_valid(col_name, tab_name)
                    if values is None:
                        continue
                    col_name, tab_name = values
                    if tab_name.upper() not in out.keys():
                        out[tab_name.upper()] = [[col_name.upper(), str(is_placeholder), opt.name, opt.level]]
                    else:
                        out[tab_name.upper()].append([col_name.upper(), str(is_placeholder), opt.name, opt.level])
        for k in out.keys():
            out[k] = np.vstack(out[k])
        return out

    def __find_index_by_columns(self, cols: List[str], tab: str, tabInfo: TabelInfoAnaylsis) -> float:
        # cols = liset(cols))
        index_info = tabInfo.get_IndexInfo(tab)
        table_size = tabInfo.get_tab_rownumb(tab)
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

    #####################
    #     grade sql     #
    #####################

    def sql_grade(self) -> int:
        '''
        给sql打分
        :return: score(0-1)
        '''

        # --- FULL SCAN filter --- #
        full = self.plan_info.has('ALL', 'type')
        if full and self.semantic_info.sql_statement() != "INSERT":
            tab_names = []
            for full_scan in full:
                tab_names.append(full_scan['table'])
            self.recom.add_problems(self.__verbose(key='FULL', tab_names=tab_names))
            return 0
        # --- PK/UL only filter --- #
        if self.__check_equal_only() and self.__check_use_pk_uk_only():
            return 1
        # --- INSERT filter --- #
        if self.semantic_info.sql_statement() == "INSERT":
            num_col = self.__count_insert_columns(self.semantic_info)
            if num_col == 0:
                return 0
            else:
                num_value_trunk = self.__count_insert_value_trunk(self.semantic_info)
                if num_value_trunk > VALUES_THRESHOLD:
                    self.recom.add_problems(self.__verbose(key='INSERT'))
                    return 0
                else:
                    return 1
        else:
            if not self.plan_info.is_valid_Explain():
                return 0
        # --- EXPLAIN ROWS filter --- #
        X = self.plan_info.get_Cardinality()

        if X < THRESHOLD and X >= 0:
            return 1
        else:
            return 0

    #########################
    #     recommend sql     #
    #########################
    def sql_recom(self):
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
        # ----
        recom = struct.show_edges(['default', 'index'])
        return recom


