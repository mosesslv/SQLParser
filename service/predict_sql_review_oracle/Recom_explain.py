# created by bohuai jiang
# on 2019/7/29
# -*- coding: UTF-8 -*-

from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit
from typing import Tuple, Optional, List, Dict
from service.sql_parser_graph.SQLParser import SQLParser
import numpy as np
from service.predict_sql_review_oracle.AIException import SQLConflict


class RecomDefault(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __verbose(self, key: str, **kwargs) -> str:
        translate = {'FULL': '全索引', 'SCAN': '扫描', 'FAST': '快速', 'RANGE': '索引范围'}
        if key == 'GENERAL':
            tb_num = kwargs['tab_num']
            return f'SQL 扫描了{tb_num:d}张表'
        if key == 'FULL':
            tb_names = set(kwargs['tab_names'])
            reco = '表'
            for name in tb_names:
                tab_size = self.param['tab_info'].get_tab_numrows(name)
                if tab_size > 10000:
                    reco += name + '和'
            if reco[0:-1] != '':
                return reco[0:-1] + '使用全表扫描'
            else:
                return ''
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
        if key.upper() == 'WHERE':
            used_equal = kwargs['used_equal']
            pattens = kwargs['info']
            opt = ''
            for token in pattens.split(' '):
                opt += translate[token] if token in translate.keys() else token
            tab_name = self.table_alias[kwargs['tab_name']]
            index = kwargs['index_name']
            col_names = self.param['tab_info'].get_col_from_indexInfo(index, tab_name).values[0]
            col_list = '('
            for col in col_names:
                col_list += col.lower() + ','
            if used_equal:
                return '扫描表' + tab_name + '在索引' + kwargs[
                    'index_name'] + col_list[0:-1] + ')上,使用' + opt + '但参数区分度可能不高'
            else:
                return '扫描表' + tab_name + '在索引' + kwargs[
                    'index_name'] + col_list[0:-1] + ')上,使用' + opt + '但参数范围可能过大'
        if key.upper() == 'PLACE_HOLDER':
            tab_name = kwargs['tab_name']
            return '表' + self.table_alias[tab_name] + '中,' + kwargs['index_name'] + '使用绑定变量，请确定绑定变量取值范围'
        if key.upper() == 'PLACE_HOLDER_TIME':
            tab_name = kwargs['tab_name']
            return '表' + self.table_alias[tab_name] + '中,' + kwargs['index_name'] + '使用绑定变量，请确保时间范围合理'
        if key.upper() == 'FUZZY_SEARCH':
            tab_name = kwargs['tab_name']
            col_name = kwargs['col_name']
            return '表' + self.table_alias[tab_name] + '的字段' + col_name + '上使用前置模糊匹配,去掉前置模糊匹配'

    def __get_tab_col_relation(self, sem_info: SQLParser) -> Dict[str, np.array]:
        '''
        :param sem_info: sql semantic information
        :return: {tab_nam:[col_names, is_bindVariable, operation, level]}
        '''
        sem_info = sem_info.elements
        out = dict()
        tabInfo = self.param['tab_info']

        for opt in sem_info.by_type['OPT']:
            if opt.in_statement == 'WHERE':
                is_placeholder = False
                children_ids = sem_info.find_root(opt)
                for id in children_ids:
                    child = sem_info.by_id[id]
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
                        tab_name = tab_alias[tab_idx]
                        if tab_name.upper() not in out.keys():
                            out[tab_name.upper()] = [[col_name.upper(), str(is_placeholder), opt.name, opt.level]]
                        else:
                            out[tab_name.upper()].append([col_name.upper(), str(is_placeholder), opt.name, opt.level])
        for k in out.keys():
            out[k] = np.vstack(out[k])
        return out

    def get_tab_name_by_index(self, index_name: str) -> Optional[str]:
        tab_info = self.param['tab_info']
        for tab_name in tab_info:
            index_info = tab_info[tab_name]['table_indexes']
            value = index_info[index_info['INDEX_NAME'] == index_name]
            if not value.empty:
                return tab_name
        return None

    def rownum_placeholder_detector(self, semantic: SQLParser, verbose: VerboseUnit) -> None:
        '''

        :param semantic:
        :return: recommendation
        '''
        elements = semantic.elements
        for ele in elements.by_type['OPT']:
            is_rownumber = False
            for c in sorted(ele.edges):
                unit = elements.by_id[c]
                # level = unit.level
                if unit.name == 'ROWNUM':
                    is_rownumber = True
                    continue
                if is_rownumber:
                    if ':P' in unit.name:
                        # -- found placeholder -- #
                        idlist = sorted([ele.id] + list(ele.edges))
                        sql_code = ' '.join(elements.by_id[i].token.value for i in idlist)
                        verbose.add_problems('发现' + sql_code + '使用绑定变量，请开发确定绑定变量范围')

                    if unit.type == 'VALUE' and int(unit.name) > self.param['threshold']:
                        verbose.add_problems('分页记录数过多')
                        verbose.add_solve('减少分页记录数')

    def __get_used_indexes(self, col_list: Dict[str, np.array]) -> Dict[str, np.array]:
        '''
        :return: tab- name :  (key-index_name, value-distinct key, cardinality, Options)
        '''
        Explain = self.param['plan_info']
        indexes = Explain.has('INDEX', 'OPERATION')
        tabInfo = self.param['tab_info']
        tab_index = []
        for index in indexes:
            index_name = index['OBJECT_NAME']
            tab_name = tabInfo.get_tabName_by_idxName(index_name)
            if not tab_name:
                raise SQLConflict('AIErr_Oracle_20003')
            tab_index.append((tab_name, index_name))

        out = dict()
        for pair in tab_index:
            tab_name = pair[0]
            idx_name = pair[1]
            for key in col_list:
                if tab_name == self.table_alias[key]:
                    tab_name = tab_name
                    index_explain = Explain.has(idx_name, 'OBJECT_NAME')[0]
                    index_info = tabInfo.get_IndexInfo(tab_name)
                    row_num = tabInfo.get_tab_numrows(tab_name)
                    distinc = index_info[index_info['INDEX_NAME'] == idx_name].DISTINCT_KEYS
                    corr_key = index_info[index_info['INDEX_NAME'] == idx_name].INDEX_COLUMNS
                    cardinality = index_explain.CARDINALITY
                    option = index_explain.OPTIONS
                    is_in_index = False
                    if not distinc.empty:
                        for col_name in col_list[key][:, 0]:
                            if col_name in corr_key.values[0]:
                                is_in_index = True
                        if is_in_index:
                            if key in out.keys():
                                out[key].append(
                                    [idx_name, str(float(distinc.values[0]) / (row_num + 0.00001)), str(cardinality),
                                     option, ','.join(corr_key.values[0])])
                            else:
                                out[key] = [
                                    [idx_name, str(float(distinc.values[0]) / (row_num + 0.00001)), str(cardinality),
                                     option, ','.join(corr_key.values[0])]]
        for key in out.keys():
            out[key] = np.vstack(out[key])
        return out

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[Optional[TransmitContainer], VerboseUnit]:
        recom = VerboseUnit(self.param['semantic_info'].elements)
        # --- build table alias --- #
        tab_name, alias_name = self.param['semantic_info'].get_table_name(alias_on=True)
        self.table_alias = dict()
        for tab, alias in zip(tab_name, alias_name):
            self.table_alias[alias] = tab
        #######################################
        # -- recommend form table features -- #
        #######################################

        explain = self.param['plan_info']  # .sort_values(by='CARDINALITY', ascending=False)

        #####################
        # --- full scan --- #
        #####################

        full = explain.has('FULL', 'OPTIONS')
        if full:
            name = [v['OBJECT_NAME'] for v in full]
            recom.add_problems('' + self.__verbose(key='FULL', tab_names=name) + '')

        ############################
        # --- collect problems --- #
        ############################

        col_in_sql = self.__get_tab_col_relation(self.param['semantic_info'])
        used_indexes = self.__get_used_indexes(col_in_sql)
        TabelInfo = self.param['tab_info']
        checked_index = []
        checked_col = []
        for tab in col_in_sql.keys():
            # --- place holder handler --- #
            if len(used_indexes) > 0:
                for i, values in enumerate(col_in_sql[tab]):
                    if values[1] == 'True':  # and values[0] not in checked_col and values[2] in ['<', '>', '>=', '<=']:
                        try:  # case: multiple table
                            index_col = used_indexes[tab][:, 4]
                            the_index = used_indexes[tab][0, 0]
                        except:
                            continue
                        if values[0] in ','.join(x for x in index_col):
                            distinc = TabelInfo.get_index_distinc(index_name=the_index, tab_name=self.table_alias[tab])
                            distinc = float(distinc) / TabelInfo.get_tab_numrows(self.table_alias[tab])
                            if not (values[2] == '=' and distinc > self.param['distinct_theta']):
                                if self.param['tab_info'].get_col_type(values[0],self.table_alias[tab]) == 'DATE':
                                    recom.add_problems(
                                        self.__verbose(key='PLACE_HOLDER_TIME', index_name=values[0], tab_name=tab))
                                else:
                                    recom.add_problems(
                                        self.__verbose(key='PLACE_HOLDER', index_name=values[0], tab_name=tab))
                                checked_col.append(values[0])
                    # --- Fuzzy search ---- #
                    if values[2] == 'LIKE':
                        recom.add_solve(self.__verbose(key='FUZZY_SEARCH', tab_name=tab, col_name=values[0]))
                    # -- get related index name, given col name --#
                    index_names = TabelInfo.find_indexName_by_colName(self.table_alias[tab], values[0])
                    index_list = []
                    if tab in used_indexes.keys():
                        for name in index_names:
                            # -- search from used index --#
                            for i, idx_name in enumerate(used_indexes[tab][:, 0]):
                                if idx_name == name:
                                    index_list.append(used_indexes[tab][i, :])
                                    break
                        for v in index_list:
                            i_name = v[0]
                            if float(v[2]) >= self.param['threshold'] and i_name not in checked_index:
                                # -- add default verbose -- #
                                info = v[3]
                                # -- #
                                checked_index.append(i_name)
                                used_equal = True if values[2] == '=' else False
                                recom.add_problems(
                                    self.__verbose(key='WHERE', index_name=i_name, tab_name=tab, info=info,
                                                   used_equal=used_equal))

        return None, recom
