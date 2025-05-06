# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 
# LAST MODIFIED ON:
# AIM:

from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit
from typing import Tuple, Optional, List, Dict, Union
from service.sql_parser_graph.SQLParser import SQLParser
from service.predict_sql_review_oracle.Utility.OracleExplainAnalysis import OracleExplainAnalysis
import numpy as np
from service.predict_sql_review_oracle.Recom_index import RecomIndex
from service.predict_sql_review_oracle.Utility.SORT import TabTree


class RecomPage(REC_FUNC):
    def __int__(self, **kwargs):
        super().__init__(**kwargs)

    def __verbose(self, key: str, **kwargs) -> Union[str, Dict[str, str]]:
        if key == 'OUT_BOUNDARY':
            return {'PROB': '分页记录数过多', 'SOLVE': '减少分页记录'}
        if key == 'BUILD_INDEX':
            oby_col = kwargs['OBY_COL']
            whr_col = kwargs['WHR_COL']
            tab_name = kwargs['TAB_NAME']
            direction = kwargs['DIRECTION']
            redundance_checker = []
            recom = ''
            index_list = ''
            total_length = len(oby_col) + len(whr_col)
            for col in set(whr_col):
                if col not in redundance_checker:
                    recom += col + '和'
                    index_list += col + ','
                    redundance_checker.append(col)
            for col in set(oby_col):
                if col not in redundance_checker:
                    recom += col + '和'
                    index_list += col + ((' ' + direction) if direction != '' else '') + ','
                    redundance_checker.append(col)
            if total_length > 1:
                recom = '表' + self.alias_table[tab_name] + '中，可以尝试在' + recom[0:-1] + '字段上加组合索引'
            else:
                recom = '表' + self.alias_table[tab_name] + '中，可以尝试在' + recom[0:-1] + '字段上加索引'
            recom += '(倒续)\n' if direction.upper() == 'DESC' else '\n'
            recom += '\t建立索引的DDL语句为： CREATED INDEX xxx(索引名) ON ' + self.alias_table[tab_name] + \
                     '(' + index_list[0:-1] + ')'
            return recom
        if key == 'OTHER_INDEX':
            tab_name = kwargs['tab_name']
            index = kwargs['index_name']
            col_names = self.param['tab_info'].get_col_from_indexInfo(index, self.alias_table[tab_name]).values[0]
            col_list = '('
            for col in col_names:
                col_list += col.lower() + ','
            recom = '表' + self.alias_table[tab_name] + '中，可以使用索引' + index + col_list[0:-1] + ')'
            hint = 'INDEX(' + kwargs['tab_name'] + ' ' + kwargs['index_name'] + ')'
            return {'SOLVE': recom, 'HINT': hint}
        if key == 'PLACE_HOLDER':
            return 'rownum 使用绑定变量，请开发确认绑定变量范围'
        return ''

    def get_tab_col_rownum_orderby_relation(self, semantic: SQLParser) -> Optional[
        Tuple[Dict[str, np.array], Tuple[int, int, int, str], Optional[Dict[str, List[str]]]]]:
        '''

        :param semantic:
        :return: Dict[table_name , List[str]] , (value, count-level, order by level (s))
        '''
        elements = semantic.elements
        rownum_top_level = 100000  # only get outer most level rownum
        order_by_level = -1
        order_by_info = None
        tab_col = dict()
        rownum_info = None
        tabinfo = self.param['tab_info']
        oby_type = ''

        # --- get order by & table col info --- #
        for ele in elements.by_type['OPT']:

            # -- order by info -- #
            if ele.name == 'ORDER BY':
                order_by_level = ele.level
                order_by_info = dict()
                col_list = elements.find_root(ele, col_only=True)
                for col_id in col_list:
                    col = elements.by_id[col_id]
                    tab_unit_list = [elements.by_id[i] for i in elements.find_tab(col, tab_only=True)]
                    tab_alias_list = [unit.get_name() for unit in tab_unit_list]
                    tab_name_list = [unit.name for unit in tab_unit_list]
                    value = tabinfo.chck_col_vld(col.name, tab_name_list)
                    if value is not None:
                        col_name, tab_id = value
                        try:
                            order_by_info[tab_alias_list[tab_id]].append(col_name)
                        except:
                            order_by_info[tab_alias_list[tab_id]] = [col_name]

            # -- tab col relation -- #
            if ele.in_statement == 'WHERE':
                is_placeholder = False
                col_idx_list = elements.find_root(ele, col_only=True)
                if col_idx_list is None: continue
                if len(col_idx_list) > 1 or len(col_idx_list) == 0:
                    continue
                for id in col_idx_list:
                    child = elements.by_id[id]
                    if ':P' in child.name:
                        is_placeholder = True
                        break
                col = elements.by_id[col_idx_list[0]]
                tab_unit_list = [elements.by_id[i] for i in elements.find_tab(col, tab_only=True)]
                tab_alias_list = [unit.get_name() for unit in tab_unit_list]
                tab_name_list = [unit.name for unit in tab_unit_list]
                value = tabinfo.chck_col_vld(col.name, tab_name_list)
                if value is not None:
                    col_name, tab_id = value
                    try:
                        tab_col[tab_alias_list[tab_id]].append(
                            [col_name.upper(), str(is_placeholder), ele.name, ele.level])
                    except:
                        tab_col[tab_alias_list[tab_id]] = [[col_name.upper(), str(is_placeholder), ele.name, ele.level]]
        for k in tab_col.keys():
            tab_col[k] = np.vstack(tab_col[k])
        for ele in elements.by_type['STRUCT']:
            if ele.name == 'ROWNUM' and ele.in_statement == 'WHERE':
                for idx in elements.get_left(ele):
                    unit = elements.by_id[idx]
                    level = unit.level
                    if unit.type == 'VALUE':
                        value = int(unit.name)
                        if level < rownum_top_level:
                            rownum_top_level = level
                            rownum_info = (value, rownum_top_level, order_by_level)
                    else:
                        if 'Placeholder' in str(unit.token.ttype):
                            if level < rownum_top_level:
                                rownum_top_level = level
                                rownum_info = (-1, rownum_top_level, order_by_level)

            if ele.name in ['DESC', 'ASC']:
                oby_type = ele.name
        rownum_info = rownum_info + (oby_type,)

        return tab_col, rownum_info, order_by_info

    def rownum_detector(self, semantic: SQLParser) -> Optional[Tuple[int, int, int, str]]:
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
                        value = int(unit.name)
                        if level < top_level:
                            top_level = level
                            out = (value, level, order_by_level)
        return out

    def explain_modify(self, rownum_info: Tuple[int, int, int, str], explain: OracleExplainAnalysis) -> int:
        '''
        :param rownum_info: [rownum_size, rownum_level]
        :param explan:
        :return: Cardinality without rownum
        '''
        rownum_size, rownum_level, order_by_level, _ = rownum_info
        cardinality = rownum_size
        if order_by_level <= rownum_level:  # order by 在 rownum 外面
            for g_id in explain.graph:
                if rownum_level >= explain.graph[g_id]['level']:
                    cardinality += explain.explain.loc[g_id]['CARDINALITY']
            return rownum_size + cardinality
        else:
            return -1

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[Optional[TransmitContainer], VerboseUnit]:
        # --- initialization --- #
        recom = VerboseUnit(self.param['semantic_info'].elements)
        explain = self.param['plan_info']
        semantic = self.param['semantic_info']
        tabinfo = self.param['tab_info']
        self.add_to_dict('index', RecomIndex())

        # -- core input -- #
        tab_col, rownum_info, order_by_info = self.get_tab_col_rownum_orderby_relation(semantic)

        # --- table name analysis --- #
        tab_name, alias = semantic.get_table_name(alias_on=True)
        self.alias_table = dict()
        for name, a in zip(tab_name, alias):
            self.alias_table[a] = name

        # -- build index recommendation -- #
        if order_by_info:
            oby_tab = list(order_by_info.keys())[0]
            oby_col = order_by_info[oby_tab]
            try:
                whr_col = tab_col[oby_tab][:, 0]
            except:
                whr_col = []

            tab_size = tabinfo.get_tab_numrows(self.alias_table[oby_tab])
            sort_tree = TabTree()
            for col in oby_col:
                distinct = float(tabinfo.get_col_distinc_by_colInfo(col, self.alias_table[oby_tab])) / float(tab_size)
                sort_tree.insert(name=col, distinct=distinct)
            oby_col = sort_tree.tree_traverse(increase=False, out=[])
            sort_tree = TabTree()
            whole = []
            for col in whr_col:
                distinct = float(tabinfo.get_col_distinc_by_colInfo(col, self.alias_table[oby_tab])) / float(tab_size)
                whole.append(col)
                if distinct > 0.0001 and col not in oby_col:
                    sort_tree.insert(name=col, distinct=distinct)
            whr_col = sort_tree.tree_traverse(increase=False, out=[])
            if len(whr_col) == 0 : whr_col = whole
            # -- check if exist a index -- #
            new_index = []
            for col in whr_col + oby_col:
                if col not in new_index:
                    new_index.append(col)
            if rownum_info[3].upper() != 'DESC':
                index_name = tabinfo.chck_idx_exst(table_name=self.alias_table[oby_tab], col_name=new_index)
            else:
                index_name = None
            if index_name:  # -- has index
                # -- check if the index been used --#
                info = explain.has(index_name, 'OBJECT_NAME')
                if not info:  # -- the index hasn't been used
                    verb = self.__verbose(key='OTHER_INDEX', tab_name=oby_tab, index_name=index_name)
                    recom.add_solve(verb['SOLVE'])
                    recom.add_hint(level=rownum_info[2], hint=verb['HINT'])
            else:
                # -- build index -- #
                verb = self.__verbose(key='BUILD_INDEX', OBY_COL=oby_col, WHR_COL=whr_col, TAB_NAME=oby_tab
                                      , DIRECTION=rownum_info[3])
                recom.add_solve(verb)

            # -- resize table -- #
            tab_size = rownum_info[0]
            if tab_size != -1:
                tabinfo.reset_tab_numrows(self.alias_table[oby_tab], rownum_info[0])
            else:
                verb = self.__verbose(key='PLACE_HOLDER')
                recom.add_problems(verb)
            # -- check rest table index -- #
            try:
                del tab_col[oby_tab]
            except:
                pass
            if len(tab_col) != 0:
                transmit = TransmitContainer()
                transmit.index_receive = tab_col
                _, recom_index = self.get_deges('index').recom(info=transmit)
                recom.add(recom_index)

        # --- rownum recommendation --- #
        if rownum_info is not None:
            rownum_size = self.explain_modify(rownum_info, explain)

            if rownum_size != -1 and rownum_size > self.param['threshold']:
                verb = self.__verbose(key='OUT_BOUNDARY')
                recom.add_problems(verb['PROB'])
                recom.add_solve(verb['SOLVE'])

        return None, recom
