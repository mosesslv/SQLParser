# created by bohuai jiang
# on 2019/7/29
# store all architecture units
# -*- coding: utf-8 -*-

from typing import List, Dict, Tuple, List, Optional,Union
import re
from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnitList

class TransmitContainer:
    def __init__(self):
        self._index_transmit = None
        self._index_receive = None

        self._join_transmit = None
        self._join_receive = None

        self._sub_transmit = None
        self._sub_receive = None

    # --- index_transmit --- #
    @property
    def index_transmit(self) -> Tuple[List[str], float, bool]:
        return self._index_transmit

    @index_transmit.setter
    def index_transmit(self, index_info: Tuple[List[str], float, bool]):
        '''
        :param index_info: str or col name
        :return:
            list[str] - col combinations or a index
            float - its distinct value
            bool - true if it is a index else false
        '''
        self._index_transmit = index_info

    @property
    def index_receive(self) -> Dict[str, List[str]]:
        return self._index_receive

    @index_receive.setter
    def index_receive(self, col_tab_info: Dict[str, List[str]]):
        '''

        :param col_tab_info:
        :return: {tab_name : List[col1, col2, ..]}
        '''
        self._index_receive = col_tab_info

    # ---
    @property
    def join_transmit(self) -> Tuple[List[str], float, bool]:
        return self._join_transmit

    @join_transmit.setter
    def join_transmit(self, index_info: Tuple[List[str], float, bool]):
        '''
        :param index_info: str or col name
        :return:
            list[str] - col combinations or a index
            float - its distinct value
            bool - true if it is a index else false
        '''
        self._join_transmit = index_info

    @property
    def join_receive(self) -> Dict[str, List[str]]:
        return self._join_receive

    @join_receive.setter
    def join_receive(self, col_tab_info: Dict[str, List[str]]):
        '''

        :param col_tab_info:
        :return: {tab_name : List[col1, col2, ..]}
        '''
        self._join_receive = col_tab_info



class VerboseUnit:
    def __init__(self):
        self.problems = []
        self.solve = []
        self.hint = dict()

    def add_problems(self, prob: Union[str, List[str]]):
        if type(prob) == str:
            self.problems.append(prob)
        else:
            self.problems.extend(prob)

    def add_solve(self, sol: str):
        self.solve.append(sol)

    def add_hint(self, level: int, hint: str):
        if level in self.hint.keys():
            self.hint[level] += hint
        else:
            self.hint[level] = hint

    def add_hint_dict(self, hint:Dict[int,str]):
        for k in hint.keys():
            if k not in self.hint.keys():
                self.hint[k] = hint[k]
            else:
                value = hint[k]
                if value not in self.hint[k]:
                    self.hint[k] += value

    def add(self, unit: 'VerboseUnit'):
        self.problems.extend(unit.problems)
        # for sol in unit.solve:
        #     if '请评估是否还有效率更高的扫描方式' not in sol:
        #         self.solve.append(sol)
        self.solve.extend(unit.solve)
        self.add_hint_dict(unit.hint)

    def _verbose_clean(self, values: List[str]) -> str:
        out = ''
        values = set(values)
        for v in values:
            if v != '':
                out +='\t' + v + '\n'
        return out

    def _clean_all_verbose(self):
        problems = self._verbose_clean(self.problems)
        solve = self._verbose_clean(self.solve)
        return problems, solve

    def show(self) -> str:
        problems, solve = self._clean_all_verbose()
        recom = ''
        if self.problems:
            recom += '问题点:\n' + problems
        if self.solve:
            recom += '建议:\n' + solve
        # if len(self.hint) != 0:
        #     recom += '加入hint:\n'
        #     for k in sorted(self.hint.keys()):
        #         value = self.hint[k]
        #         recom += '\t第' + str(k+1) + '层加入:/*+' + value + '*/\n'
        return recom


# -- basic AI function units

class AI_FUNC:

    def __init__(self, *args):
        pass

    def __feature_extraction(self, feature):
        pass

    def __feature_preprocess(self, x):
        pass

    def predict(self, x):
        pass

    def get_problem_tag(self, recom: str):
        class_tag = []
        type = {'全表扫': '全表扫',
                '范围': '扫描范围过大',
                '区分度': '索引区分度不足',
                'use': '表链接',
                'ROWNUM': '分页问题',
                'UNNEST': '子查询'}

        for key in type:
            if key in recom:
                class_tag.append(type[key])
        if not class_tag:
            class_tag.append('索引问题')
        return class_tag

# -- basic AI architecture units

class AI_ARC:

    def __init__(self):
        self._edges = []
        # self._parents = []

    def __iter__(self):
        return iter(self._edges)

    @property
    def edges(self):
        return self._edges

    @edges.setter
    def edges(self, edges: List['AI_ARC']):
        self._edges = edges

    def add(self, unit: 'AI_ARC'):
        self._edges.append(unit)


class REC_FUNC():

    def __init__(self, **kwargs) -> None:
        self._func_dict = dict()
        self._edges = []
        self.param = kwargs

    def set(self, values: dict) -> None:
        self.param = values

    def __verbose(self, key: str, **kwargs) -> str:
        pass

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[TransmitContainer, VerboseUnit]:
        '''
        purpose: for model to model interaction
        :return: object- needed arguments , str- recommendation if has any
        '''
        pass

    @property
    def func_dict(self) -> Dict[str, 'AI_ARC']:
        return self._func_dict

    @func_dict.setter
    def func_dict(self, func_dict: Dict[str, 'REC_FUNC']) -> None:
        self._func_dict = func_dict

    def get_deges(self, key: str) -> 'REC_FUNC':
        return self._func_dict[key]

    def add_to_dict(self, key: str, unit: 'REC_FUNC') -> None:
        unit.set(self.param)
        self._func_dict[key] = unit
        self._edges.append(self._func_dict.values())

    def show_edges(self, key_list: List[str]) -> str:
        recom = VerboseUnit()
        for key in key_list:
            _, rec = self.get_deges(key=key).recom()
            recom.add(rec)

        return recom.show()
################################33


# -- basic recommendation function units
