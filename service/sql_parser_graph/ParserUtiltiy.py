# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/11/11
# LAST MODIFIED ON:
# AIM: utility functions

from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnit, ParseUnitList
from typing import Tuple, List, Optional


def find_add_hint_loc(unit: ParseUnit, graph: ParseUnitList) -> int:
    '''
    find loc where to place hint
    :param unit:
    :param graph:
    :return: loc id
    '''
    hint_key = ['SELECT', 'UPDATE', 'DELETE']
    unit_lvl = unit.level
    unit_id = unit.id
    closest_key = -1
    out_loc = -1
    for id in graph.by_id:
        c_unit = graph.by_id[id]
        c_lvl = c_unit.level
        c_name = c_unit.name
        if unit_lvl == c_lvl:
            if c_name in hint_key and id <= unit_id:
                if id > closest_key:
                    out_loc = id
        if c_lvl > unit_lvl:
            break

    return out_loc


def find_add_hint_loc_by_lvl(lvl: int, graph: ParseUnitList) -> int:
    '''

    :param lvl:
    :param graph:
    :return: loc id
    '''
    hint_key = ['SELECT', 'UPDATE','DELETE']
    closest_key = -1
    out_loc = -1
    for id in graph.by_id:
        c_unit = graph.by_id[id]
        c_lvl = c_unit.level
        c_name = c_unit.name
        if lvl == c_lvl:
            if c_name in hint_key:
                if id > closest_key:
                    out_loc = id
        if c_lvl > lvl:
            break

    return out_loc


def find_col_tab_from_view(view_col: str, view_id: int, graph: ParseUnitList) -> Tuple[List[str], str]:
    '''

    :param view_col: col_name from a view
    :param view_id:  view id
    :param graph: parser graph
    :return:  tab_name(list) . col_name
    '''
    view_unit = graph.by_id[view_id]
    tab_names = []
    col = None
    run_once = True
    for id in view_unit.edges:
        child_unit = graph.by_id[id]
        child_name = child_unit.name
        child_type = child_unit.type
        child_alias = child_unit.as_name
        if child_type == 'TAB' and '(' not in child_name and child_name != 'DUAL':
            tab_names.append(child_name)
        if child_type == 'COL' and (child_alias == view_col or child_name == view_col) and run_once:
            col = child_unit
            run_once = False
    if col is None:
        return tab_names, view_col
    else:
        out_tab = []
        out_col = col.name
        col_tab = graph.find_tab(col, tab_only=True)

        for i in col_tab:
            col_tab_name = graph.by_id[i].name
            if col_tab_name in tab_names:
                out_tab.append(col_tab_name)
        return out_tab, out_col


def get_unit_id(name: str, is_alias: True, graph: ParseUnitList, left_most: Optional[int] = None,
                right_most: Optional[int] = None) -> int:
    '''

    :param name:
    :param is_alias:
    :param graph:
    :param left_most: set index range
    :param right_most: set index range
    :return:  id
    '''
    go_loop = False
    id_list = range(len(graph.by_id))
    if left_most:
        id_list = range(left_most - 1, len(graph.by_id))
    if right_most:
        go_loop = True
        id_list = range(0, right_most + 1)

    out_id = -1
    for id in id_list:
        unit = graph.by_id[id]
        unit_name = unit.name
        unit_alise = unit.as_name
        if is_alias:
            if unit_alise == name:
                return id
        else:

            if unit_name == name:
                if not go_loop:
                    return id
                else:
                    if out_id < id:
                        out_id = id
    return out_id

def get_tab_name_by_cols(col_list:List[str], graph:ParseUnitList) -> Optional[str]:
    tab_list = []
    left_tabs = []
    for col in col_list:
        unit = get_unit_id(col,is_alias=False, graph=graph)
        right_tab = [graph.by_id[id].name for id in graph.find_tab(graph.by_id[unit],tab_only=True)]
        tab_list = list(set(left_tabs).union(set(right_tab)))
        if tab_list == []:
            tab_list = left_tabs + right_tab
    if tab_list:
        return tab_list.pop()
    else:
        return None

def get_tab_related_view(tab_id:int, graph:ParseUnitList) -> str:
    tab_unit = graph.by_id[tab_id]
    for p in tab_unit.parent:
        unit = graph.by_id[p]
        if unit.type == 'VIEW':
            return unit.as_name
    return ''