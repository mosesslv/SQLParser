# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/11/18
# LAST MODIFIED ON:
# AIM: extend function for sql parser
from service.sql_parser_graph.SQLParser import SQLParser
from service.sql_parser_graph.units import ParseUnit, ParseUnitList
from typing import Tuple, List, Optional


def find_col_tab_from_view(view_col_id: int, view_id: int, graph: ParseUnitList) -> Tuple[List[int], int]:
    '''

    :param view_col: col_name from a view
    :param view_id:  view id
    :param graph: parser graph
    :return:  tab_name_id(list) . col_name_id
    '''
    view_unit = graph.by_id[view_id]
    tab_names = []
    col = None
    run_once = True
    view_col = graph.by_id[view_col_id].name
    for id in view_unit.edges:
        child_unit = graph.by_id[id]
        child_name = child_unit.name
        child_type = child_unit.type
        child_alias = child_unit.as_name
        if child_type == 'TAB' and '(' not in child_name and child_name != 'DUAL':
            tab_names.append(id)
        if child_type == 'COL' and (child_alias == view_col or child_name == view_col) and run_once:
            # -- avoid been overwrite by view_col_id
            col = id
            run_once = False
    if col is None:
        return tab_names, view_col
    else:
        out_tab = []
        out_col = col.id
        col_tab = graph.find_tab(col, tab_only=True)
        for i in col_tab:
            if i in tab_names:
                out_tab.append(i)
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
