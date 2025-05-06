# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/8/1
# LAST MODIFIED ON: 2019/10/6 10:34
# AIM: Recommend table join


from service.predict_sql_review_oracle.Recom_index import RecomIndex
from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC, TransmitContainer, VerboseUnit
from typing import Dict, Tuple, List, Optional
from service.predict_sql_review_oracle.Utility.TableInfoAnalysis import TabelInfoAnaylsis
import service.sql_parser_graph.ParserUtiltiy as pu
import math
import numpy as np
from service.predict_sql_review_oracle.Utility.SORT import TabTree
import copy


class JoinSplitConatiner:
    def __init__(self, tab_names: List[str]):
        self.connect = dict()  # {tab : {tab: {col:bool}}}
        self.tabs = tab_names
        for tab in tab_names:
            self.connect[tab] = dict()

    def add(self, tab_names: List[str], col_names: List[str], level: List[int]) -> None:
        left_t = tab_names[0]
        right_t = tab_names[1]
        left_c = col_names[0]
        right_c = col_names[1]

        if right_t not in self.connect[left_t]:
            #                                 col　 placeholder operation  level
            self.connect[left_t][right_t] = [[right_c, 'False', '=', level[0]]]
        else:
            self.connect[left_t][right_t].append([right_c, 'False', '=', level[0]])
        if left_t not in self.connect[right_t]:
            self.connect[right_t][left_t] = [[left_c, 'False', '=', level[0]]]
        else:
            self.connect[right_t][left_t].append([left_c, 'False', '=', level[0]])

    def get_join_tab_col_info(self, tab_nam: str) -> Dict[str, np.array]:
        for k in self.connect.keys():
            for kk in self.connect[k].keys():
                self.connect[k][kk] = np.vstack(self.connect[k][kk])
        return self.connect[tab_nam]


class RecomJoin(REC_FUNC):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # self.alias_table = dict()
        self.table_alias = dict()
        self.tab_name = []
        self.connect_type = ''
        self.start_with_hj = False

    def _col_split(self) -> Tuple[
        Dict[str, np.array], JoinSplitConatiner]:
        '''
        :param sem_info:
        :return: independent COLs - {TAB:{COL1:use_placeholder}}
                 Join COLS - {TAB: [(joined_TABm,TAB_joined_col)]}
        '''
        sem_info = self.param['semantic_info'].elements
        tabInfo = self.param['tab_info']
        indepen_col = dict()

        join_col = JoinSplitConatiner(list(self.table_alias.keys()))

        for opt in sem_info.by_type['OPT']:
            is_join_col = True
            is_placeholder = False

            children_ids = sem_info.find_root(opt)
            cnt_col = 0
            if len(children_ids) >= 2:
                for id in children_ids:
                    child = sem_info.by_id[id]
                    if child.type == 'COL':
                        cnt_col += 1
                        if ':P' in child.name:
                            is_placeholder = True
                if cnt_col < 2:
                    is_join_col = False
            else:
                is_join_col = False
            # -- find index --#
            join_tab_names = []
            join_col_names = []
            join_level = []
            for id in children_ids:
                each_col = sem_info.by_id[id]
                if each_col.type == 'COL':
                    col_name = each_col.name
                    tab_list = [sem_info.by_id[i] for i in sem_info.find_tab(each_col, tab_only=True)]
                    tab_alias = [ele.get_name() for ele in tab_list]
                    tab_names = [ele.name for ele in tab_list]
                    if len(tab_list) == 1 and tab_list[0].type == 'VIEW':
                        tab_name = tab_alias[0]
                    else:
                        values = tabInfo.chck_col_vld(col_name, tab_names)
                        if values is None:
                            is_join_col = False
                            continue
                        col_name, tab_idx = values
                        tab_name = tab_alias[tab_idx]
                    if not is_join_col:
                        if opt.in_statement == "WHERE":
                            if tab_name.upper() not in indepen_col.keys():
                                indepen_col[tab_name.upper()] = [
                                    [col_name.upper(), str(is_placeholder), opt.name,
                                     pu.find_add_hint_loc(opt, sem_info)]]
                            else:
                                indepen_col[tab_name.upper()].append(
                                    [col_name.upper(), str(is_placeholder), opt.name,
                                     pu.find_add_hint_loc(opt, sem_info)])
                    else:
                        join_tab_names.append(tab_name.upper())
                        join_col_names.append(col_name.upper())
                        join_level.append(opt.level)

            # -- add join col
            if is_join_col:
                join_col.add(join_tab_names, join_col_names, join_level)
        for k in indepen_col.keys():
            indepen_col[k] = np.vstack(indepen_col[k])

        return indepen_col, join_col

    def __get_join_type(self) -> Dict[str, List[Tuple[str, str]]]:
        '''

        :return: {join_type: (tab1, tab2)}
        '''
        out = dict()
        sem_info = self.param['semantic_info'].elements
        for join_unit in sem_info.by_type['JOIN']:
            child = sorted(join_unit.edges)
            join_type = join_unit.name
            tab_left = sem_info.by_id[sem_info.find_tab(sem_info.by_id[child[0]], tab_only=True)[0]].get_name()
            tab_right = sem_info.by_id[sem_info.find_tab(sem_info.by_id[child[1]], tab_only=True)[0]].get_name()
            try:
                out[join_type].append((tab_left, tab_right))
            except:
                out[join_type] = [(tab_left, tab_right)]
        return out

    def __verbose(self, key: str, **kwargs) -> str:
        out = ''
        if key.upper() == 'DRIVE':
            return '以' + self.table_alias[kwargs['tab_name']] + '为驱动表'
        if key.upper() == 'ORDER':
            tab_value = kwargs['tab_list']
            out = 'leading('
            for v in tab_value:
                out += v + ' '
            out = out[0:-1] + ')'
            return out
        if key.upper() == 'NEST_LOOP':
            out = ' use_nl(' + kwargs['left'] + ' ' + kwargs['right'] + ')'
        if key.upper() == 'HASH_JOIN':
            out = ' use_hash(' + kwargs['left'] + ' ' + kwargs['right'] + ')'
        if key.upper() == 'NONE':
            out = '请对表'
            for tab in set(kwargs['tab_list']):
                out += self.table_alias[tab] + '或'
            out = out[0:-1] + '添加限定条件'
        if key.upper() == 'BACK_TABLE':
            out = '扫描表' + kwargs['tab_name'] + '使用索引' + kwargs['index_name'] + '不需要回表'
        if key.upper() == 'NO_RECOM':
            tab_names = kwargs['tab_list']
            recom = ''
            for tab in tab_names:
                recom += self.table_alias[tab] + '(别名' + tab + ')' + '和'
            out = '表' + recom[0:-1] + '无更好链接优化建议'
        return out

    def __dict_spliter(self, dictionary: Dict[str, Dict[str, bool]]) -> List[Dict[str, Dict[str, bool]]]:
        out = []
        for key in dictionary.keys():
            out.append({key: dictionary[key]})
        return out

    def __to_str(self, given_list: list) -> list:
        out = []
        for v in given_list:
            if type(v) == tuple:
                out.append(v[0])
            elif type(v) == dict:
                out.append(list(v.keys())[0])
            else:
                out.append(v)
        return out

    def __heuristic_bfs(self, graph: JoinSplitConatiner, start: str) \
            -> List[Tuple[str, float]]:
        '''iterative depth first search from start'''
        path = []
        q = [start]
        fist_run = True
        while q:
            v = q.pop(0)
            tv = v if type(v) == str else v[0]
            if tv not in self.__to_str(path):
                if fist_run:
                    path.append((v, 0))
                else:
                    path.append(v)
                # -- sort by distinct -- #
                edges = graph.get_join_tab_col_info(tv)
                transmit = TransmitContainer()
                tree = TabTree()
                for k in edges.keys():
                    transmit.index_receive = {k: edges[k]}
                    # -- acquire distinct key from INDEX RECOMM MODEL -- #
                    receive, _ = self.get_deges('index').recom(info=transmit)
                    value = receive.index_transmit[1]
                    tree.insert(name=k, distinct=value)
                # -- #
                q = tree.tree_traverse(increase=False, out=[], with_value=True) + q

            fist_run = False
        return path

    def __get_smallest_tab(self, tabInfo: TabelInfoAnaylsis) -> Tuple[str, float]:
        # -- row_number --#
        smallest_size = math.inf
        smallest_name = None

        for tab in self.tab_name:
            if '(' not in tab:
                try:
                    rownum = tabInfo.get_tab_numrows(self.table_alias[tab])
                except:
                    rownum = math.inf
                if rownum < smallest_size:
                    smallest_size = rownum
                    smallest_name = tab
        return smallest_name, smallest_size

    def __check_join_valid(self, join_col: JoinSplitConatiner) -> bool:
        connect = copy.copy(join_col.connect)
        for v in connect:
            if not connect[v]:
                del join_col.connect[v]
                join_col.tabs.remove(v)
        if len(join_col.connect) == 0:
            return False
        else:
            return True
        # for v in join_col.connect:
        #     if join_col.connect[v]:
        #         return True
        # return False

    # ---- row number ---- #
    def __rownumber_detector(self) -> List[Tuple[int, int]]:
        '''

        :return: int: rownumber size, rownumber level
        '''
        elements = self.param['semantic_info'].elements
        out = []
        for ele in elements.by_type['STRUCT']:
            if ele.name == 'ROWNUM':
                for idx in elements.get_cousin(ele, level=1):
                    unit = elements.by_id[idx]
                    level = unit.level
                    if unit.type == 'VALUE':
                        value = int(unit.name)
                        out.append((value, level))
        return out

    def __get_drive_joinTab_relation(self, drive: str, join_col: JoinSplitConatiner) -> str:
        '''

        :param drive:
        :param join_col:
        :return: GREAT - drive is view and its contains join_col
                 LESS  - JOIN_COL contains drive
                 NONE  - no relation
        '''
        # -- get drive type -- #
        sem_info = self.param['semantic_info'].elements
        drive_id = pu.get_unit_id(name=drive, is_alias=True, graph=sem_info)
        drive_unit = sem_info.by_id[drive_id]
        for each_j_col in join_col.connect.keys():
            j_id = pu.get_unit_id(name=each_j_col, is_alias=True, graph=sem_info)
            j_unit = sem_info.by_id[j_id]
            if j_id in drive_unit.edges:
                return "GREAT"
            if drive_id in j_unit.edges:
                return "LESS"
        return "NONE"

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[Optional[TransmitContainer], VerboseUnit]:
        # --- add connection --- #
        transmit = TransmitContainer()
        recom = VerboseUnit(self.param['semantic_info'].elements)
        self.add_to_dict('index', RecomIndex())
        self.add_to_dict('join', RecomJoin())
        Explain = self.param['plan_info']
        TableInfo = self.param['tab_info']
        lead_recom = VerboseUnit(self.param['semantic_info'].elements)
        # -- initialize tables --#
        tab_names, alias_names = self.param['semantic_info'].get_table_name(alias_on=True, view_on=True)
        for tab, alias in zip(tab_names, alias_names):
            self.table_alias[alias] = tab
        self.tab_name = alias_names
        if info is None:
            independent_col, join_col = self._col_split()
        else:
            independent_col, _join_col = info.join_receive
            join_col = JoinSplitConatiner([])
            join_col.__dict__ = _join_col
        join_type = self.__get_join_type()

        # --- build driven tab list --- #
        #################################
        #       LEFT JOIN DETECTOR      #
        #################################
        valid_driven_candidate = []
        if 'LEFT JOIN' in join_type.keys():
            been_joined_list = []
            for tab_pair in join_type['LEFT JOIN']:
                left_tab = tab_pair[0]
                right_tab = tab_pair[1]
                if right_tab not in been_joined_list:
                    been_joined_list.append(right_tab)
                if left_tab not in been_joined_list:
                    valid_driven_candidate.append(left_tab)

        if not self.__check_join_valid(join_col):
            return self.get_deges('index').recom()
        # --- sort tab by distinct value --- #
        # - use BINARY TREE - #
        tab_list = TabTree()
        tab_distinct = dict()

        ######################
        # --- 回表detect --- #
        ######################
        # indexes = Explain.contains('INDEX', 'OPERATION')
        # for index in indexes:
        #     index_name = index['OBJECT_NAME']
        #     index_table = Explain.find_tab_name(index['ID'])
        #     real_name = TableInfo.get_plan_info_get_tab_name(index_name)
        #     if real_name != index_table:
        #         recom.add_solve(self.__verbose(key='BACK_TABLE', tab_name=real_name, index_name=index_name))
        # --- 回表end --- #

        ################################
        # --- searching driven tab --- #
        ################################

        recom_table = dict()
        # --- sort tab by their distinc values --- #
        for tab in independent_col.keys():
            # --- transmit info --- #
            recom_ = VerboseUnit(self.param['semantic_info'].elements)
            transmit.index_receive = {tab: independent_col[tab]}
            receive, recom_index = self.get_deges('index').recom(info=transmit)
            for prob in recom_index.problems:
                recom_.add_problems(prob)
            recom_.add_hint_dict(recom_index.hint)

            tab_list.insert(name=tab, distinct=receive._index_transmit[1])
            tab_distinct[tab] = receive._index_transmit[1]
            recom_table[tab] = recom_

        tab_list = tab_list.tree_traverse(increase=False, out=[])
        tab_size = len(tab_list)
        drive_tab = None
        cnt = 0
        while cnt < tab_size:
            if valid_driven_candidate:
                if tab_list[cnt] in valid_driven_candidate and tab_list[cnt] in join_col.connect.keys():
                    drive_tab = tab_list.pop(cnt)
            else:
                drive_tab = tab_list.pop(0)
            if drive_tab in join_col.connect.keys():
                break
            cnt += 1
        if tab_size == 0:
            recom.add_solve('' + self.__verbose(key='NONE', tab_list=self.tab_name))
            return transmit, recom
        if tab_size > 2:
            lead_recom.add_solve(self.__verbose(key='DRIVE', tab_name=drive_tab))
        else:  # 0 < tab_size < 2
            smallest_tab, smallest_size = self.__get_smallest_tab(TableInfo)
            if smallest_size < self.param['threshold']:
                if valid_driven_candidate:
                    if smallest_size in valid_driven_candidate:
                        drive_tab = smallest_tab
                else:
                    drive_tab = smallest_tab

                if smallest_size not in tab_distinct.keys():
                    tab_distinct[smallest_tab] = 1.0
                recom.add_solve(self.__verbose(key='DRIVE', tab_name=smallest_tab))
        # --- none filter ---#
        if drive_tab is None and len(valid_driven_candidate) > 0:
            drive_tab = tab_list.pop(0)
        #####################
        # --- pop hint  --- #
        #####################

        temp_rec_verb = ''
        # -- ordered tab by drive_tab --#
        try:
            drv_tab_lst = self.__heuristic_bfs(graph=join_col, start=drive_tab)
        except:
            j_d_rltion = self.__get_drive_joinTab_relation(drive_tab, join_col)
            if j_d_rltion == 'LESS':
                # -- if it is a view -- #
                relate_col = independent_col[drive_tab][0][0]
                semantic_info = self.param['semantic_info'].elements
                relate_idx = pu.get_unit_id(name=relate_col, is_alias=False, graph=semantic_info, right_most=0)
                tab_idx = pu.get_unit_id(name=self.table_alias[drive_tab], is_alias=False, graph=semantic_info, right_most=relate_idx)
                n_drive_tab = pu.get_tab_related_view(tab_idx, semantic_info)
                drv_tab_lst = self.__heuristic_bfs(graph=join_col, start=n_drive_tab)

            else:
                drive_tab = list(join_col.connect.keys())[0]
                try:
                    drv_tab_lst = self.__heuristic_bfs(graph=join_col, start=drive_tab)
                except:
                    drv_tab_lst = [(drive_tab, 0)]
                tab_distinct[drive_tab] = 0
        # -- residual handler -- #
        rest = list(set(tab_list).difference(set([v[0] for v in drv_tab_lst])))
        if rest:
            cp_indpdnt_col = copy.copy(independent_col)
            cp_jn_col_conn = copy.copy(join_col.connect)
            for k in independent_col:
                if k not in rest:
                    del cp_indpdnt_col[k]
            for k in join_col.connect:
                if k not in rest:
                    del cp_jn_col_conn[k]
            cp_jn_col_tab = rest
            # -- sent to other recom_join -- #
            transmit.join_receive = (cp_indpdnt_col, {'tabs': cp_jn_col_tab, 'connect': cp_jn_col_conn})
            receive, recom_join = self.get_deges('join').recom(info=transmit)
            recom.add(recom_join)
            # -- sort rest --#

        lead_recom.add_hint(hint=self.__verbose(key='ORDER', tab_list=self.__to_str(drv_tab_lst)), level=0)
        if len(drv_tab_lst) == 1:
            tab_list.append(drv_tab_lst[0])
            return transmit, recom
        temp_rec_verb += '表' + self.table_alias[drv_tab_lst[0][0]] + '和表' + self.table_alias[drv_tab_lst[1][0]] + '关联使用'
        # -- check if already use nest loop -- #
        tab_join_struct = Explain.get_join_struct()

        if tab_distinct[drive_tab] > self.param['distinct_theta']:
            already_NL = False
            if 'nl' in tab_join_struct.keys():
                for tab_pair in tab_join_struct['nl']:
                    if drv_tab_lst[0][0] in tab_pair and drv_tab_lst[1][0] in tab_pair:
                        already_NL = True
                        break
            if not already_NL:
                try:
                    del recom_table[drive_tab]
                except:
                    pass
                for k in recom_table:
                    recom.add(recom_table[k])
                recom.add(lead_recom)
                recom.add_hint(hint=self.__verbose(key='NEST_LOOP', left=drv_tab_lst[0][0], right=drv_tab_lst[1][0]),
                               level=0)
                temp_rec_verb += 'nest loop'
            else:

                temp_rec_verb = self.__verbose(key='NO_RECOM', tab_list=[drv_tab_lst[0][0], drv_tab_lst[1][0]])
        else:
            # -- check is already us hash join -- #
            self.start_with_hj = True
            already_HJ = False
            if 'hj' in tab_join_struct.keys():
                for tab_pair in tab_join_struct['hj']:
                    if drv_tab_lst[0][0] in tab_pair and drv_tab_lst[1][0] in tab_pair:
                        already_HJ = True
                        break
            if not already_HJ:
                recom.add_hint(hint=self.__verbose(key='HASH_JOIN', left=drv_tab_lst[0][0], right=drv_tab_lst[1][0]),
                               level=0)
                temp_rec_verb += 'hash join'
            else:
                temp_rec_verb = self.__verbose(key='NO_RECOM', tab_list=[drv_tab_lst[0][0], drv_tab_lst[1][0]])
        parents = [drv_tab_lst[0][0], drv_tab_lst[1][0]]
        recom.add_solve(temp_rec_verb)

        # -- handler rest case -- #
        for v in drv_tab_lst[2::]:
            temp_rec_verb = ''
            cur_name = v[0]
            cur_distinc = v[1]
            tab_left = None
            # -- get left table --#
            neigbour = self.__to_str( \
                self.__dict_spliter( \
                    join_col.get_join_tab_col_info(cur_name)))
            for p in parents:
                if p in neigbour:
                    tab_left = p
                    break
            temp_rec_verb += '表' + self.table_alias[tab_left] + '和表' + self.table_alias[cur_name] + '关联使用'
            if cur_distinc > self.param['distinct_theta'] and not self.start_with_hj:
                # -- check if already use nest loop -- #
                already_NL = False
                if 'nl' in tab_join_struct.keys():
                    for tab_pair in tab_join_struct['nl']:
                        if tab_left in tab_pair and cur_name in tab_pair:
                            already_NL = True
                            break
                if not already_NL:
                    recom.add_hint(hint=self.__verbose(key='NEST_LOOP', left=tab_left, right=cur_name), level=0)
                    temp_rec_verb += 'nest loop'
                else:
                    temp_rec_verb = self.__verbose(key='NO_RECOM', tab_list=[tab_left, cur_name])
            else:
                # -- check if already use hash join -- #
                already_HJ = False
                if 'hj' in tab_join_struct.keys():
                    for tab_pair in tab_join_struct['hj']:
                        if tab_left in tab_pair and cur_name in tab_pair:
                            already_HJ = True
                            break
                if not already_HJ:
                    recom.add_hint(hint=self.__verbose(key='HASH_JOIN', left=tab_left, right=cur_name), level=0)
                    temp_rec_verb += 'hash join'
                else:
                    temp_rec_verb = self.__verbose(key='NO_RECOM', tab_list=[tab_left, cur_name])
            parents.append(cur_name)
            recom.add_solve(temp_rec_verb)
        return transmit, recom


if __name__ == "__main__":
    tab = {1: 1, 2: 2, 3: 3, 4: 4}
    tree = TabTree()
    for k in tab.keys():
        name = k
        value = tab[k]
        tree.insert(name, value)
    out = tree.tree_traverse(increase=False, out=[])
    print(out)
