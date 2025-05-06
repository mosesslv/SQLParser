# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/7/23
# LAST MODIFIED ON: 2019/12/5
# AIM: sort key words


from sqlparse.sql import Statement, Comment, Where, Identifier, IdentifierList, Parenthesis, Function, \
    Comparison, Operation, Token, TokenList, Values
from sqlparse import tokens as T
from typing import Union, List, Tuple, Optional, Set
import copy
import json


class ParseUnit:
    def __init__(self, case_sensitive_on: bool = True):
        self.case_sensitv = case_sensitive_on
        self.id = None
        self._name = None  # sql code name
        self._as_name = None  # as what name
        self._from_name = None  # from where
        self._type = None  # TAB-table , COL-column, SUB-subquery ,OPT- >,<,=.., FUNC-MAX,SUM..
        self._keyword = None
        self._in_statement = 'OTHER'

        # self._opt = None
        self._parents = set()
        self._children = set()
        self._level = 0
        self.token = None

    def dumps(self) -> str:
        '''
        dumps ParseUnit it str
        easy to save
        :return:
        '''
        # - remove set - #
        self.parent = list(self.parent)
        self.edges = list(self.edges)
        # - remove token - #
        token_bp = copy.copy(self.token)
        self.token = None
        out = json.dumps(self.__dict__)
        # - roll back - #
        self.parent = set(self.parent)
        self.edges = set(self.edges)
        # - remove token - #
        self.token = token_bp
        return out

    def load(self, v: str):
        try:
            self.__dict__ = json.loads(v)
        except:
            raise Exception('读取ParserUnit错误, ParserUnit储存格式不正确')
        self.parent = set(self.parent)
        self.edges = set(self.edges)

    @property
    def in_statement(self) -> str:
        return self._in_statement

    @property
    def level(self) -> int:
        return self._level

    @property
    def keyword(self) -> str:
        return self._keyword

    @property
    def name(self) -> str:
        return self._name

    @property
    def as_name(self) -> str:
        return self._as_name

    @property
    def from_name(self) -> str:
        return self._from_name

    @property
    def parent(self) -> set:
        return self._parents

    @property
    def type(self) -> str:
        return self._type

    @property
    def edges(self) -> set:
        return self._children

    # @property
    # def opt(self) -> str:
    #     return self._opt
    @keyword.setter
    def keyword(self, key: str):
        self._keyword = key.upper() if not self.case_sensitv else key

    @level.setter
    def level(self, level: int):
        self._level = level

    @name.setter
    def name(self, name: Optional[str]):
        if type(name) == str:
            self._name = name.upper() if not self.case_sensitv else name
        else:
            self._name = name

    @as_name.setter
    def as_name(self, as_name: str):
        self._as_name = as_name.upper() if not self.case_sensitv else as_name

    @from_name.setter
    def from_name(self, from_name: str):
        self._from_name = from_name.upper() if not self.case_sensitv else from_name

    @parent.setter
    def parent(self, parent: Set['ParseUnit']):
        self._parents = parent

    # @opt.setter
    # def opt(self, opt: str):
    #     self._opt = opt

    @type.setter
    def type(self, type: str):
        if type not in ['COL', 'TAB', 'SUB', 'VIEW', 'OPT', 'FUNC', 'JOIN', 'STRUCT', 'VALUE']:
            raise ValueError('type must be either one of following [COL,TAB,SUB,VIEW,OPT,FUNC,JOIN,STRUCT,VALUE]')
        self._type = type.upper()

    @in_statement.setter
    def in_statement(self, state: str):
        self._in_statement = state

    @edges.setter
    def edges(self, edges: Set['ParseUnit']):
        self._children = edges

    def overwrite(self, unit: 'ParseUnit'):
        if unit.name is not None:
            self._name = unit.name
        if unit.as_name is not None:
            self._as_name = unit.as_name
        if unit.from_name is not None:
            self._from_name = unit.from_name
        if unit.parent is not None:
            self._parents = unit.parent
        if unit.type is not None:
            self._type = unit.type
        if not unit.edges:
            self._children = unit.edges

    # def inherit(self, unit: 'ParseUnit', update_edges: bool = False):
    #     self._name = unit.name
    #     self._as_name = unit.as_name
    #     if unit.from_name != 'DUMMY':
    #         self._from_name = unit.from_name
    #     self._type = unit.type
    #     if update_edges:
    #         self._children.add(unit.id)

    def show(self) -> str:
        out = ''
        if self._from_name is not 'DUMMY' and not None:
            out += self._from_name + '.'
        out += self._name
        if self._as_name is not 'DUMMY' and not None:
            out += ' as ' + self._as_name
        return out

    def add_parents(self, parents: Union[List[int], Set[int]]) -> None:
        for p in parents:
            self._parents.add(p)

    def __repr__(self):
        out = '%s\n' % str(self.id)
        out += '\ttype:%s\n' % self.type
        out += '\tname:%s\n' % self.name
        out += '\tkeyword:%s\n' % self.keyword
        out += '\tstatement:%s\n' % self.in_statement
        out += '\tlevel:%s\n' % self.level
        out += '\tas_name:%s\n' % self.as_name
        out += '\tfrom:%s\n' % self.from_name
        out += '\tparents:%s\n' % str(self.parent)
        out += '\tchildren:%s\n' % str(self.edges)

        return out

    def get_name(self) -> str:
        if self.as_name is not None and self.as_name.upper() != 'DUMMY':
            return self.as_name
        else:
            return self.name


class ParseUnitList:
    def __init__(self, case_sensitive_on: bool = True) -> None:
        # -- tab col relation -- #
        self.by_type = {'COL': [],
                        'TAB': [],
                        'SUB': [],
                        'VIEW': [],
                        'OPT': [],
                        'FUNC': [],
                        'JOIN': [],
                        'STRUCT': [],
                        'VALUE': []}
        self.by_id = dict()  # G
        self._allow_sub_has_table = False
        self._last_tab_id = 0
        self.case_sensitv = case_sensitive_on

    def __insert(self, unit: ParseUnit) -> int:
        # o(mn) m<n
        id = len(self.by_id)
        unit.id = id
        self.by_type[unit.type].append(unit)
        self.by_id[unit.id] = unit
        return id

    def __update_by_type(self) -> None:
        for key in ['SUB', 'VIEW', 'TAB', 'OPT', 'FUNC', 'JOIN', 'COL', 'STRUCT', 'VALUE']:
            for unit in self.by_type[key]:
                self.by_id[unit.id] = unit

    def __update_by_id(self):
        self.by_type = {'COL': [],
                        'TAB': [],
                        'SUB': [],
                        'VIEW': [],
                        'OPT': [],
                        'FUNC': [],
                        'JOIN': [],
                        'STRUCT': [],
                        'VALUE': []}
        for id in self.by_id:
            unit = self.by_id[id]
            self.by_type[unit.type].append(unit)

    ########################################
    #           add  function              #
    ########################################

    # ----------- add by token type -----------#

    def _add_Identifier(self, tokens: Token, type: str, key: str, level: int, in_statement: str,
                        parents: List[int] = None) -> Tuple[int, Union[Token, TokenList]]:
        out = ParseUnit()
        if type == 'TAB':
            self._last_tab_id = len(self.by_id)

        if '(' in tokens.value and tokens.value != '(' and 'SELECT' in tokens.value.upper():
            out.type = 'SUB'
        else:
            out.type = type
        out.keyword = key
        out.level = level

        out.token = tokens
        out.in_statement = in_statement
        if parents is not None and parents != []:
            out.add_parents(parents)

        abnormal = None
        cnt = 0  # from name,  name   , as name
        info_container = ["DUMMY", "DUMMY", "DUMMY"]
        has_dot = False
        try:
            for t in tokens.tokens:
                t_v = t.value
                if t_v.upper() == 'AS':
                    continue
                if t_v == ' ':
                    continue
                if t_v == '.':
                    has_dot = True
                    continue
                if isinstance(t, Parenthesis) or isinstance(t, Function):
                    abnormal = t
                if 'PLACEHOLDER' in str(t.ttype).upper():
                    out.type = 'STRUCT'
                info_container[cnt] = t_v
                cnt += 1
            if not has_dot:
                info_container = ["DUMMY"] + info_container
            out.name = info_container[1]
            out.from_name = info_container[0]
            out.as_name = info_container[2]
        except:
            out.name = tokens.value
        # --- double check whether used  dot --- #

        if out.as_name is None:
            out.as_name = 'DUMMY'

        if out.type == 'TAB' and out.as_name == 'DUMMY':
            out.as_name = out.name
        # -- patch --#
        if out.name is None:
            if abnormal is not None:
                out.name = abnormal.value
            else:
                out.name = out.as_name
        # -- view handler -- #
        if out.type == 'SUB' and out.as_name != 'DUMMY':
            out.type = 'VIEW'
        if in_statement == 'WITH' and out.name != 'WITH' and isinstance(abnormal, Parenthesis):
            out.type = 'VIEW'
            temp = out.name
            out.name = out.as_name
            out.as_name = temp
        # -- add  order by or group by -- #
        keyList = ['ORDER BY', 'GROUP BY']

        if key in keyList:
            # -- find nearest opt -- #
            for id in range(len(self.by_id))[::-1]:
                acquire_id = id
                unit = self.by_id[id]
                if unit.type == 'OPT' and unit.name == key:
                    break
            out.parent.add(acquire_id)

        id = self.__insert(out)
        return id, abnormal

    def _add_Comparison(self, tokens: Token, type: str, key: str, level: int, in_statement: str,
                        parents: List[int] = None) \
            -> Optional[List[dict]]:
        if isinstance(tokens, Comparison):
            # -- get opt unit --#
            opt = None
            for t in tokens:
                if t.ttype == T.Operator.Comparison:
                    opt = t.value
            unit = ParseUnit()
            unit.name = opt
            unit.type = 'OPT'
            unit.keyword = key
            unit.level = level
            count = 0
            for t in tokens:
                if not t.is_whitespace:
                    count += 1
                if count == 2:
                    unit.token = t
                    break
            expect_id = len(self.by_id) + 1
            unit.in_statement = in_statement
            if parents is not None and parents != []:
                unit.add_parents(parents)

            # -- left unit -- #
            parents = [expect_id]
            parents_token_left = self.add(tokens=tokens.left, type=type, key=key, level=level, parents=parents,
                                          in_statement=in_statement)

            self.__insert(unit)

            parents_token_right = self.add(tokens=tokens.right, type=type, key=key, level=level, parents=parents,
                                           in_statement=in_statement)

            # unit.edges.add(left_v)
            # unit.edges.add(right_v)

            if parents_token_left and parents_token_right:
                return parents_token_left + parents_token_right
            elif parents_token_left:
                return parents_token_left
            else:
                return parents_token_right
        else:
            unit = ParseUnit()
            unit.name = tokens.value
            unit.type = 'OPT'
            unit.keyword = key
            unit.level = level
            unit.token = tokens
            current_id = len(self.by_id)
            self.__insert(unit)
            unit.in_statement = in_statement
            if parents is not None and parents != []:
                unit.add_parents(parents)
            unit.edges = {current_id - 1, current_id + 1}
            left_unit = self.by_id[current_id - 1]
            if left_unit.name == ')':
                left_id = list(left_unit.parent)[0]
                unit.edges = {left_id, current_id + 1}
            return None

    def _add_Operation(self, tokens: Operation, type: str, key: str, level: int, in_statement: str,
                       parents: List[int] = None):
        out = []
        for i, t in enumerate(tokens.tokens):
            if str(t.ttype) == 'Token.Operator':
                unit = ParseUnit()
                unit.name = t.value
                unit.type = 'OPT'
                unit.keyword = key
                unit.level = level
                unit.from_name = 'DUMMY'
                unit.as_name = 'DUMMY'
                unit.token = tokens
                unit.in_statement = in_statement
                unit.edges = {len(self.by_id) - 1, len(self.by_id) + 1}
                if parents is not None and parents != []:
                    unit.add_parents(parents)
                self.__insert(unit)
            else:
                rest = self.add(tokens=t, type=type, key=key, level=level, parents=[],
                         in_statement=in_statement)
                
                if rest:
                    out += rest
        
        if len(out):
            return out
        
        return None

    def _add_Function(self, tokens: Function, key: str, level: int, in_statement: str, parents: List[int] = None) \
            -> Tuple[int, Optional[list]]:
        unit = ParseUnit()
        unit.name = tokens.tokens[0].value
        unit.type = 'FUNC'
        unit.keyword = key
        unit.level = level
        unit.token = tokens.tokens[0]
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        id = self.__insert(unit)
        return id, tokens.tokens[1::]

    def _add_Parenthesis(self, tokens: Parenthesis, key: str, level: int, in_statement: str, parents: List[int] = None) \
            -> Tuple[int, Parenthesis]:
        unit = ParseUnit()
        unit.name = tokens.value
        unit.type = 'SUB'
        unit.keyword = key
        unit.level = level
        unit.from_name = 'DUMMY'
        unit.as_name = 'DUMMY'
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        id = self.__insert(unit)
        return id, tokens

    def add(self, tokens: Union[Token, TokenList], type: str, in_statement: str, key: str, level: int, \
            parents: List[int] = None) -> Optional[List[dict]]:
        if tokens.is_whitespace:
            return None
        if isinstance(tokens, Identifier):
            id, abnormal = self._add_Identifier(tokens=tokens, type=type, parents=parents, key=key, level=level,
                                                in_statement=in_statement)
            if abnormal is not None:
                if isinstance(abnormal, Function):
                    return self.add(tokens=abnormal, type=type, parents=[id], key=key, level=level,
                                    in_statement=in_statement)
                else:
                    return [{'parents': [id], 'tokens': [abnormal]}]
            else:
                return None
        elif isinstance(tokens, Comparison):
            abnormal = self._add_Comparison(tokens=tokens, type=type, parents=parents, key=key,
                                            in_statement=in_statement,
                                            level=level)
            return abnormal
        elif isinstance(tokens, Function):
            id, token_list = self._add_Function(tokens=tokens, parents=parents, key=key, level=level,
                                                in_statement=in_statement)
            return [{'parents': [id], 'tokens': token_list}]
        elif isinstance(tokens, Parenthesis):
            id, token = self._add_Parenthesis(tokens=tokens, parents=parents, key=key, level=level,
                                              in_statement=in_statement)
            return [{'parents': [id], 'tokens': [token]}]
        elif isinstance(tokens, Values):
            rest = self._add_value(tokens=tokens, level=level, parents=parents, in_statement=in_statement)
            return rest
        elif isinstance(tokens, Operation):
            # ---- #
            return self._add_Operation(tokens=tokens, type=type, parents=parents, key=key, in_statement=in_statement,
                                level=level)
        elif tokens.value.upper() == 'IN':
            self._add_In(tokens=tokens, in_statement=in_statement, key=key, level=level, parents=parents)
        else:
            # capture missed comparetor:
            if tokens.ttype == T.Operator.Comparison:
                self._add_Comparison(tokens=tokens, type=type, parents=parents, key=key, in_statement=in_statement,
                                     level=level)
                return None
            if tokens.ttype is not None and (
                    str(tokens.ttype[0]) in ['Literal', 'Number'] or str(tokens.ttype[-1]) == 'Placeholder'):
                type = 'VALUE'
            else:
                type = 'STRUCT'
            id, token_list = self._add_Identifier(tokens=tokens, type=type, parents=parents, key=key, level=level,
                                                  in_statement=in_statement)
            if token_list is not None:
                self.add(tokens=token_list, type=type, in_statement=in_statement, key=key, level=level, parents=[id])
        return None

    # ----------- add by keywords ----------- #

    def _add_In(self, tokens: Token, key: str, level: int, in_statement: str, parents: List[int] = None) -> None:
        # acquire id
        cur_id = len(self.by_id)
        left_id = cur_id - 1
        right_id = cur_id + 1
        # --build in Node -#
        unit = ParseUnit()
        unit.name = 'IN'
        unit.type = 'OPT'
        unit.edges = {left_id, right_id}
        unit.keyword = key
        unit.level = level
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        self.__insert(unit)
        left = self.by_id[left_id]
        left.parent.add(cur_id)

    def add_order(self, tokens: Token, key: str, level: int, in_statement: str, parents: List[int] = None) -> Optional[
        List[dict]]:
        next_id = len(self.by_id) + 1
        unit = ParseUnit()
        unit.name = tokens.value
        unit.type = 'OPT'
        unit.keyword = key
        unit.level = level
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        unit.edges.add(next_id)
        self.__insert(unit)
        return None

    def add_like(self, tokens: Token, key: str, level: int, in_statement: str, parents: List[int] = None) -> Optional[
        List[dict]]:
        pre_id = len(self.by_id) - 1
        unit = ParseUnit()
        unit.name = tokens.value
        unit.type = 'OPT'
        unit.keyword = key
        unit.level = level
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        unit.edges.add(pre_id)
        # unit.edges.add(pre_id + 2)
        self.__insert(unit)
        return None

    def add_between(self, tokens: Token, key: str, level: int, in_statement: str, parents: List[int] = None) -> \
            Optional[
                List[dict]]:
        id_pre = len(self.by_id) - 1
        unit = ParseUnit()
        unit.name = 'BETWEEN'
        unit.type = 'OPT'
        unit.keyword = key
        unit.level = level
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        unit.edges.add(id_pre)
        # unit.edges.add(id_n_left)
        # unit.edges.add(id_n_right)
        self.__insert(unit)
        return None

    def _add_value(self, tokens: Token, level: int, in_statement: str, parents: List[int] = None) -> Optional[
        List[dict]]:
        self._allow_sub_has_table = True
        col_id = list(self.by_id[len(self.by_id) - 1].parent)[0]
        # -- update statements -- #

        unit = ParseUnit()
        unit.name = 'VALUES'
        unit.type = 'OPT'
        unit.keyword = 'VALUES'
        unit.level = level
        unit.token = tokens
        unit.in_statement = 'VALUES'
        if parents is not None and parents != []:
            unit.add_parents(parents)

        unit.edges = {col_id, col_id + 2}
        this_id = self.__insert(unit)

        for i in range(col_id, len(self.by_id)):
            self.by_id[i].in_statement = 'VALUES'
            self.by_id[i].parent.add(this_id)
        self.__update_by_id()

        out = []
        for t in tokens.tokens[1::]:
            if isinstance(t, Parenthesis):
                p, tokens = self._add_Parenthesis(tokens=t, key='VALUES', level=level, parents=[this_id],
                                                  in_statement='VALUES')
                out.append({'parents': [p], 'tokens': [tokens]})
        return out

    def add_is(self, tokens: Token, key: str, level: int, in_statement: str, parents: List[int] = None) -> Optional[
        List[dict]]:
        pre_id = len(self.by_id) - 1
        unit = ParseUnit()
        unit.name = tokens.value
        unit.type = 'OPT'
        unit.keyword = key
        unit.level = level
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        unit.edges.add(pre_id)
        self.__insert(unit)
        return None

    def add_join(self, tokens: Token, key: str, level: int, in_statement: str, parents: List[int] = None) -> Optional[
        List[dict]]:
        next_id = len(self.by_id) + 1
        unit = ParseUnit()
        unit.name = tokens.value
        unit.type = 'JOIN'
        unit.keyword = key
        unit.level = level
        unit.token = tokens
        unit.in_statement = in_statement
        if parents is not None and parents != []:
            unit.add_parents(parents)
        unit.edges.add(self._last_tab_id)
        # -- union all handler -- #
        if unit.name not in ['UNION ALL', 'UNION']:
            unit.edges.add(next_id)
        self.__insert(unit)
        return None

    #########################################

    def __iter__(self):
        return iter(self.by_id.values())

    #########################################
    #          build relation function      #
    #########################################

    def build_relation(self):
        # --- build parents ---#
        symbol_idx = dict()  # {as_name/id: [index]}
        idx_edges = dict()  # {id : [index]}
        for key in self.by_id.keys():
            idx_edges[key] = set()

        check_keys = ['COL'] if not self._allow_sub_has_table else ['COL', 'SUB']

        # -- store tab location for union all -- #
        tab_union_all = []
        # -- buil tab col relation --#
        for key in ['SUB', 'VIEW', 'TAB', 'COL']:
            for unit in self.by_type[key]:
                key_i = unit.type
                # -- add edges -- #
                if len(unit.parent) > 0:
                    for p in unit.parent:
                        idx_edges[p].add(unit.id)

                if key_i in ['SUB', 'TAB', 'VIEW']:
                    if '(' not in unit.name and unit.name != 'DUAL':
                        tab_union_all.append(unit.id)

                    symbol = unit.as_name
                    if symbol not in symbol_idx.keys():
                        symbol_idx[symbol] = [unit.id]
                    else:
                        symbol_idx[symbol].append(unit.id)

                    if unit.name not in symbol_idx.keys():
                        symbol_idx[unit.name] = [unit.id]
                    else:
                        symbol_idx[unit.name].append(unit.id)
                # -- update parents --#
                if key_i in check_keys:
                    if unit.from_name != 'DUMMY':
                        try:
                            parent_indexes = symbol_idx[unit.from_name]
                        except:
                            parent_indexes = []
                            # raise SQLGrammarError('invalid column: ' + unit.name)
                        for parent in parent_indexes:
                            unit.parent.add(parent)
                            idx_edges[parent].add(unit.id)
                    else:
                        all_parents = self.add_all_parents(unit.level)
                        if len(all_parents) == 1:
                            parent = self.by_id[all_parents.pop()]
                            as_name = parent.as_name if parent.as_name != 'DUMMY' else parent.name
                            unit.from_name = as_name
                        unit.add_parents(self.add_all_parents(unit.level))
                        for p in unit.parent:
                            idx_edges[p].add(unit.id)
        self.__update_by_type()
        # --- build parents ---#
        between_switch = dict()
        # --- union all tab count pointer ---#
        tb_cnt_unn_ll = 0
        # --- like switch --- #
        like_switch = False
        for id in self.by_id.keys():
            unit = self.by_id[id]
            edges = unit.edges
            level = unit.level

            # --- build between relation -- #
            try:
                between_on = between_switch[level][0]
            except:
                between_switch[level] = (False, -1, 0)
                between_on = False

            if between_on:
                b_id = between_switch[level][1]
                count = between_switch[level][2]
                if count < 2:
                    if unit.name == unit.keyword:
                        if unit.name.upper() == "AND":
                            between_switch[level] = (True, b_id, count + 1)
                        else:
                            between_switch[level] = (False, -1, 0)
                    else:
                        between_switch[level] = (True, b_id, count)
                        unit.parent.add(b_id)
                else:
                    between_switch[level] = (False, -1, 0)
            if unit.name.upper() == 'BETWEEN':
                between_switch[level] = (True, id, 0)

            # --- build union all --- #
            if unit.name.upper() in ['UNION ALL', 'UNION']:
                unn_ll_id = unit.id
                for tb_id in tab_union_all[tb_cnt_unn_ll::]:
                    tb_cnt_unn_ll += 1
                    if tb_id > unn_ll_id:
                        unit.edges.add(tb_id)
                        break
            # --- build like relation -- #
            if like_switch > 0:
                unit.parent.add(like_switch)

            if unit.name.upper() == 'LIKE':
                like_switch = unit.id
            if unit.keyword.upper() != 'LIKE':
                like_switch = -1
            for ed in edges:
                try:
                    self.by_id[ed].parent.add(id)
                except:
                    continue

        # --- build edges --- #
        for id in self.by_id:
            parents = self.by_id[id].parent
            for pa in parents:
                self.by_id[pa].edges.add(id)
        self._allow_sub_has_table = False
        self.__update_by_id()

    def add_all_parents(self, level: int) -> Set[int]:
        parents = set()
        for key in ['TAB', 'VIEW']:
            for unit in self.by_type[key]:
                if unit.level == level:
                    parents.add(unit.id)
        return parents

    ############################################
    #              graph search                #
    ############################################
    def get_left(self, graph: ParseUnit) -> List[int]:
        parent = graph.parent
        crr_id = graph.id
        result = []
        path = []
        q = list(parent)
        while q:
            v = q.pop(0)
            if not v in path:
                path = path + [v]
                units = self.by_id[v]
                children = []
                for e in units.edges:
                    if e != crr_id:
                        children.append(e)
                if len(units.edges) == 0:
                    result.append(units.id)
                q = q + children
        return result

    def get_cousin(self, graph: ParseUnit, level: int = 1) -> List[int]:
        result = []
        path = []
        q = [(graph.id, 0, 0)]
        if level == 0:
            return [graph.id]
        shorest_dist = 100000
        while q:
            v, c_level, dist = q.pop(0)
            if c_level == 0 and v != graph.id and dist <= shorest_dist:
                result.append(v)
                shorest_dist = dist
            if not v in path:
                path = path + [v]
                units = self.by_id[v]
                # -- operation -- #
                # -- #
                children = []
                parents = []
                for child in units.edges:
                    children.append((child, c_level - 1, dist + 1))
                for parent in units.parent:
                    if c_level + 1 <= level:
                        parents.append((parent, c_level + 1, dist + 1))
                q = q + children + parents
        return result

    def find_root(self, graph: ParseUnit, col_only: bool = False, except_tab: bool = True) -> Optional[List[int]]:
        root = []
        path = []
        q = [graph.id]
        while q:
            v = q.pop(0)
            if not v in path:
                path = path + [v]
                units = self.by_id[v]
                if col_only:
                    if units.type == 'COL' and '(' not in units.name:
                        root.append(units.id)
                else:
                    if len(units.edges) == 0:
                        root.append(units.id)
                if except_tab:
                    if units.type != 'TAB':
                        q = q + list(units.edges)
                else:
                    q = q + list(units.edges)
        return root

    def find_tab(self, colum: ParseUnit, tab_only: bool = False, direction: str = 'UP') -> Optional[List[int]]:
        tabs = []
        path = []
        q = [colum.id]
        while q:
            v = q.pop(0)
            if not v in path:
                path = path + [v]
                # --- #
                units = self.by_id[v]
                if tab_only:
                    if units.type in ['TAB', 'VIEW']:
                        if units.id not in tabs:
                            tabs.append(units.id)
                else:
                    if len(units.parent) == 0 and len(units.edges) == 0:
                        if units.id not in tabs:
                            tabs.append(units.id)
                # ---#
                if direction.upper() == 'UP':
                    q = q + list(units.parent)
                elif direction.upper() == 'DOWN':
                    q = q + list(units.edges)
                else:
                    q = q + list(units.parent) + list(units.edges)
        return tabs

    ############################
    #        remove node       #
    ############################

    def remove(self, id_list: List[int]):
        all_list = []
        for id in id_list:
            trunk_id = self._get_remove_trunk(self.by_id[id])
            all_list.extend(trunk_id)

        for id in all_list:
            del self.by_id[id]

    def _get_remove_trunk(self, unit: ParseUnit) -> List[int]:
        id_list = []
        target_level = unit.level
        path = []
        q = [unit.id]
        while q:
            v = q.pop(0)
            if not v in path:
                if type(v) == int:
                    path = path + [v]
                    # --- #
                    units_ = self.by_id[v]
                    c_level = units_.level
                    if units_.type != 'TAB' and units_.type != 'SUB':
                        id_list.append(units_.id)
                        q = q + list(units_.parent) + list(units_.edges)
                    else:
                        if c_level > target_level:
                            id_list.append(units_.id)
        return id_list

    ###############################
    #        plot relation        #
    ###############################
    def tab_merge(self):
        pass

    def polt_children(self, unitl: ParseUnit):
        pass

    def show_all_relations(self):
        pass

    ################################
    #       save and load          #
    ################################

    def dumps(self) -> str:
        out_by_id = copy.copy(self.by_id)
        # - save by_id - #
        for k in out_by_id:
            out_by_id[k] = out_by_id[k].dumps()

        return json.dumps(out_by_id)

    def loads(self,value:dict):
        by_id = json.loads(value)
        self.by_id = dict()

        for id in by_id:
            # id = int(id)
            unit = ParseUnit()
            unit.load(by_id[id])
            self.by_id[int(id)] = unit
        self.__update_by_id()


    def save(self, file_name: str):
        out_by_id = copy.copy(self.by_id)
        # - save by_id - #
        for k in out_by_id:
            out_by_id[k] = out_by_id[k].dumps()

        with open(file_name, 'w') as out_file:
            json.dump(out_by_id, out_file)

    def load(self, file_address: str):
        try:
            with open(file_address, 'r') as read_file:
                by_id = json.load(read_file)
        except:
            raise Exception('读取ParserUnitList错误, ParserUnitList储存格式不正确')

        self.by_id = dict()

        for id in by_id:
            # id = int(id)
            unit = ParseUnit()
            unit.load(by_id[id])
            self.by_id[int(id)] = unit
        self.__update_by_id()
