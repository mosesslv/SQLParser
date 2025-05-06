# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/7/23
# LAST MODIFIED ON: 2019/12/5
# AIM: sql parser

import sqlparse
from sqlparse.sql import Where, IdentifierList, Identifier, TokenList, Token, Parenthesis, Comment, Case, Operation, \
    Function, Values
from service.sql_parser_graph.KeywrodDict import Keywordstack
from service.sql_parser_graph.units import ParseUnitList, ParseUnit
from typing import Union, List, Optional, Tuple, Dict
import re

# --- Basic keyword ---- #
TAB_KEYWORD = ['FROM', 'UPDATE', 'EXISTS', 'INTO', 'MERGE INTO']
COL_KEYWORD = ['INSERT', 'SELECT', 'WHERE', 'CASE', 'ON', 'AND', 'HAVING', 'OR', 'SET', 'BY', 'PRIOR',
               'DISTINCT', 'START WITH', 'GROUP BY', 'WHEN']
# --- Extend keyword --- #
STATEMENT_KEYWORD = ['INSERT', 'DELETE', 'UPDATE', 'SELECT']
IN_KEYWORD = ['IN']
ORDER_KEYWORD = ['ORDER BY', 'GROUP BY']
LIKE_KEYWORD = ['LIKE']
VALUE_KEYWORD = ['VALUES']
BETWEEN_KEYWORD = ['BETWEEN']
JOIN_KEYWORD = ['LEFT JOIN', 'INNER JOIN', 'OUTER JOIN', 'RIGHT JOIN', 'UNION ALL', 'UNION']
IS_KEYWORD = ['IS']
STATE_KEY = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'SET', 'ORDER BY', 'GROUP BY', 'WITH', 'MERGE INTO']
WHERE_EXCEPT = ['ROWNUMBER']
# --- special keyword --- #

START_WITH_CONNECT_BY = {'START': 0, 'WITH': 1, 'CONNECT': 2, 'BY': 3}
MERGE_INTO = {'MERGE': 0, 'INTO': 1}

# --- special case --- #
FROM_FUNC = ['PARTITION']


class SQLParser:

    def __init__(self, sql: str, case_sensitive_on: bool = True, **kwargs) -> None:
        self.case_sensitv = case_sensitive_on
        self.exception_list = kwargs['exception'] if 'exception' in kwargs.keys() else []
        self._has_where = False
        self.origin_sql = sql
        self.run_parser = kwargs['run_parser'] if 'run_parser' in kwargs.keys() else True
        sql = self.sql_interpreter(sql)
        tokens = sqlparse.parse(sql)
        origin_tokens = sqlparse.parse(sql)
        if len(tokens) > 1:
            raise Exception("sql is not single")
        else:
            self._stmt = tokens[0]
            self.origin_tokens = origin_tokens[0]
        if self.run_parser:
            self.get_elements()
        self._sql_text = sql

    def get_elements(self):
        self.elements = ParseUnitList()
        self.lu_parse(self._stmt)
        return self.elements

    @property
    def tokens(self) -> Union[TokenList, Token]:
        return self._stmt

    @property
    def has_where(self):
        """
        SQL语句是否有条件
        :return: boolean
        """
        has_where = False
        for token in self.token_walk(self._stmt.tokens, topdown=True, yield_current_token=True):
            if isinstance(token, Where):
                has_where = True
                break

        return has_where

    @property
    def has_hint(self):
        """
        SQL语句是否有HINT
        :return: boolean
        """
        has_hint = False
        for token in self.token_walk(self._stmt.tokens, topdown=True, yield_current_token=False):
            if str(token.ttype) == "Token.Comment.Multiline.Hint":
                has_hint = True
                break

        return has_hint

    @property
    def has_order(self):
        """
        SQL语句是否有 order by
        :return: boolean
        """
        has_order = False
        for token in self.token_walk(self._stmt.tokens, topdown=True, yield_current_token=False):
            if token.is_keyword and token.value.upper() == "ORDER":
                has_order = True
                break
        return has_order

    @property
    def order_has_calculate(self):
        """
        MYSQL SQL中的ORDER BY 是否有 + - 运算, 影响到执行计划的生成
        :return: boolean
        """
        if not self.has_order:
            return False

        has_order_cal = False

        traversal_tokens = [self._stmt.tokens]
        # 分析token, 将所有的子句纳入遍历范围
        for token in self.token_walk(self._stmt.tokens):
            if isinstance(token, Parenthesis):
                traversal_tokens.append(token)

        for tk in traversal_tokens:
            try:
                token_first = self._token_first(tk)
            except:
                continue

            if token_first is not None:
                order_flag = False
                by_flag = False

                order_by_flag = False
                change_flag = False

                order_by_content = ""

                for token in self.token_walk(tk, topdown=False, yield_current_token=False):
                    if token.is_whitespace or isinstance(token, Comment):
                        # 空格 回车 等
                        continue

                    if token.ttype is not None and token.ttype[0] in ["Punctuation"]:
                        # 标点符号等
                        continue

                    if token.is_keyword and token.value.upper() == "ORDER":
                        order_flag = True

                    if token.is_keyword and order_flag and token.value.upper() == "BY":
                        order_by_flag = True

                    if order_by_flag and (isinstance(token, Operation) or isinstance(token, Identifier)
                                          or isinstance(token, IdentifierList)):
                        change_flag = True

                        if isinstance(token, Identifier):
                            order_by_content = "{0}{1}".format(order_by_content, token.value)
                        if isinstance(token, Operation):
                            order_by_content = "{0}{1}".format(order_by_content, token.value)
                        if isinstance(token, IdentifierList):
                            order_by_content = "{0}{1}".format(order_by_content, token.value)
                    else:
                        if change_flag:
                            order_by_flag = False

                # end for

                if len(order_by_content) <= 0:
                    continue

                # 包含多个排序条件
                for c in order_by_content.split(","):
                    plus_pos = c.find("+")
                    subtract_pos = c.find("-")

                    if plus_pos < 0 and subtract_pos < 0:
                        has_order_cal = False
                        break
                    else:
                        has_order_cal = True

                if not has_order_cal:
                    break
        # end for
        return has_order_cal

    def sql_statement(self):
        """
        sql statement property
        :return:
        """
        return self._stmt.get_type().upper().strip()

    def _is_function_contain_keyword(self, function: Token, keyword_list: List[str]) -> Optional[str]:
        if isinstance(function, Function):
            if function.tokens[0].value.upper() in keyword_list:
                return function.tokens[0].value.upper()
        return None

    def is_statement(self, token: Token):
        first_word = re.findall('[a-zA-Z]+', token.value.strip())
        if first_word:
            if first_word[0].upper() in STATEMENT_KEYWORD:
                return True
        return False

    def _modify_tokens(self, step: int, id: int, statement: TokenList, keywords: Dict[str, int]) -> Tuple[int, Token]:
        '''

        :param id:
        :param statement:
        :param keywords:
        :return: id, modified_token
        '''
        t = statement[id]
        if t.value.upper() in keywords and statement.token_next(id)[1].value.upper() in keywords:
            next_idx, next_token = statement.token_next(id)
            next_token = next_token.value.upper()
            curr_token = t.value.upper()
            if keywords[next_token] - keywords[curr_token] == 1:
                t.is_keyword = True
                t.normalized = curr_token.upper() + ' ' + next_token.upper()
                t.value = t.value + ' ' + next_token
                step = next_idx - id + 1
                return step, t
        else:
            return step, t

    def lu_parse(self, statement: TokenList, level: int = 0, t_idx: int = 3, **kwargs) -> None:
        type_name = ['COL', 'TAB', 'SUB', 'IN', 'STRUCT']
        parents = kwargs['parents'] if 'parents' in kwargs else []
        build_relation = kwargs['build_relation'] if 'build_relation' in kwargs else True
        keyword_capture = kwargs['keyword'] if 'keyword' in kwargs else ''
        unit_statement = kwargs['in_statement'] if 'in_statement' in kwargs else 'OTHER'
        order_by_loop = False
        if not isinstance(statement, TokenList):
            statement = [statement]
        state_length = len(statement.tokens)
        i = 0

        while i < state_length:
            # ------------- #
            #  label tokens #
            # ------------- #
            t = statement[i]
            v = self._is_function_contain_keyword(t, COL_KEYWORD + TAB_KEYWORD + IN_KEYWORD + ORDER_KEYWORD)
            step, _ = self._modify_tokens(step=1, id=i, statement=statement, keywords=START_WITH_CONNECT_BY)
            step, t = self._modify_tokens(step=step, id=i, statement=statement, keywords=MERGE_INTO)
            if t.value in self.exception_list:
                t.is_keyword = False
                t.ttype = sqlparse.tokens.Name
                t = Identifier([t])

            if (t.is_keyword or v is not None):
                keyword_capture = t.normalized
                if v is not None:
                    keyword_capture = v

                if keyword_capture in COL_KEYWORD:
                    t_idx = 0
                elif keyword_capture in TAB_KEYWORD:
                    t_idx = 1
                elif keyword_capture in JOIN_KEYWORD or 'JOIN' in keyword_capture:
                    t_idx = 1
                    self.elements.add_join(tokens=t, key=keyword_capture, parents=parents, level=level,
                                           in_statement=unit_statement)
                    i += step
                    continue
                elif keyword_capture in IN_KEYWORD:
                    t_idx = 3
                elif keyword_capture in ORDER_KEYWORD:
                    t_idx = 0
                    self.elements.add_order(tokens=t, key=keyword_capture, parents=parents, level=level,
                                            in_statement=unit_statement)
                    order_by_loop = True
                    i += step
                    continue
                elif keyword_capture in BETWEEN_KEYWORD:
                    t_idx = 0
                    self.elements.add_between(tokens=t, key=keyword_capture, parents=parents, level=level,
                                              in_statement=unit_statement)
                    i += step
                    continue
                elif keyword_capture in LIKE_KEYWORD:
                    t_idx = 0
                    self.elements.add_like(tokens=t, key=keyword_capture, parents=parents, level=level,
                                           in_statement=unit_statement)
                    i += step
                    continue
                elif keyword_capture in IS_KEYWORD:
                    t_idx = 0
                    self.elements.add_is(tokens=t, key=keyword_capture, parents=parents, level=level,
                                         in_statement=unit_statement)
                    i += step
                    continue
                elif keyword_capture in FROM_FUNC:
                    t_idx = 4
            elif t.value == ',':
                if unit_statement in TAB_KEYWORD:
                    t_idx = 1
                elif unit_statement in COL_KEYWORD:
                    t_idx = 0

            if keyword_capture in STATE_KEY:
                unit_statement = keyword_capture

            # ------------------ #
            # add token to Graph #
            # ------------------ #
            if not t.is_whitespace and not isinstance(t, Comment) and 'Comment' not in str(t.ttype):
                if order_by_loop:
                    order_by_loop = False
                    if ' ' in t.value:
                        self.lu_parse(t, t_idx=t_idx, build_relation=True, parents=parents, level=level,
                                      keyword=keyword_capture, in_statement=unit_statement)
                        i += step
                        continue
                if isinstance(t, Case):
                    self.lu_parse(t, t_idx=t_idx, build_relation=True, parents=parents, level=level,
                                  keyword=keyword_capture, in_statement=unit_statement)
                    i += step
                    continue
                if isinstance(t, Where):
                    count_valid = 0
                    for tt in self.token_walk(t, yield_current_token=False):
                        if str(tt.ttype) == 'Token.Name' or tt.value.upper() in WHERE_EXCEPT:
                            count_valid += 1
                    if count_valid > 0:
                        self._has_where = True
                    self.lu_parse(t, t_idx=t_idx, build_relation=True, parents=parents, level=level,
                                  in_statement=unit_statement)
                    i += step
                    continue
                if not isinstance(t, IdentifierList):
                    # print(' value :', t)
                    rest = self.elements.add(t, type_name[t_idx], parents=parents, key=keyword_capture,
                                             in_statement=unit_statement, level=level)

                    if t_idx == 3:
                        t_idx = 0
                    if keyword_capture == 'INTO' and \
                            self.elements.by_id[len(self.elements.by_id) - 1].type == 'TAB':
                        t_idx = 0
                    if isinstance(t, Values):
                        t_idx = 4

                    # print('after :', token_id,'\n')
                    # -- subquery -- #
                    if rest is not None:
                        for rest_v in rest:
                            sub_parents = rest_v['parents']
                            rest_tokens = rest_v['tokens']
                            # if build_relation:
                            #     self.elements.build_relation()
                            for rest_t in rest_tokens:
                                # print('sub_parent: ',token_id-1, 'sub_value: ', rest, '\n')
                                if isinstance(rest_t, Parenthesis):
                                    level_ = level + 1 if self.is_statement(rest_t) else level
                                    self.lu_parse(statement=rest_t, parents=sub_parents, t_idx=t_idx,
                                                  build_relation=True, level=level_,
                                                  in_statement=unit_statement)

                    # -- fix on path -- #
                    if keyword_capture == 'ON' and t.value.upper() != 'ON':
                        t_idx = 1
                else:
                    self.lu_parse(t, t_idx=t_idx, build_relation=False, parents=parents, keyword=keyword_capture,
                                  in_statement=unit_statement, level=level)
            i += step
        if self.is_statement(statement) and build_relation:
            self.elements.build_relation()

    def display_elements(self, in_detial=False, view_on=False) -> str:
        out = ''
        for v in self.elements:
            if not in_detial:
                if v.type in ['COL', 'SUB', 'TAB', 'FUNC', 'OPT']:
                    # print(v)
                    out += v.__str__() + '\n'
            else:
                # print(v)
                out += v.__str__() + '\n'
        return out

    # ---------------------- #
    #  table reconstruction  #
    # ---------------------- #
    def get_table_name(self, alias_on=False, view_on=False) -> Union[Tuple[List[str], List[str]], List[str]]:
        tab_names = []
        as_names = []
        TAB_LIST = self.elements.by_type['TAB']
        VIEW_LIST = self.elements.by_type['VIEW'] if view_on else []
        for unit in TAB_LIST + VIEW_LIST:
            if unit.type == 'TAB' and '(' not in unit.name and 'DUAL' not in unit.name:
                tab_names.append(re.sub(r'[\"\']', "", unit.name))
                if unit.as_name != 'DUMMY':
                    as_names.append(re.sub(r'[\"\']', "", unit.as_name))
                else:
                    as_names.append(re.sub(r'[\"\']', "", unit.name))
            if unit.type == 'VIEW':
                tab_names.append(re.sub(r'[\"\']', "", unit.as_name))
                as_names.append(re.sub(r'[\"\']', "", unit.as_name))

        if alias_on:
            return tab_names, as_names
        else:
            return tab_names

    def get_view_name(self, view_id=False) -> Union[List[str], Dict[str, int]]:
        if view_id:
            view_names = dict()
        else:
            view_names = []

        for unit in self.elements.by_type['VIEW']:
            name = unit.as_name
            id = unit.id
            if view_id:
                view_names[name] = id
            else:
                view_names.append(name)
        return view_names

    def token_walk(self, token, topdown=True, yield_current_token=True):
        """
        walk all token
        :param token:
        :param topdown:
        :return:
        """

        def __has_next_token(t):
            return hasattr(t, "tokens")

        if yield_current_token:
            yield token

        for idx, tk in enumerate(token):
            if __has_next_token(tk) and topdown:
                for x in self.token_walk(tk, topdown, yield_current_token):
                    yield x
            else:
                yield tk

    def _is_Hanzi(self, char: str) -> bool:
        if not '\u4e00' <= char <= '\u9fa5':
            return False
        return True

    def _contains_Hanzi(self, word: str) -> bool:
        for char in word:
            if self._is_Hanzi(char):
                return True
        return False

    def sql_interpreter(self, sql: str) -> str:
        # -- 1. remove comments --#
        sql = sql.strip()
        ends = len(sql)
        for i in range(len(sql))[::-1]:
            if sql[i] not in ['\n', '\t', ' ', ';']:
                ends = i + 1
                break
        sql = sql[0:ends]
        sql = sql.replace('(+)', '')
        sql = re.sub(re.compile("/\*.*?\*/", re.DOTALL), "", sql)
        sql = re.sub(r'(?m)^ *--.*\n?', '', sql)
        tokens = sqlparse.parse(sql)
        opt_filter = ['>', '<', '=']
        # -- 2. split sql --#
        sql = ''
        pre_keyword = None
        wirte = False
        for t in self.token_walk(tokens, True, False):
            if t.value == '(':
                if wirte:
                    sql += ','
                    wirte = False
                sql += t.value.upper() if not self.case_sensitv else t.value

            else:
                if t.value in opt_filter and sql[-2] in opt_filter:
                    sql = sql[0:-1] + t.value
                else:
                    sql += t.value.upper() if not self.case_sensitv else t.value
            if t.is_keyword:
                if t.value.upper() == 'INTO' and pre_keyword == 'INSERT':
                    wirte = True
                pre_keyword = t.value.upper()
            if sql[-1] == '\n':
                sql = sql[0:-1] + ' ' + '\n'
            if len(sql.split(' ')) >= 2:
                sql_words = sql.split(' ')
                if self._contains_Hanzi(sql_words[-2]) and "'" not in sql_words[-2] and '"' not in sql_words[-2]:
                    sql_words = sql_words[0:-2] + ['\"' + sql_words[-2] + '\"'] + [sql_words[-1]]
                    sql = ' '.join(v for v in sql_words)

        sql_words = sql.split(' ')
        if self._contains_Hanzi(sql_words[-1]) and "'" not in sql_words[-1] and '"' not in sql_words[-1]:
            sql_words = sql_words[0:-1] + ['\"' + sql_words[-1] + '\"']
            sql = ' '.join(v for v in sql_words)
        return sql

    def reconstruct(self) -> str:
        n_line_elem = ['SELECT', 'FROM', 'WHERE', 'ORDER BY']
        without_space = [',', '(', ')']
        keys_list = sorted(self.elements.by_id.keys())
        out = ''
        buffer = Keywordstack()
        for i in keys_list:
            unit = self.elements.by_id[i]
            if unit.type != 'SUB':
                level = unit.level
                value = self.elements.by_id[i].token.value
                if unit.type == 'STRUCT' and value not in [')', '(']:
                    buffer.insert(unit)
                else:
                    if not buffer.is_empty():
                        unit_ = buffer.pop()
                        buffer.reset()
                        level_ = unit.level
                        value_ = unit_.name
                        if value_ in n_line_elem:
                            value_ = '\n' + '\t' * level_ + value_
                        if value_ in without_space:
                            out = out[0:-1] if out[-1] == ' ' else out
                            out += value_
                        else:
                            out += value_ + ' '

                    if value in n_line_elem:
                        value = '\n' + '\t' * level + value
                    if value in without_space:
                        out = out[0:-1] if out[-1] == ' ' else out
                        out += value
                    else:
                        out += value + ' '
        return out

    # ----------------------- #
    #     table similarity    #
    # ----------------------- #
    def get_topology(self, detial: bool = False) -> List[str]:
        topology = []
        # allows type: ['COL', 'TAB', 'SUB', 'VIEW', 'OPT', 'FUNC', 'JOIN', 'STRUCT', 'VALUE']
        for id in self.elements.by_id:
            unit = self.elements.by_id[id]
            u_type = unit.type
            if u_type in ['COL', 'TAB']:
                topology.append(u_type)
            elif u_type in ['SUB', 'VIEW']:
                continue
            elif u_type == 'OPT':
                pass
        return topology


    def table_name_grep(self,sql:str)->list:
        idx = 0
        tk_list = sql.split(' ')
        out = []
        for token in tk_list:
            if 'FROM' == token.upper():
                idx += 1
                out.append(tk_list[idx])
            else:
                idx+=1
        return out


    def add_schema_name(self, schema_name: str) -> str:
        '''
        替换schema_name 和 sequence_name  (通过NEXTVAL抓到)
        '''
        sequence_keys = []
        for col in self.elements.by_type['COL']:
            # case a.NEXTVAL
            if col.name.upper() == 'NEXTVAL':
                sequence_keys.append(col.from_name)
                
        tab_names = self.get_table_name(alias_on=False, view_on=False)

        out = ''
        for v in self.origin_tokens.flatten():
            if v.value.replace('"', '') in tab_names or v.value in sequence_keys:
                out += schema_name + '.' + v.value
            else:
                out += v.value
        return out
    # def add_schema_name(self,schema_name:str) ->str :
    #     '''
    #     #     在以上方法失效的情况下，添加schema_name
    #     #
    #     '''
    #     idx = 0
    #     tk_list = list(self._sql_text())
    #     out = []
    #     for token in tk_list:
    #         if 'FROM' == token.upper():
    #             tk_list[idx+1] = schema_name +'.'+ tk_list[idx+1]
    #             out.append(token)
    #             idx +=1
    #         else:
    #             out.append(token)
    #             idx +=1
    #     return ' '.join(out)

    def _to_sql(self, list_range: List[int], header: str = '') -> str:
        # -- 该方法不成熟　-- #
        out = ''
        current_level = self.elements.by_id[list_range[0]].level
        for id in range(list_range[0], list_range[-1] + 1):
            unit = self.elements.by_id[id]
            try:
                out += header if out[-1] == '\n' else ''
            except:
                pass
            if unit.level != current_level:
                continue
            child = sorted(list(unit.edges))
            if unit.in_statement in ['SELECT', 'INSERT', 'UPDATE']:
                out += unit.name + '\n'
            elif unit.name in ['WHERE', 'ORDER BY', 'GROUP BY', 'AND', 'OR']:
                out += '\n' + header + unit.name + ' '
            elif unit.type == 'VIEW':
                out += '\n' + header + self._to_sql(child, header + '\t') + ' as ' + unit.as_name
            elif unit.type == 'SUB':
                child_level = self.elements.by_id[child[0]].level
                if child_level < current_level:
                    out += '\n' + header + self._to_sql(child, header + '\t')
            elif unit.type not in ['SUB', 'VIEW']:
                name = unit.name
                if unit.from_name is not None and unit.from_name != 'DUMMY' and unit.from_name != unit.from_name:
                    name = unit.from_name + '.' + name
                if unit.as_name is not None and unit.as_name != 'DUMMY' and unit.as_name != unit.name:
                    name = name + ' as ' + unit.as_name
                out += name + ' '
        return out

    def to_SQL(self) -> str:
        id_list = sorted(list(self.elements.by_id.keys()))
        return self._to_sql(id_list)
# if __name__ == "__main__":
#     with open("Complex_SQL", "r") as f:
#         sql_text = f.read()
   #
    # sql_text = """select * from tab1 where id > 10order by id + 1 desc"""
    # sp = SQLParser(sql_text, exception=[], case_sensitive_on=False, run_parser=True)
    # print(sp.add_schema_name('ttt'))
   #  # sp.sql_statement()
   #  with open('Out_graph.txt', 'w') as f:
   #      f.write(sp.display_elements(in_detial=True))

    # sp.elements.save('test.txt')
    # sp.elements.load('test.txt')

    # v = sp.elements.dumps()
    # print(v)
    # sp.elements.loads(v)

    # print(sp.add_schema_name('hehehe'))
    #
    # print(sp.sql_interpreter(sql_text))
    # print(sp.has_where)
    # print(sp.origin_sql)
    # print('table_names :', sp.get_table_name())
    # # print(sp.elements.by_id[28].get_name())
    #
    # for ele in sp.elements.by_type['STRUCT']:
    #     if ele.name == 'ROWNUM':
    #         pass
