#!/bin/env python
# -*- coding: UTF-8 -*-
"""
临时存储并解析sqlmap
"""

import os
import sys
import imp
imp.reload(sys)
import xml.etree.ElementTree as ET
import re
import hashlib
import logging
import traceback
# reload(sys)

logger = logging.getLogger(__name__)

class SQL():
    def __init__(self, is_my_filename, namespace, sqlmap_piece_id, sql_text, sql_digest, sqlmap_files, statement_type="SELECT"):
        self.namespace = namespace
        self.sqlmap_piece_id = sqlmap_piece_id
        self.sql_text = sql_text
        self.sqlmap_files = sqlmap_files
        self.sql_digest = sql_digest
        self.sql_id = "{0}*{1}*{2}".format(is_my_filename, namespace, sqlmap_piece_id)
        self.statement_type = statement_type


class MemoryParser:
    def __init__(self, hide_dynamic=True):
        """
        使用内存作为解析暂存地
        :param sqlmap_file_path_prefix:
        :param hide_dynamic:  boolean, 是否将动态标签隐藏在注释中，以便执行计划的获得是按照最长情况获取的
        :return:
        """

        # <sql> 标签的SQL
        self._module_sql_piece_dict = {}
        # <select> <insert> <update> <delete> 标签
        self._main_sql_piece_dict = {}
        # 这个只管入库
        self.top_tag_handler_func = {
            'sql': self._sql_handler,
            'insert': self._main_handeler,
            'update': self._main_handeler,
            'delete': self._main_handeler,
            'select': self._main_handeler,
        }
        # 遇到什么tag就做什么解析
        self.parse_tag_handler_func = {
            'sql': self.parse_sql_piece,
            'insert': self.parse_sql_piece,
            'update': self.parse_sql_piece,
            'delete': self.parse_sql_piece,
            'select': self.parse_sql_piece,
            'include': self._include_parse_handler,
            'isEmpty': self._condition_parse_handler,
            'isNotEmpty': self._condition_parse_handler,
            'isEqual': self._condition_parse_handler,
            'isNotEqual': self._condition_parse_handler,
            'if': self._condition_parse_handler,
            'dynamic': self._dynamic_parse_handler,
            'iterate': self._iterate_parse_handler,
            # 'foreach': self._iterate_parse_handler,
            '__condition__': self._condition_parse_handler,
            'selectKey': self._dummy_tag,
            'where': self._where_and_set_parse_handler,
            'set': self._where_and_set_parse_handler,
            'trim': self._trim_parse_handler,
            'unknow': self._condition_parse_handler,
        }
        self.sqlid_in_file = {}
        # self.sqlmap_parameter_pattern_str = '''[#$]([\w]+)?(\[\])?(:\w+)?[#$]|[#$]\{([\w]+)(:\w+)?\}'''
        # 修复 #parameter[].value# 这种 parameter 的表示方式
        self.sqlmap_parameter_pattern_str = '''[#$]([\w]+)?(\[\])?(:\w+)?(\.\w+)?[#$]|[#$]\{([\w]+)(:\w+)?\}'''
        # self.sqlmap_parameter_pattern_str = '''[#$]\{([\w]+)?(\[\])?(:\w+)?(\.\w+)?\}'''
        self.sqlmap_parameter_pattern = re.compile(self.sqlmap_parameter_pattern_str)

        self.sqlmap_blankline_pattern = re.compile('''\n[\s|\n]*\n''')

        self.formated_parameter_pattern = re.compile('''\s''')
        self._namespaces = []
        # self.sqlmap_file_path_prefix = sqlmap_file_path_prefix

        self.top_level_tag = ("statement", "select", "insert", "delete", "update")
        self.hide_dynamic = hide_dynamic
        self.sql_source = ''

    @property
    def namespaces(self):
        """
        返回所有 namespace
        :return:
        """
        return list(set(self._namespaces))

    @property
    def items(self):
        """
        可用于生成SQL的入口
        :return:
        """
        # kv_list = []
        # for key in self._main_sql_piece_dict.keys():
        #     # kv_list.append((key.split("*")[0, 1]))
        #     kv_list.append(key.split("*")[0, 1])
        # return kv_list
        # return kv_list
        # return self._main_sql_piece_dict.keys()
        return self._main_sql_piece_dict.keys()

    def _get_sql_id(self, filename, namespace, sql_id):
        """
        根据 sqlmap namespace 和 <sql id=xxx> 生成唯一标识
        :param namespace:
        :param sql_id:
        :return: str
        """
        is_my_filename = 'NO'
        if '_my' in filename.lower():
            is_my_filename = 'YES'
        return "{0}*{1}*{2}*{3}".format(is_my_filename, namespace, sql_id, filename)

##### STORE 入口
    def add(self, root=None, sqlmap_path=None):
        """
        将需要解析的文件或 Element 放进 parser
        :param root: ElementTree
        :param sqlmap_path: str
        :return: bool
        """
        filename = None
        if sqlmap_path is not None:
            filename = os.path.basename(sqlmap_path)

        # if root is not None:
        #     if root.tag != "sqlMap":
        #         return False

        if root is not None:
            if root.tag != "sqlMap" and root.tag != 'mapper':
                return False
            elif root.tag == 'sqlMap':
                self.sql_source = 'ibatis'
            elif root.tag == 'mapper':
                self.sql_source = 'mybatis'

            namespace = root.get("namespace")
            logger.info("这是认可的 sqlMap，namesapce = {0}".format(namespace))
            self._namespaces.append(namespace)
            for top_piece in root.getchildren():
                self.add_top_piece(namespace, top_piece, filename=filename)
            return True

        if os.path.exists(sqlmap_path):
            try:
                logger.debug("解释 xml 文件 {0} 开始".format(sqlmap_path))
                tree = ET.parse(sqlmap_path)
                root = tree.getroot()
                logger.info("解释 xml 文件 {0} 成功，root.tag = {1}".format(sqlmap_path, root.tag))
                return self.add(root=root, sqlmap_path=filename)
            except Exception as ex:
                logger.info("解释 xml 文件 {0} 失败".format(sqlmap_path))
                logger.warning(ex)
                logger.warning(traceback.format_exc())
                return False

        return False

    def add_top_piece(self, namespace, e, filename=None):
        """
        sqlmap 顶层 tag 入库
        :param namespace:
        :param e:
        """
        func = self.top_tag_handler_func.get(e.tag)
        if func:
            _id = e.get("id")
            if _id:
                sqlmap_id = self._get_sql_id(filename, namespace, _id)
                if sqlmap_id not in self.sqlid_in_file:
                    self.sqlid_in_file[sqlmap_id] = set([])
                self.sqlid_in_file[sqlmap_id].add(filename)

            func(filename, namespace, e)
        else:
            # skip
            pass

    def _sql_handler(self, filename, namespace, e, **kwargs):
        """
        处理 <sql> 标签入库
        :param namespace:
        :param e:
        """
        _id = e.get("id")
        # self._module_sql_piece_dict[self._get_sql_id(filename, namespace, _id)] = e
        is_my_filename = 'NO'
        if '_my' in filename.lower():
            is_my_filename = 'YES'
        self._module_sql_piece_dict["{0}*{1}*{2}".format(is_my_filename, namespace, _id)] = e

    def _main_handeler(self, filename, namespace, e, **kwargs):
        """
        处理 <insert> <update> <delete> <select> 标签的入库
        :param namespace:
        :param e:
        """
        _id = e.get("id")
        self._main_sql_piece_dict[self._get_sql_id(filename, namespace, _id)] = e

####PARSE 入口

    def parse(self, is_my_filename, filename, namespace, element=None, sqlmap_piece_id=None):
        """
        解析 sqlmap 中的 select,insert,delete,update
        :param namespace:
        :param element:
        :param sqlmap_piece_id:
        :return: str
        :raise ValueError:
        """
        e = None
        if sqlmap_piece_id is not None:
            sql_id = self._get_sql_id(filename, namespace, sqlmap_piece_id)
            main_e = self._main_sql_piece_dict.get(sql_id)
            module_e = self._module_sql_piece_dict.get("{0}*{1}*{2}".format(is_my_filename, namespace, sqlmap_piece_id))
            if main_e is not None:
                e = main_e
            if module_e is not None:
                e = module_e
        else:
            sqlmap_piece_id = e.get("id")
            sql_id = "killkill magic not exists "
        if e is None:
            e = element
        if e is None:
            raise ValueError("element 和 sqlmap_piece_id 不能同时为None 或通过 sqlmap_piece_id 无法对应的sqlmap片段")
        if e.tag not in self.top_level_tag:
            raise ValueError("此为入口函数，仅允许 select,insert,update,delete,statement 的 element 进入")
        relate_sqlmap_files = self.sqlid_in_file.get(sql_id, [])
        sql_message = "/*@ files={2} namespace={0} id={1}  @*/".format(namespace, sqlmap_piece_id, str.join(",", relate_sqlmap_files))
        # 定位element
        sql_text_list = self.parse_sql_piece(filename, namespace, e)
        sql_text_list.insert(0, sql_message)
        #sql_text_list = self._replace_sqlmap_parameter(sql_text_list)

        sql_text = str.join("\n", sql_text_list)

        # 绑定变量替换
        sql_text = self._replace_sqlmap_parameter(sql_text)

        # 去处多余的空行
        sql_text = self.sqlmap_blankline_pattern.sub('\n', sql_text)

        formated_sql_text = self.formated_parameter_pattern.sub('', sql_text).lower()
        sql_text_digest = hashlib.md5(formated_sql_text.encode("utf-8")).hexdigest()

        # return self._replace_sqlmap_parameter(sql_text)

        return SQL(
            is_my_filename=is_my_filename,
            namespace=namespace,
            sqlmap_piece_id=sqlmap_piece_id,
            sql_text=sql_text,
            sql_digest=sql_text_digest,
            sqlmap_files=relate_sqlmap_files,
            statement_type=e.tag.upper()
        )

    def _get_attr_string(self, e):
        attr_string = ""
        for k, v in e.items():
            attr_string += " {0}='{1}'".format(k, v)
        return attr_string

    def _replace_sqlmap_parameter(self, s):
        """
        替换 sqlmap 中的绑定变量，返回值的类型取决于传入值的内容
        :param s: str| unicode | l
        :return:  str| unicode | l
        """

        #mybatls改变匹配格式
        if self.sql_source == 'mybatis':
            # self.sqlmap_parameter_pattern_str = '''[#$]\{([\w]+)?(\[\])?(:\w+)?(\.\w+)?\}'''
            self.sqlmap_parameter_pattern_str = '''[#$]\{([\w]+)?(\.\w+)?,?(\s)?(\w+\=\w+)?\}'''
            self.sqlmap_parameter_pattern = re.compile(self.sqlmap_parameter_pattern_str)

        def replace_func(m):
            if m.group(0).startswith('#'):
                # print m.groups()
                # 正则可能有些不好匹配的情况
                # 在这里做特殊处理
                parameter_name = str(m.group(1))
                if self.sql_source == 'ibatis':
                    formated_parameter_name = parameter_name.replace("[", "").\
                        replace("]", "").replace(".", "_").replace(" ", "")
                    return ":P_{0}".format(formated_parameter_name)
                else:
                    formated_parameter_name = ":P_{0}".format(parameter_name)
                    if m.group(2) is not None:
                        formated_parameter_name += m.group(2)
                    # if m.group(3) is not None:
                    #     formated_parameter_name += ',' + m.group(3)
                    return formated_parameter_name
            else:
                # 遇到 $order_by_column$ 列
                return "'{0}'".format(m.group(1))
        if isinstance(s, str):
            return self.sqlmap_parameter_pattern.sub(replace_func, s)
        elif isinstance(s, list):
            l = []
            for line in s:
                l.append(self.sqlmap_parameter_pattern.sub(replace_func, line))
            return l
        else:
            raise ValueError("参数 s 只能是 str 或 list, 不接受 {0} ".format(type(s)))

    def _add_tail(self, l, e):
        """
        将 tail string 添加到 l 尾部
        :param l:
        :param e:
        """
        if e is not None and e.tail is not None:
            tail = e.tail.rstrip()
            if tail != "":
                l.append(tail)

    def _dummy_tag(self, filename, namespace, e):
        """
        不需要处理的 element
        :param namespace:
        :param e:
        :return:
        """
        # for e_ in e.iter():
        sql_text_list = []
        self._add_tail(sql_text_list, e)
        return sql_text_list

    def _parse_tag(self, filename, namespace, e):
        """
        分析 tag 的策略模式入口
        :param namespace:
        :param e:
        :return:
        """
        func = self.parse_tag_handler_func.get(e.tag)
        if func:
            # logger.warning("_parse_tag {0} {1}".format(namespace, e.tag))
            _sql_text_list = func(filename, namespace, e)
        else:
            if e.tag.startswith("is"):
                func = self.parse_tag_handler_func.get("__condition__")
                _sql_text_list = func(filename, namespace, e)
            else:
                # _sql_text_list = ["/* ERROR: UNKNOW TAG <{0}> */\n".format(e.tag)]
                func = self.parse_tag_handler_func.get('unknow')
                _sql_text_list = func(filename, namespace, e)
        return _sql_text_list

    def _include_parse_handler(self, filename, namespace, e):
        """
        处理 include 标签
        :param namespace:
        :param e:
        :return: list
        """
        sql_text_list = []
        # for e_ in e.iter():
        for e_ in [e] + list(e):
            if e_.tag == "include":
                refid_text = e_.get("refid", "")
                if refid_text != "":
                    refid = refid_text.split(".")[-1]

                    if len(refid_text.split(".")) == 1:
                        # 没有带点的，namespace参考自上一级的 namespace
                        ref_namespace = namespace
                    else:
                        # 带有点的，所以分割后第一段是 namespace
                        ref_namespace = refid_text.split(".")[0]
                    is_my_filename = 'NO'
                    if '_my' in filename.lower():
                        is_my_filename = 'YES'
                    ref_e = self._module_sql_piece_dict.get("{0}*{1}*{2}".format(is_my_filename, ref_namespace, refid), None)
                    if ref_e is not None:
                        func = self.parse_tag_handler_func.get("sql")
                        sql_text_list = sql_text_list + func(filename, ref_namespace, ref_e)
                        continue
                        # 这里才是正常情况
                    else:
                        # ref_e is None
                        # 漏写对应的 <sql id='xxxxxx'> 这种标签
                        logger.warning("在 namespace：{0} 中查找 <sql id={1}> 的片段失败".format(ref_namespace, refid))
                        for _module_sql_piece_key in self._module_sql_piece_dict.keys():
                            logger.warning("<sql id={0}> is missing ".format(_module_sql_piece_key))
                        sql_text_list.append("/* ERROR: <include> tag refid 指向 {0} 缺失 */".format(refid))
                        continue
                else:
                    sql_text_list.append("/* ERROR: <include> tag 缺失 refid */")
                    continue
            else:
                sql_text_list.append(self._parse_tag(filename, namespace, e_))
                continue
            # logger.warning("_sql_text_list {0} {1}".format(namespace, e_.tag))
            # _sql_text_list = self._parse_tag(namespace, e_)
            # sql_text_list = sql_text_list + _sql_text_list
        self._add_tail(sql_text_list, e)
        return sql_text_list

    def _dynamic_parse_handler(self, filename, namespace, e):
        """
        处理 dynamic 标签
        :param namespace:
        :param e:
        :return: list
        """
        sql_text_list = []

        dyn_start_text = (e.text or "").strip()
        dyn_start_prepend = e.get("prepend", "")

        attr_string = self._get_attr_string(e)

        # 添加 dynamic 标签的SQL语句
        sql_text_list.append("/*{0} {1}*/\n{2}\n{3}".format(
            e.tag, attr_string, dyn_start_prepend, dyn_start_text))

        for e_ in list(e):

            _sql_text_list = self._parse_tag(filename, namespace, e_)
            sql_text_list = sql_text_list + _sql_text_list

            # self._add_tail(sql_text_list, e_)

        self._add_tail(sql_text_list, e)

        return sql_text_list

    def _condition_parse_handler(self, filename, namespace, e):
        """
        处理各种条件判断的标签
        :param namespace:
        :param e:
        :return: list
        """
        sql_text_list = []
        # for e_ in e.iter():
        for e_ in [e] + list(e):

            if e_ is e:
                text = (e_.text or "")
                prepend = e_.get("prepend", "")
                attr_string = self._get_attr_string(e_)
                # if len(text.strip()) > 0:
                line = "/*{0} {1}*/  {2}   {3}".format(e_.tag, attr_string, prepend, text.rstrip())
                if self.hide_dynamic:
                    line = self._comment(line)

                sql_text_list.append(line)
            else:
                _sql_text_list = self._parse_tag(filename, namespace, e_)

                if self.hide_dynamic:
                    _sql_text_list = self._comment(_sql_text_list)

                sql_text_list = sql_text_list + _sql_text_list

        self._add_tail(sql_text_list, e)

        return sql_text_list

    def _iterate_parse_handler(self, filename, namespace, e):
        """
        处理 iterate 标签
        :param namespace:
        :param e:
        :return: list
        """
        sql_text_list = []
        # for e_ in e.iter():
        for e_ in [e] + list(e):
            prepend = e_.get("prepend", "")
            open_attr = e_.get("open", "")
            close_attr = e_.get("close", "")
            conjunction_attr = e_.get("conjunction", "")

            attr_string = self._get_attr_string(e)

            text = e_.text

            joined_string = str.join(" " + conjunction_attr + " ", [text, text, text])
            if text is not None:
                if len(text) > 0:
                    sql_text_list.append("/*{0} {5}*/ \n{1} {2} {3} {4}".format(
                        e_.tag, prepend,
                        open_attr, joined_string, close_attr, attr_string))
                continue
            # 分析非文本节点
            _sql_text_list = self._parse_tag(filename, namespace, e_.tag)
            sql_text_list = sql_text_list + _sql_text_list

        self._add_tail(sql_text_list, e)
        return sql_text_list

    def _where_and_set_parse_handler(self, filename, namespace, e):
        """
        处理 where 和 set 标签
        :param namespace:
        :param e:
        :return: list
        """
        sql_text_list = []
        # for e_ in e.iter():
        for e_ in [e] + list(e):

            if e_ is e:
                text = (e_.text or "")

                # if len(text.strip()) > 0:
                line = "{0} {1}".format(e_.tag, text.rstrip())
                sql_text_list.append(line)
            else:
                _sql_text_list = self._parse_tag(filename, namespace, e_)
                sql_text_list = sql_text_list + _sql_text_list

        self._add_tail(sql_text_list, e)

        return sql_text_list

    def _trim_parse_handler(self, filename, namespace, e):
        """
        处理 trim 标签
        :param namespace:
        :param e:
        :return: list
        """
        sql_text_list = []
        # for e_ in e.iter():
        prefix_string = ''
        suffix_string = ''
        for e_ in [e] + list(e):

            if e_ is e:
                text = (e_.text or "")
                prefix_string = e_.get("prefix", "")
                suffix_string = e_.get("suffix", "")
                # if len(text.strip()) > 0:
                # line = "{0} {1} {2}".format(prefix_string, text.rstrip(), suffix_string)
                # sql_text_list.append(line)
            else:
                _sql_text_list = self._parse_tag(filename, namespace, e_)
                sql_text_list = sql_text_list + _sql_text_list

        sql_text_list.insert(0, prefix_string)
        sql_text_list.append(suffix_string)
        self._add_tail(sql_text_list, e)

        return sql_text_list

    def parse_sql_piece(self, filename, namespace, e):
        """
        解析 sqlmap 中的sql片段
        这是递归深度优先遍历的入口
        :param namespace:
        :param e:
        """
        sql_text_list = []

        _allow_level_tag = [x for x in self.top_level_tag]
        _allow_level_tag.append("sql")

        # for e_ in e.iter():
        for e_ in [e] + list(e):
            if e_.tag in _allow_level_tag:
                # 最顶层的入口，针对 5 大标签
                text = e_.text
                if text:
                    sql_text_list.append(text)
                    continue
            # if e_ != e:
            #     _sql_text_list = self._parse_tag(namespace, e_)
            # else:
            #     _sql_text_list = []
            _sql_text_list = self._parse_tag(filename, namespace, e_)
            sql_text_list = sql_text_list + _sql_text_list
        if e is not None and e.tail is not None:
            self._add_tail(sql_text_list, e)
        return sql_text_list

    def _comment(self, statement_line_or_statements):
        if isinstance(statement_line_or_statements, list) or isinstance(statement_line_or_statements, tuple):
            statements = statement_line_or_statements
            commented_lines = []
            for line in statements:
                commented_lines.append(self._comment(line))
            return commented_lines
        else:
            statement_line = statement_line_or_statements
            statement_line = "-- {0}".format(statement_line)
            return statement_line.replace("\n", "\n-- ")

if __name__ == "__main__":
    mp = MemoryParser(hide_dynamic=True)
    is_sqlmap = mp.add(sqlmap_path=r"/tmp/MipAlertDao.xml")
    if is_sqlmap:
        print("this is sqlmap")
    else:
        print("this is not sqlmap")
        exit()
    sql_info = mp.parse(namespace="com.lufax.mip.dao.MipAlertDao", sqlmap_piece_id="getCountByAppAndCreatedAt")
    print(sql_info.sql_text)

