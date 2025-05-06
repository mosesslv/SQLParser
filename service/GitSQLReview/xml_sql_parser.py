#!/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'root'

import os
import re
from service.GitSQLReview.sqlmap import MemoryParser

class XmlSqlParser():

    def __init__(self, app_path=None, file_path=None):
        """
        输入app的地址，如 /wls/source/anshuo-app
        """
        self.repo_path = app_path
        self.file_path = file_path

    def _make_filter(self, filter_regex=None):
        if filter_regex is not None:
            if isinstance(filter_regex, str):
                filter = re.compile(filter_regex)
            else:
                filter = filter_regex
        else:
            return re.compile(".*")
        return filter

    def search_files(self, filter_regex=None):
        """
        将 xml 文件解析为 sql
        :param filter_regex:
        """
        filter = self._make_filter(filter_regex)
        for root, dirs, files in os.walk(self.repo_path.strip()):
            if os.path.join(self.repo_path, "build/") in root+"/":
                continue
            if os.path.join(self.repo_path, ".git/") in root+"/":
                continue
            for filename in files:
                if filename.endswith(".class"):
                    continue
                file_full_path = os.path.join(root, filename)
                if filter.search(file_full_path):
                    yield file_full_path

    def get_sql_by_app(self):
        newer_store = MemoryParser()
        for xml_file in self.search_files(filter_regex=".xml$"):
            is_sqlmap = newer_store.add(sqlmap_path=xml_file)
            print(xml_file)

        newer_dict = {}
        for namespace_sqlmap_id in newer_store.items:
            (is_my_filename, namespace, sqlmap_id, filename) = namespace_sqlmap_id.split('*')
            sql_info = newer_store.parse(is_my_filename, filename, namespace, sqlmap_piece_id=sqlmap_id)
            newer_dict[sql_info.sql_id] = sql_info

        return newer_dict.values()



    def get_sql_by_file(self, **kwargs):
        newer_store = kwargs.get('mp_class', MemoryParser())
        is_sqlmap = newer_store.add(sqlmap_path=self.file_path)

        newer_dict = {}
        for namespace_sqlmap_id in newer_store.items:
            (is_my_filename, namespace, sqlmap_id, filename) = namespace_sqlmap_id.split('*')
            sql_info = newer_store.parse(is_my_filename, filename, namespace, sqlmap_piece_id=sqlmap_id)
            newer_dict[sql_info.sql_id] = sql_info

        return newer_dict.values()
