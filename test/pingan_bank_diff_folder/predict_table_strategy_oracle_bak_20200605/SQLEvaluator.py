# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/1/21 下午1:35
# LAST MODIFIED ON:
# AIM: 比较改前与改后的差别
# 将信息打包成package

import pandas as pd
import configparser
import sys
from typing import List, Tuple
import service.sql_parser_graph.SQLParser as parser
from service.ai_sr_models import AISrOracleSharepoolSrc
from service.predict_table_strategy_oracle.Utility.Package import EvaluatorPackage, BuilderPackage
from service.oracle_opt.oracle_common import OracleCommon
from service.predict_table_strategy_oracle.Utility.Utility_Data_get_from_sql import GetSQL
from service.predict_table_strategy_oracle.Utility.DataHandle import OracleSharePoolHandle
from service.predict_table_strategy_oracle import Utility
import numpy as np
from termcolor import colored
import json
import os


class Eval:

    def __init__(self, handle: OracleSharePoolHandle, package: BuilderPackage):
        self.handle = handle
        self.package = package
        self.oral_lib = OracleCommon(self.handle)
        self.instance_url = self.handle.get_instance_url()
        # -- read configure -- #
        self.connection = configparser.ConfigParser()
        curfile = __file__
        curdir = os.path.split(curfile)[0]

        out = self.connection.read('{0}/../../config.ini'.format(curdir))
        if len(out) == 0:
            raise Exception('链接数据库配置文件config.ini读取错误（config.init　不存在？）')

    # ---- meta utility function --- #
    def remove_instance_name(self, table_name: str):
        try:
            instance, schema, tab = table_name.split('.')
        except:
            return table_name
        return schema + '.' + tab

    def _add_virtual_index(self, index_name: str, table_name: str, col_list: List[str]) -> None:
        '''
        构建虚拟索引，没有任何输出
        :param index_name:
        :param table_name:
        :param col_list:
        :return:
        '''
        buffer = ''
        for col in col_list:
            buffer += col + ','
        sql = f'create index {self.remove_instance_name(index_name):s} on {self.remove_instance_name(table_name)}({buffer[0:-1]}) nosegment'
        self.handle.ora_execute_dml_sql(sql)

    def _remove_virtual_index(self, schema_name: str, index_name: str) -> None:
        '''
        删除虚拟索引, 没有任何输出
        :param index_name:
        :return:
        '''
        sql = f'drop index {index_name}'
        self.handle.ora_execute_dml_sql(sql)

    # --- utility function -- #
    def get_sql_id_list(self, tab_name: str):
        _, sqlid_list = self.package.get(tab_name)
        return sqlid_list

    def build_virtual_index(self, tab_name: str):
        sql_sess_on = 'alter session set "_use_nosegment_indexes"=true'
        self.handle.ora_execute_dml_sql(sql_sess_on)
        tuple_list, sqlid_list = self.package.get(tab_name)
        for col_combs, index_name in tuple_list:
            print(colored(f'\t{index_name:s}', 'yellow'))
            self._add_virtual_index(index_name=index_name, table_name=tab_name, col_list=col_combs.split(' '))

    def remove_virtual_index(self, tab_name: str):
        tuple_list, sqlid_list = self.package.get(tab_name)
        instance, schema, tab = tab_name.split('.')
        for col_combs, index_name in tuple_list:
            print(colored(f'\t{index_name:s}', 'yellow'))
            self._remove_virtual_index(schema_name=schema, index_name=index_name)
        sql_sess_off = 'alter session set "_use_nosegment_indexes"=false'
        self.handle.ora_execute_dml_sql(sql_sess_off)

    def get_explain(self, sql_text: str, instance_schema_tab: str) -> (pd.DataFrame, str):
        '''

        :param sql_text:
        :param schema_name:
        :return: explian_raw, explain_text
        '''

        schema_name = instance_schema_tab.split('.')[1]
        header = ["STATEMENT_ID", "PLAN_ID", "TIMESTAMP", "REMARKS", "OPERATION", "OPTIONS", "OBJECT_NODE",
                  "OBJECT_OWNER",
                  "OBJECT_NAME", "OBJECT_ALIAS", "OBJECT_INSTANCE", "OBJECT_TYPE", "OPTIMIZER", "SEARCH_COLUMNS", "ID",
                  "PARENT_ID", "DEPTH", "POSITION", "COST", "CARDINALITY", "BYTES", "OTHER_TAG", "PARTITION_START",
                  "PARTITION_STOP", "PARTITION_ID", "OTHER", "DISTRIBUTION", "CPU_COST", "IO_COST", "TEMP_SPACE",
                  "ACCESS_PREDICATES", "FILTER_PREDICATES", "PROJECTION", "TIME", "QBLOCK_NAME"]

        explain = self.oral_lib.get_explain(schema_name, sql_text)
        explain_raw = pd.DataFrame(explain['explaindata'], columns=header)
        explain_text = explain['explaindesc']
        return explain_raw, explain_text

    def compared_explain(self, explain_old: pd.DataFrame, explain_new: pd.DataFrame) -> float:
        left_v = np.sum(explain_old["COST"])
        right_v = np.sum(explain_new["COST"])
        return (left_v - right_v) / left_v * 100.0

    def save_to_database(self, sql_id: str, **kwargs) -> None:
        try:
            sharepool = AISrOracleSharepoolSrc.objects.get(sql_id=sql_id, instance_url=self.instance_url,
                                                           snapshot=self.handle.snapshot)
        except AISrOracleSharepoolSrc.DoesNotExist:
            print(f'数据库ai_sr_oracle_sharepool_src 未找到 sql id : {sql_id:s}')
            return
        except Exception as e:
            return
        for key in kwargs:
            value = kwargs[key]
            try:
                if isinstance(value, pd.DataFrame):
                    value = json.dumps(value.to_dict())
                if not isinstance(value, str):
                    value = json.dumps(value)
                sharepool.__dict__[key] = value
            except:
                pass
        sharepool.save()

    def read_from_database(self, sql_id: str, column='') -> object:
        sql = f'select * from dpaacc.ai_sr_oracle_sharepool_src where sql_id = "{sql_id:s}" and snapshot = "{self.handle.snapshot:s}" and instance_url = "{self.instance_url}"'
        try:
            df = GetSQL(sql_text=sql, connect_info=self.connection).get_data()
        except:
            return None
        if column == '':
            return df
        else:
            return df.iloc[0][column]

    # ------------- #
    #      main     #
    # ------------- #
    def run(self) -> EvaluatorPackage:

        origin_out_stream = sys.stdout
        Utility.print_screen_only()

        out = EvaluatorPackage()
        # -- get and save new explain -- #
        sql_id_checker = dict()
        print(colored('构建虚拟索引：', 'yellow'))
        for e, tab in enumerate(self.package):
            sql_id_list = self.get_sql_id_list(tab)
            self.build_virtual_index(tab)
            for id in sql_id_list:
                try:
                    sql_id_checker[id].append(tab)
                except:
                    sql_id_checker[id] = [tab]
        print(colored('获取执行计划：', 'yellow'))
        length = len(sql_id_checker)
        cnt = 0
        #--- #
        for i, id in enumerate(sql_id_checker):
            # --- verbose -- #
            percent = float(i) / float(length) * 100.0
            sys.stdout.write(
                "\r{0:.3g}%".format(percent))
            sys.stdout.flush()
            if int(percent) % 5 == 0:
                cnt += 1
            # --- #
            tab = sql_id_checker[id][0]
            
            sql_text = str(self.read_from_database(sql_id=id, column='sql_text'))
            sp = parser.SQLParser(sql_text)
            sql_text = sp.add_schema_name(tab.split('.')[1])

            try:
                new_raw, new_text = self.get_explain(sql_text=sql_text, instance_schema_tab=tab)
            except Exception as ex:
                continue
            # for tab in sql_id_checker[id]:
            self.save_to_database(id, plan_raw_after=new_raw, plan_text_after=new_text)
        print(colored('\t执行计划获取完成', 'yellow'))
        # -- get, save old explain and evaluate them

        print(colored('删除虚拟索引:', 'yellow'))
        for tab in self.package:
            self.remove_virtual_index(tab)

        print(colored('获取执行计划：', 'yellow'))
        length = len(sql_id_checker)
        cnt = 0
        for i, id in enumerate(sql_id_checker):
            # --- verbose -- #
            percent = float(i) / float(length) * 100.0
            sys.stdout.write(
                "\r{0:.3g}%".format(percent))
            sys.stdout.flush()
            if int(percent) % 5 == 0:
                cnt += 1
            # --- #
            tab = sql_id_checker[id][0]
            df = self.read_from_database(sql_id=id)
            sql_text = df.iloc[0]['sql_text']

            sp = parser.SQLParser(sql_text)
            sql_text = sp.add_schema_name(tab.split('.')[1])

            try:
                old_raw, old_text = self.get_explain(sql_text=sql_text, instance_schema_tab=tab)           
            except:
                continue
            new_raw = pd.DataFrame(json.loads(df.iloc[0]['plan_raw_after']))
            new_text = df.iloc[0]['plan_text_after']
            performance = self.compared_explain(old_raw, new_raw)
            self.save_to_database(id, improvement=str(performance), plan_raw_before=old_raw,
                                  plan_text_before=old_text)

            # --- conceal to package --- #
            for tab in sql_id_checker[id]:
                out.insert(instnt_schm_tb_name=tab, sql_id=id, sql_text=sql_text, plan_before=old_text,
                           plan_after=new_text, improvement=performance)

        print(colored('\t执行计划获取完成', 'yellow'))

        sys.stdout = origin_out_stream

        return out
