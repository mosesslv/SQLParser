# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/1/21 下午2:56
# LAST MODIFIED ON:
# AIM:
from service.predict_table_strategy_oracle.Utility.Utility_Data_get_from_sql import GetSQL
from service.predict_table_strategy_oracle.Utility.Package import Package, BuilderPackage, EvaluatorPackage
import service.predict_table_strategy_oracle.Utility.htmlGenerator as htmlg
from service.sql_parser_graph.SQLParser import SQLParser
import service.sql_parser_graph as parser
import pandas
from pandas.io.sql import DatabaseError
import datetime
import configparser
import os
from typing import Tuple, Dict, Optional, List

class Reporter:

    def __init__(self, builder_packge: BuilderPackage, eval_package: EvaluatorPackage, sqlId_freq: Dict[str, float], lift_threshold: float):
        self.error = dict()
        self.builder_package = builder_packge
        self.eval_package = eval_package.toDataFrame()
        # -- #
        self.connection = configparser.ConfigParser()
        curfile = __file__
        curdir = os.path.split(curfile)[0]
        out = self.connection.read('{0}/../../config.ini'.format(curdir))
        if not out:
            self.error['config'] = 'error'
        self._fatal_error_chck()
        self.html_text = builder_packge.html + htmlg.g_verbose('')

        self.sqlId_freq = sqlId_freq
        self.lift_threshold = lift_threshold
        
        # -- by jbh 
        self.accept_tab = []

    def _filter_sqlId(self, insnt_schm_tab_name: str) -> [str]:
        '''
        remove ZERO improvement, sort DECREASING
        :param insnt_schm_tab_name: instance.schema.tab
        :return: list of sql_id
        '''
        df = self.eval_package[self.eval_package['instant_schema_tab_name'] == insnt_schm_tab_name]
        # - remove none improvement value
        df = df[df['improvement'] != 0]
        # - descent sort  by improvement value
        df = df.sort_values('improvement', ascending=False)
        return df

    # --- utility function ---- #
    def report_tab_index(self) -> Tuple[str, List[dict]]:
        str_buffer = ''
        if self.builder_package.is_empty():
            return str_buffer, []

        out_list = []
        for insnt_schm_tab_name in self.builder_package:
            if insnt_schm_tab_name not in self.accept_tab:
                continue

            out_dict = dict()
            cols_indexes, _ = self.builder_package.get(insnt_schm_tab_name)
            sql_ids = self._filter_sqlId(insnt_schm_tab_name)['sql_id'].values

            # -- save to buffer -- #
            str_buffer += insnt_schm_tab_name + '\n'
            out_dict['表名'] = insnt_schm_tab_name
            str_buffer += '构建索引：'
            for col_comb, index_name in cols_indexes:
                str_buffer += '\t  ' + index_name + '  (' + col_comb + ')\n'
            str_buffer += '对应的sql id：'
            out_dict['对应的sql_id'] = sql_ids
            for id in sql_ids:
                str_buffer += '\t  ' + id + '\n'

            out_list.append(out_dict)
        return str_buffer, out_list

    def report_sql_performance(self) -> Tuple[str, List[dict]]:
        str_buffer = ''
        if self.builder_package.is_empty():
            return str_buffer, []
        # - remove none improvement value
        df = self.eval_package[self.eval_package['improvement'] != 0]
        # - remove tables which lift less than threshold
        df = df[df['instant_schema_tab_name'].isin(self.accept_tab)]
        # - descent sort  by improvement value
        df = df.sort_values('improvement', ascending=False)
        out_list = []
        for id in df.index:
            out_dict = dict()
            data = df.loc[id]

            str_buffer += 'tab name : '
            str_buffer += data['instant_schema_tab_name'] + '\n'
            str_buffer += 'sql id : '
            str_buffer += data['sql_id'] + '\n'
            out_dict['sql_id'] = data['sql_id']

            str_buffer += 'sql text : \n'
            str_buffer += data['sql_text'] + '\n'

            sql_text = data['sql_text']
            if '\n' in sql_text:
                out_dict['sql_text'] = sql_text
            else:
                out_dict['sql_text'] = parser.reformat(sql_text)
            perf = data['improvement']
            str_buffer += f'性能提高 : {perf :.2f}%\n'
            out_dict['性能提高'] = f'{perf :.2f}%'

            out_dict['执行次数(次/天)'] = int(self.sqlId_freq[data['sql_id']])

            str_buffer += '原始执行执行计划 : \n'
            str_buffer += data['plan_berfore'] + '\n'
            out_dict['原始执行执行计划'] = data['plan_berfore']

            str_buffer += '加入索引后的执行计划 : \n'
            str_buffer += data['plan_after'] + '\n'
            out_dict['加入索引后的执行计划'] = data['plan_after']

            out_list.append(out_dict)
            str_buffer += '=================================================\n'
        return str_buffer, out_list

    def report_overall(self) -> Tuple[str, Optional[List[dict]]]:
        str_buffer = ''
        if self.builder_package.is_empty():
            return str_buffer, []
        out_dict = []
        for insnt_schm_tab_name in self.builder_package:
            e_dict = dict()

            df = self.eval_package[self.eval_package['instant_schema_tab_name'] == insnt_schm_tab_name]
            improvement = df['improvement'].values
            total = len(improvement)
            pos = (sum(improvement > 0) / total) * 100.0
            neg = (sum(improvement < 0) / total) * 100.0
            med = 100 - pos - neg

            # skip positive less than theta
            if pos < self.lift_threshold*100:
                continue

            cols_indexes, sql_ids = self.builder_package.get(insnt_schm_tab_name)
            str_buffer += 'tab name : ' + insnt_schm_tab_name + '\n'
            e_dict['表名'] = insnt_schm_tab_name

            str_buffer += '构建索引 : \n'
            index_list = []
            for col, index in cols_indexes: 
                col = col.split(' ')
                str_buffer += '\t' + index + ' (' + (',').join(v for v in col) + ')\n'
                index_list.append(index + ' (' + (',').join(v for v in col) + ')')
            e_dict['构建索引'] = index_list

            str_buffer += f' 性能最多提高 {max(improvement):.2f}\n'
            str_buffer += f'\t其中有 {pos:.2f}% 的sql性能提高, {100-pos:.2f}% 的sql性能不变\n\n'
            
            e_dict[f' 性能最多提高 {max(improvement):.2f}'] = f'\t其中有 {pos:.2f}% 的sql性能提高, {100-pos:.2f}% 的sql性能不变\n\n'
            
            
            out_dict.append(e_dict)
            self.accept_tab.append(insnt_schm_tab_name)
            # e_dict[
            #     f' 性能最多提高 {max(improvement):.2f}'] = f'\t其中有 {pos:.2f}% 的sql性能提高, {med:.2f}% 的sql性能不变, {neg:.2f}% 的sql性能变差\n\n'
            
        return str_buffer, out_dict

    # --- utility function ---- #
    def load_database(self, tab_name: str) -> pandas.DataFrame:
        self._fatal_error_chck()

        pk_sql = f'select * from {tab_name:s}'
        df = pandas.DataFrame()
        try:
            df = GetSQL(sql_text=pk_sql, connect_info=self.connection).get_data()
        except DatabaseError as e:
            self.error['pandas_database'] = e.__str__()
        except Exception as e:
            self.error['mysql_connect'] = e.__str__()
        self._fatal_error_chck(extra_info=tab_name)
        return df

    def _fatal_error_chck(self, extra_info: str = ''):
        if 'config' in self.error:
            raise Exception('链接数据库配置文件config.ini读取错误（config.init　不存在？）')
        if 'pandas_database' in self.error:
            if not extra_info or extra_info == "":
                raise Exception('pandas 访问数据错误')
            else:
                raise Exception(f'pandas 访问数据错误（{extra_info:s} 未建立？）')
        if 'mysql_connect' in self.error:
            raise Exception('尝试链接数据库错误')
        pass

    # --- main function --- #
    def run(self, output_type = 'ALL') -> str:
        overall_summary, overall_list = self.report_overall()
        sql_id_summary, sql_id_list = self.report_tab_index()
        sql_in_detail, sql_detail_list = self.report_sql_performance()


        self.html_text += htmlg.g_head('优化建议')
        self.html_text += htmlg.g_verbose('')
        for ddl in self.builder_package.to_ddl_given_instnt_schm_tb_name(self.accept_tab):
            self.html_text += htmlg.g_list(ddl)
        self.html_text += htmlg.g_verbose('')

        if overall_summary:
            print('------------------------------')
            print('          整体优化效果')
            print('------------------------------')
            print(overall_summary)
        else:
            print('没有达标的索引策略建议')
            self.html_text += htmlg.g_verbose('没有达标的索引策略建议')

        if len(overall_list) > 0:
            self.html_text += htmlg.g_head('整体优化效果') + htmlg.g_verbose('')
            for e in overall_list:
                self.html_text += htmlg.g_table_dict_h(e)
            self.html_text += htmlg.g_head('明细优化效果')
            for info in sql_id_list:
                for key in info:
                    value = info[key]
                    if isinstance(value, str):
                        self.html_text += htmlg.g_verbose(value + ' 对应的sql id:')
                    else:
                        for sql_id in value:
                            self.html_text += htmlg.g_list(words=sql_id, link=sql_id)

            for e in sql_detail_list:
                self.html_text += htmlg.g_table_dict_h(e, linked='sql_id')
                self.html_text += htmlg.g_verbose('')

        self.html_text += htmlg.g_ending()

        detail_summary = ''
        if sql_id_summary != '' and sql_in_detail != '':
            detail_summary += sql_id_summary + '\n'
            detail_summary += '------------------------------\n'
            detail_summary += '          sql in detail \n'
            detail_summary += '------------------------------\n'
            detail_summary += sql_in_detail
        else:
            print('没有达标的索引策略建议')
            self.html_text += htmlg.g_verbose('没有达标的索引策略建议')

        # ---- #
        curtime = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        if output_type in ['ALL', 'TXT']:
            filename = '{0}_{1}_detail.txt'.format(self.builder_package.get_instance_name(), curtime)
            # 使用'w' 不需要考虑清楚sql
            with open('/tmp/' + filename, 'w') as f:
                f.write(detail_summary)

        if output_type in ['ALL', 'HTML']:
            filename_html = '{0}_{1}_report.html'.format(self.builder_package.get_instance_name(), curtime)
            with open('/tmp/' + filename_html, 'w') as f:
                f.write(self.html_text)

        return self.html_text
