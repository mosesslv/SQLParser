# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/27 上午10:33
# LAST MODIFIED ON:
# AIM:
# NOTE: 比较相似度可以使用bit vector
import pandas
import configparser
import sys
from service.predict_table_strategy_oracle.Utility.Utility_Data_get_from_sql import GetSQL
from service.predict_sql_review_oracle.Utility.SORT import TabTree
from service.predict_table_strategy_oracle.Utility.Package import BuilderPackage
import json
import numpy as np
import os
from typing import List, Tuple, Dict
import copy
from service.predict_table_strategy_oracle import Utility
import service.predict_table_strategy_oracle.Utility.htmlGenerator as htmlg

# --- error handle --- #
from pandas.io.sql import DatabaseError

ACCEPT_TIME = 1
PERCENTILE = 10
DISTINCT_THETA = 0.005
DISTINCT_SINGLE_IDX = 0.05
MAX_INDEX = 5


class StrategyBuilder:

    def __init__(self, instance_name: str, **kwargs):
        '''
        allow model to read database and csv
        :param args:
        '''
        self.instance_name = instance_name
        self.error = dict()
        self.connection = configparser.ConfigParser()
        curfile = __file__
        curdir = os.path.split(curfile)[0]
        out = self.connection.read('{0}/../../config.ini'.format(curdir))
        if not out:
            self.error['config'] = 'error'
        try:
            self.csv_address = kwargs['csv_address']
        except:
            self.csv_address = ''

        self.accept_tab = []

        # html result
        try:
            self.html_text = kwargs['html_text']
        except:
            self.html_text = htmlg.g_begining() + htmlg.g_css() + htmlg.g_head('报告')

    # ---------------------- #
    #  core utility function #
    # ---------------------- #

    def load_database_info(self):
        self._fatal_error_chck()
        pk_sql = f'select * from ai_sr_tab_strategy where instance_name = "{self.instance_name}"'

        df = pandas.DataFrame()
        try:
            df = GetSQL(sql_text=pk_sql, connect_info=self.connection).get_data()
        except DatabaseError as e:
            self.error['pandas_database'] = e.__str__()
        except Exception as e:
            self.error['mysql_connect'] = e.__str__()
        self._fatal_error_chck()
        self.tab_data = df

    def load_from_csv(self):
        self.tab_data = pandas.read_csv(self.csv_address)

    def build_index_strategy(self, verbose=True) -> BuilderPackage:
        '''
        step 1: get frequency for each cols
        :return: ddl name 
        '''
        length = len(self.tab_data)

        instance_name_list = []
        schema_name_list = []
        tab_name_list = []
        index_name_list = []
        ddl = []
        # --- initialize packages --- #
        package = BuilderPackage()
        if verbose:
            print('开始构建索引策略')
            cnt = 0
        origin_out_str = sys.stdout
        Utility.print_screen_only()
        for i in range(length):
            comb_col_freq = json.loads(self.tab_data.iloc[i]['comb_col_freq'])
            try:
                exist_index = json.loads(self.tab_data.iloc[i]['exist_idx'])
            except:
                exist_index = []
            dstnct_tab = self._build_col_distinct_tab(i)
            candidate_col, frq_tab = self._filter_col_by_frequency(comb_col_freq)
            candidate_col = self._filter_col_by_distinct(candidate_col, dstnct_tab)
            index_list = self._build_index_recom(candidate_col, comb_col_freq, dstnct_tab, frq_tab)
            index_list = self._remove_sub_set(index_list)
            # TODO: comment dummy filter in product env
            # index_list = self._dummy_filter(comb_col_freq)
            index_list = self._remove_exist_index(exist_index, index_list)

            for id, col_combs in enumerate(index_list):
                # - #
                instance_name = self.tab_data.iloc[i]['instance_name']
                schema_name = self.tab_data.iloc[i]['schema_name']
                tab_name = self.tab_data.iloc[i]['tab_name']
                sqlid_list = json.loads(self.tab_data.iloc[i]['sqlid_list'])
                instance_name_list.append(instance_name)
                schema_name_list.append(schema_name)
                tab_name_list.append(tab_name)
                index_name_list.append(col_combs)
                try:
                    ddl.append(
                        self._build_DDL(col_combs, self.tab_data.iloc[i]['tab_name'],
                                        self.tab_data.iloc[i]['schema_name'],
                                        id))
                except:  # -- case data do not has schema_name (text purpose) -- #
                    ddl.append(
                        self._build_DDL(col_combs, self.tab_data.iloc[i]['tab_name'], 'DUMMY',
                                        id))
                # --- add to package --- #
                name_key = [instance_name, schema_name, tab_name]
                index_name = schema_name + '.' + tab_name[0:-1] + f'{id:d}'
                package.insert(('.').join(v for v in name_key), col_combs, index_name, sqlid_list)

            if verbose:
                percent = float(i) / float(length) * 100.0
                sys.stdout.write(
                    "\r{0}> {1:.3g}% | {2}".format("=" * cnt, percent, '计算表-' + self.tab_data.iloc[i]['tab_name']))
                sys.stdout.flush()
                if int(percent) % 5 == 0:
                    cnt += 1

        if verbose:
            sys.stdout.flush()
            sys.stdout.write("\r{0}> {1:.3g}% 生成完成\n".format("=" * cnt, 100))
        out = pandas.DataFrame()
        sys.stdout = origin_out_str
        # TODO: remove comments
        out['instance_name'] = instance_name_list
        out['schema_name'] = schema_name_list
        out['tab_name'] = tab_name_list
        out['index_name'] = index_name_list
        out['ddl'] = ddl

        figure_path = '/tmp/'
        if not os.path.exists(figure_path):
            os.makedirs(figure_path)
        # -- print ddl --#
        print('---------------------------------')
        print('             ddl')
        print('---------------------------------')
        self.html_text += htmlg.g_head('优化建议')
        self.html_text += htmlg.g_verbose('')
        for e_ddl in ddl:
            print(e_ddl + '\n')
            self.html_text += htmlg.g_list(e_ddl)

        return package

    def report(self, verbose: bool = True):
        org_len = len(self.tab_data)
        report = ''
        if verbose:
            print('=========================================================')
            print('                         报告                            ')
            print('---------------------------------------------------------')
            report += f'发现{org_len:d}张表'
            print(report)

        self.tab_data["appearance"] = pandas.to_numeric(self.tab_data["appearance"])
        self.tab_data = self.tab_data.sort_values('appearance', ascending=False)
        max_freq_tab = self.tab_data.loc[:, ['tab_name', 'sqlid_list', 'appearance']].head(10)
        # -- get sql_num -- #
        sql_num = []
        for id in max_freq_tab.index:
            sql_num.append(len(json.loads(max_freq_tab.loc[id]['sqlid_list'])))
        max_freq_tab['sqlid_list'] = sql_num

        max_freq_tab['appearance'] = max_freq_tab['appearance'].values.astype(int)
        max_freq_tab.index = range(len(max_freq_tab))
        max_freq_tab.columns = ['表名', '性能问题SQL数', '性能问题SQL执行次数']

        if verbose:
            print(f'前{len(max_freq_tab):d}个出现频率最高的表为(次/每天):')
            report += f'前{len(max_freq_tab):d}个出现频率最高的表为(次/每天):'
            print(max_freq_tab)
            self.html_text += htmlg.g_verbose(report)
        # -- add to html -- #
        self.html_text += htmlg.g_table_pd(max_freq_tab)

        # ----- data pruning ---- #
        id = 0
        for id in range(org_len):
            col_len = len(json.loads(self.tab_data['columns'].iloc[id]))
            appearance = float(self.tab_data['appearance'].iloc[id])
            if appearance < col_len * max(1.0, float(ACCEPT_TIME)):
                break
        id = id - 1
        self.tab_data = self.tab_data.iloc[0:id + 1]
        new_len = len(self.tab_data)
        if verbose:
            print(f'\n其中{(float(new_len) / float(org_len)) * 100.0:0.2f}%({new_len}个)表有足够的统计意义可以构建索引策略')
            self.html_text += htmlg.g_verbose(
                f'\n其中{(float(new_len) / float(org_len)) * 100.0:0.2f}%({new_len}个)表有足够的统计意义可以构建索引策略')

    # -------------------------------- #
    #  meta utility serve core utility #
    # -------------------------------- #
    def _remove_exist_index(self, exist_idx: List[str], index_List: List[str]) -> List[str]:
        out = []
        for recom_idx in index_List:
            already_exist = False
            for e_exist in exist_idx:
                if self.set_include_order(e_exist.split(' '), recom_idx.split(' ')):
                    already_exist = True
                    break
            if not already_exist:
                out.append(recom_idx)

        return out

    def _build_DDL(self, col_list: str, tab_name: str, schema_name: str, id: int) -> str:
        out = f'CREATE INDEX {schema_name:s}.{tab_name:s}_INDEX{id:d} ON {tab_name}('
        for col in col_list.split(' '):
            out += col + ','
        out = out[0:-1] + ') ONLINE COMPUTE STATISTICS;'
        return out

    def _build_col_distinct_tab(self, id: int) -> Dict[str, float]:
        numrow = self.tab_data.iloc[id]['numrow']
        cols = json.loads(self.tab_data.iloc[id]['columns'])
        distinct = json.loads(self.tab_data.iloc[id]['col_distinct'])
        # --- build col distinct table --- #
        dist_tab = dict()
        for col, dist in zip(cols, distinct):
            dist_tab[col] = float(dist) / (float(numrow) + 0.00001)
        return dist_tab

    def _build_index_recom(self, cnddt_list: List[str], comb_freq_stored: List[Tuple[str, float]],
                           dstnct_tab: Dict[str, float], freq_tab: Dict[str, float]) -> List[str]:
        '''
        不知道其什么名字
        :param level:
        :param cnddt_list:
        :param comb_feq_stored:
        :return:
        '''
        # --- sort candidate columns --- #
        cnddt_list = sorted(cnddt_list)
        # --- remove no comb who doesnt contain candidate columns -- #
        comb_list = []
        for comb, freq in comb_freq_stored:
            comb = comb.split(' ')
            intersect = self.set_intersect(comb, cnddt_list)
            if intersect:
                comb_list.append(intersect)

        # --- sort comb by distinct --- #
        out = []
        for comb in comb_list:
            tree_sort = TabTree()
            for col in comb:
                tree_sort.insert(name=col, distinct=freq_tab[col])
            index_list = tree_sort.tree_traverse(increase=False, out=[], with_value=False)
            if len(index_list) == 1 and dstnct_tab[index_list[0]] < DISTINCT_SINGLE_IDX:
                continue
            index_list = ' '.join(v for v in index_list)
            if index_list not in out:
                out.append(index_list)
        return out

    def _remove_sub_set(self, comb_list: List[str]) -> List[str]:
        '''

        :param index_list:
        :return:
        '''
        comb_list = sorted(comb_list)[::-1]
        out = copy.copy(comb_list)
        length = len(comb_list)
        for i in range(length):
            comb = comb_list[i].split(' ')
            for j in range(i + 1, length):
                sub = comb_list[j].split(' ')
                if len(sub) > len(comb):
                    break
                if self.set_include_order(comb, sub):
                    try:
                        out.remove(comb_list[j])
                    except:
                        pass

        return out

    def _filter_col_by_distinct(self, col_list: List[str], dstnct_tab: Dict[str, float]) -> List[str]:
        '''
        sort the given cols by their distinct value 2in decrease order
        :param col_list: [col]
        :param idx: index
        :return: [col]
        '''
        # --- sort cols by their  distinct --- #
        tab_tree = TabTree()
        for col in col_list:
            dist_ratio = dstnct_tab[col]
            if dist_ratio >= DISTINCT_THETA:
                tab_tree.insert(name=col, distinct=dist_ratio)
        return tab_tree.tree_traverse(increase=False, out=[], with_value=False)

    def _dummy_filter(self, comb_col_freq: List[Tuple[str, float]]) -> List[str]:
        '''
        test purpose
        :param comb_col_freq:
        :return:
        '''
        out = []
        for comb, freq in comb_col_freq:
            out.append(comb)
        return out

    def _filter_col_by_frequency(self, comb_col_freq: List[Tuple[str, float]]) -> Tuple[List[str], Dict[str, float]]:
        '''
        sort the cols who form comb_col_freq by their appearance frequency in decrease order
        :param comb_col_freq: [("col1 col2 " 2)]
        :return: [cols], frequency table
        '''
        tab = dict()
        for comb, freq in comb_col_freq:
            for col in comb.split(' '):
                try:
                    tab[col] = tab[col] + freq
                except:
                    tab[col] = freq
        theta = np.percentile(list(tab.values()), PERCENTILE)
        tab_tree = TabTree()
        # sort result by its frequency
        for col, freq in tab.items():
            if freq >= theta:
                tab_tree.insert(name=col, distinct=freq)
        return tab_tree.tree_traverse(increase=False, out=[], with_value=False), tab

    def _fatal_error_chck(self):
        # TODO: uncomment following code before commit
        if 'config' in self.error:
            raise Exception('链接数据库配置文件config.ini读取错误（config.init　不存在？）')
        if 'pandas_database' in self.error:
            raise Exception('pandas 访问数据错误（ai_sr_tab_strategy 未建立？）')
        if 'mysql_connect' in self.error:
            raise Exception('尝试链接数据库错误')

    # ------------- #
    #  set utility  #
    # ------------- #

    def set_include(self, set: List[str], sub_set: List[str]) -> bool:
        for v in sub_set:
            if v not in set:
                return False
        return True

    def set_include_order(self, set: List[str], sub_set: List[str]) -> bool:
        if len(set) < len(sub_set):
            return False
        length = len(sub_set)
        for i in range(length):
            if set[i] != sub_set[i]:
                return False
        return True

    def set_intersect(self, set_left: List[str], set_right: List[str]) -> List[str]:
        out = []
        for v in set_right:
            if v in set_left:
                out.append(v)
        return out

    # --------------- #
    #  main  method   #
    # --------------- #

    def run(self, verbose: bool = True) -> BuilderPackage:
        '''
        return DDL file name
        '''
        if self.csv_address == '':
            self.load_database_info()
        else:
            self.load_from_csv()

        self.report(verbose=verbose)
        package = self.build_index_strategy(verbose=verbose)
        package.html = self.html_text
        return package


if __name__ == '__main__':
    pass
# builder = StrategyBuilder(')
# builder.load_from_csv()
# builder.report()
# builder.build_index_strategy()
# builder.run()
# test = ["PRODUCT_ID CREATED_AT","PRODUCT_ID", "CREATED_AT", "ID SID","PRODUCT_ID CREATED_AT UPDATED_AT ","BUYER_USER_ID","BUYER_USER_ID PRODUCT_ID"]
# print(builder.remove_sub_set(test))
