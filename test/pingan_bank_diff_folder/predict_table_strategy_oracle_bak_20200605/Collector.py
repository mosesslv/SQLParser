# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/26 下午4:21
# LAST MODIFIED ON:
# AIM: COLLECT tab_freq data as well as sql_freq data

from service.predict_table_strategy_oracle.InfoHandler import Handler_pingan
from typing import Dict, List, Tuple
from service.sql_parser_graph.units import ParseUnit, ParseUnitList
from service.predict_table_strategy_oracle.Utility.Utility_Data_get_from_sql import GetSQL, ExecuteSQL
from service.ai_sr_models import AISrTabStrategy
import pandas
import configparser
import datetime
import json
import os
# --- error handle --- #
from pandas.io.sql import DatabaseError

USE_SQLSTRUCT_DATA = False
MAX_ACCEPT_LENGTH = 256


class Extract():
    '''
    WARNING: only accept single schema case
    '''

    def __init__(self):
        # -- build connection -- #
        self.exist_in_aiSrTabStrategy = False
        self.error = dict()
        self.connection = configparser.ConfigParser()
        curfile = __file__
        curdir = os.path.split(curfile)[0]
        out = self.connection.read('{0}/../../config.ini'.format(curdir))
        if not out:
            self.error['config'] = 'error'
        self._fatal_error_chck()

    # ---------------------- #
    #  core utility function #
    # ---------------------- #
    def truncate_tab(self):
        from termcolor import colored
        sql = 'truncate table dpaacc.ai_sr_tab_strategy'
        print(colored('truncate ai_sr_tab_strategy','red'))
        ExecuteSQL(sql, self.connection)
        print('complete')

    def get_tab_col(self) -> Dict[str, List[str]]:
        '''
        get tab_name and col_name from a sql
        :return: [tab_name, [col_names]]
        '''
        out = dict()
        prsr_unt_lst = self.handler.sematic_info.elements
        for opt in prsr_unt_lst.by_type['OPT']:
            if opt.in_statement == 'WHERE' and \
                    not (self.__has_func_child(opt, prsr_unt_lst) and opt.name in ['IS', 'LIKE']):
                chldrn_id = prsr_unt_lst.find_root(opt)
                for id in chldrn_id:
                    child = prsr_unt_lst.by_id[id]
                    if child.type == 'COL':
                        col_name = child.name
                        tab_units = [prsr_unt_lst.by_id[i] for i in prsr_unt_lst.find_tab(child, tab_only=True)]
                        tab_names = [ele.name for ele in tab_units]
                        col_tab = self.handler.tab_info.chck_col_vld(col_name, tab_names)
                        if col_tab is None:
                            continue
                        _, t_id = col_tab
                        tab_name = tab_names[t_id]
                        try:
                            if col_name not in out[tab_name]:
                                out[tab_name].append(col_name)
                        except:
                            out[tab_name] = [col_name]
        return out

    def _fatal_error_chck(self):
        if 'config' in self.error:
            raise Exception('链接数据库配置文件config.ini读取错误（config.init　不存在？）')
        if 'pandas_database' in self.error:
            raise Exception('pandas 访问数据错误（ai_sr_tab_strategy 未建立？）')
        if 'mysql_connect' in self.error:
            raise Exception('尝试链接数据库错误')
        if 'public_columns' in self.error:
            raise Exception('尝试将tab存储到数据库错误')
        pass

    # --------------------------------- #
    #  meta utility serves core utility #
    # --------------------------------- #

    def __init_tabStrategy_data_block(self, tab_name: str, sql_frequency: float,
                                      aiSrTabStrtgy_data: pandas.DataFrame) -> dict:
        '''
        initialize a panda dataframe which has ai_sr_tab_strategy format
        :param tab_name:
        :return:
        '''
        tabStrategy = dict()
        tabStrategy['instance_name'] = str(self.handler.instance_name)
        tabStrategy['schema_name'] = str(self.handler.schema_name)
        tabStrategy['tab_name'] = tab_name
        tabStrategy['columns'] = json.dumps(self.handler.tab_info.get_all_col_name(tab_name).tolist())

        if aiSrTabStrtgy_data.empty:
            numrow, col_distinct = self.__get_col_distinct(tab_name)
            numrow = str(numrow)
            col_distinct = json.dumps(col_distinct)
            if self.handler.sql_id != "":
                sql_id_list = json.dumps([self.handler.sql_id])
            else:
                sql_id_list = None
        else:
            numrow = aiSrTabStrtgy_data.iloc[0]['numrow']
            col_distinct = aiSrTabStrtgy_data.iloc[0]['col_distinct']
            sql_id_list = aiSrTabStrtgy_data.iloc[0]['sqlid_list']
            if sql_id_list is None:
                sql_id_list = [self.handler.sql_id]
            else:
                sql_id_list = json.loads(sql_id_list)
                if self.handler.sql_id not in sql_id_list and self.handler.sql_id != "":
                    sql_id_list = sql_id_list + [self.handler.sql_id]
            sql_id_list = json.dumps(sql_id_list)

        tabStrategy['col_distinct'] = col_distinct
        tabStrategy['comb_col_freq'] = ''
        tabStrategy['numrow'] = str(numrow)
        tabStrategy['appearance'] = float(sql_frequency)
        tabStrategy['exist_idx'] = json.dumps(self.handler.index_info[tab_name])
        tabStrategy['sqlid_list'] = sql_id_list
        return tabStrategy

    def __to_class_AISrTabStrategy(self, container: dict) -> "AISrTabStrategy":
        # -- initialize AISrTabStrategy --- #
        try:
            ai_sr_tab_strategy = AISrTabStrategy.objects.get(instance_name=container['instance_name'],
                                                             schema_name=container['schema_name'],
                                                             tab_name=container['tab_name'])
        except AISrTabStrategy.DoesNotExist:
            ai_sr_tab_strategy = AISrTabStrategy()
            ai_sr_tab_strategy.updated_at = datetime.datetime.now()
            ai_sr_tab_strategy.updated_by = "sys"
            ai_sr_tab_strategy.created_at = datetime.datetime.now()
            ai_sr_tab_strategy.created_by = "sys"
        except Exception as e:
            self.error['public_columns'] = e.__str__()
        self._fatal_error_chck()
        ai_sr_tab_strategy.instance_name = container['instance_name']
        ai_sr_tab_strategy.schema_name = container['schema_name']
        ai_sr_tab_strategy.tab_name = container['tab_name']
        ai_sr_tab_strategy.columns = container['columns']
        ai_sr_tab_strategy.col_distinct = container['col_distinct']
        ai_sr_tab_strategy.comb_col_freq = container['comb_col_freq']
        ai_sr_tab_strategy.numrow = container['numrow']
        ai_sr_tab_strategy.appearance = container['appearance']
        ai_sr_tab_strategy.exist_idx = container['exist_idx']
        ai_sr_tab_strategy.sqlid_list = container['sqlid_list']
        return ai_sr_tab_strategy

    def __fetch_data_from_aiSrTabStrategy(self, tab_name: str) -> pandas.DataFrame:
        instance_name = self.handler.instance_name
        schema_name = self.handler.schema_name
        sql = "select * from ai_sr_tab_strategy " \
              f"where instance_name = \"{instance_name:s}\" " \
              f"and schema_name = \"{schema_name:s}\" " \
              f"and tab_name = \"{tab_name:s}\""
        # --- connect to database --- #
        df = pandas.DataFrame()
        try:
            df = GetSQL(sql_text=sql, connect_info=self.connection).get_data()
        except DatabaseError as e:
            self.error['pandas_database'] = e.__str__()
        except Exception as e:
            self.error['mysql_connect'] = e.__str__()
        self._fatal_error_chck()

        return df

    def __get_col_distinct(self, tab_name: str) -> Tuple[int, List[int]]:
        '''
        try to fetch col distinct from Oracle SQL Struct
        if the info is missing or USE_SQLSTRUCT_DATA is off
        then fetch them through the share pool
        :param tab_name:
        :return: (numrow, [dist_1,dist_2...])
        '''
        col_distinct = self.handler.tab_info.get_all_col_distinct(tab_name)
        numrow = self.handler.tab_info.get_tab_numrows(tab_name)
        if -1 in col_distinct or (not USE_SQLSTRUCT_DATA):
            # -- build query sql -- #
            all_col = self.handler.tab_info.get_all_col_name(tab_name)
            sql = 'select\n'
            sql += 'count(*) as numrows, \n'
            for col in all_col:
                sql += f'count(distinct({col:s})) as {col:s} ,\n'
            sql = sql[
                  0:-2] + f'\nfrom (select * from {self.handler.schema_name:s}.{tab_name:s} where rownum < 1000000)\n'

            # -- set up connection -- #
            try:
                col_distinct = self.handler.oracle_conn.ora_execute_query_get_all_data(sql_text=sql).data[0]
            except:
                raise Exception('通过share pool 访问抓取字段区分度失败')
            col_distinct = [v for v in col_distinct]
            numrow = col_distinct[0]
            col_distinct = col_distinct[1::]

        return numrow, col_distinct

    def __has_func_child(self, unit: ParseUnit, sem_info: ParseUnitList) -> bool:
        '''
        check if the given OPT unit has a FUNC child on its right
        :param unit:
        :param sem_info:
        :return:
        '''
        for c in unit.edges:
            c_unit = sem_info.by_id[c]
            if c_unit.id < unit.id:
                return False
            if c_unit.type == 'FUNC':
                return True
        return False

    def __update_list_comb(self, comb_col: List[str], aiSrTabStrtgy: pandas.DataFrame, sql_frequency: float) -> List[
        Tuple[str, int]]:
        '''

        :param aiSrTabStrtgy:
        :return: [('col1 col2 col3' , freq)]
        '''
        comb_col = ' '.join(v for v in sorted(comb_col))
        comb_col_freq = json.loads(aiSrTabStrtgy.comb_col_freq.values[0])
        out = []
        found = False
        for comb, freq in comb_col_freq:
            if comb_col == comb:
                freq += 1 * sql_frequency
                found = True
            out.append((comb, freq))
        if not found and len(comb_col_freq) < MAX_ACCEPT_LENGTH:
            out.append((comb_col, 1 * sql_frequency))
        return out

    # --------------- #
    #  main  method   #
    # --------------- #

    def run(self, info: Handler_pingan):
        '''
        the main method, collect data through share pool and save them into ai_sr_tab_strategy
        :param info:
        :return:
        '''
        self.handler = info
        tab_col_relation = self.get_tab_col()
        sql_frequency = info.sql_freq
        # --- collect data --- #
        for tab in tab_col_relation:

            aiSrTabStrtgy_data = self.__fetch_data_from_aiSrTabStrategy(tab)
            tabStrategy_container = self.__init_tabStrategy_data_block(tab_name=tab, sql_frequency=sql_frequency,
                                                                       aiSrTabStrtgy_data=aiSrTabStrtgy_data)

            # --- case 1  check if given tab already in ai_sr_tab_strategy_--- #
            if not aiSrTabStrtgy_data.empty:
                tabStrategy_container['comb_col_freq'] = json.dumps(
                    self.__update_list_comb(tab_col_relation[tab], aiSrTabStrtgy_data, sql_frequency))
                tabStrategy_container['appearance'] = str(
                    float(aiSrTabStrtgy_data.appearance.values[0]) + 1 * sql_frequency)

            # --- case 2 build new ai_sr_tab_strategy --- #
            else:
                tabStrategy_container['comb_col_freq'] = json.dumps(
                    [(' '.join(v for v in sorted(tab_col_relation[tab])), 1)])

            ai_sr_tab_strategy = self.__to_class_AISrTabStrategy(tabStrategy_container)
            # -- save data to database -- #
            ai_sr_tab_strategy.save()


if __name__ == '__main__':
    tab = 'ai_sr_sql_detail'
    all_col = ['id', 'created_by', 'created_at', 'updated_at', 'sequence', 'tenant_code', 'db_type', 'db_conn_url',
               'schema_name', 'sql_text', 'statement', 'dynamic_mosaicking', 'table_names']
    sql = 'select\n'
    sql += 'count(*) as numrows, \n'
    for col in all_col:
        sql += f'count(distinct({col:s})) as {col:s} ,\n'
    sql = sql[0:-2] + f'\nfrom {tab:s} rownum < 1000000\n'
    print(sql)
    a = Extract()
    print('done')
