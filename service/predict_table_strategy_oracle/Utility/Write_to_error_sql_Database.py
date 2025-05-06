# CREATED BY: bohuai jiang 
# CREATED ON: 2019/8/23
# LAST MODIFIED ON: 2019/8/23
# AIM: write special sql to specified database
# -*- coding:utf-8 -*-

import sqlalchemy
import MySQLdb

from test.test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS
import pandas as pd


class WriteToErrorBase:
    def __init__(self):
        info = SQL_SERVER_ADDRESS[3306]
        self.connect = \
            MySQLdb.create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}'.
                                     format(info['username'], info['passwd'],
                                            info['db_host'], info['database']))

    def write(self, dataframe: pd.DataFrame, verbose: bool = True) -> None:
        dataframe.to_sql(con=self.connect,
                         name='AIreview_special_sql',
                         if_exists='append')
        if verbose:
            print('write to AIreview_special_sql SUCCESS')


class WriteToDataBase:
    def __init__(self):
        info = SQL_SERVER_ADDRESS[3306]
        self.connect = \
            sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                     format(info['username'], info['passwd'],
                                            info['db_host'], info['database']))

    def write(self, dataframe: pd.DataFrame, database_name: str, verbose: bool = True, if_exists:str = 'append') -> None:
        dataframe.to_sql(con=self.connect,
                         name=database_name,
                         if_exists= if_exists)
        if verbose:
            print('write to AIreview_special_sql SUCCESS')
