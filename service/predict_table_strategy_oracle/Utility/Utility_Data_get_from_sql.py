# created by lv ao
# on 2020/5/26

import pandas
import mysql.connector
import time
from service.predict_table_strategy_oracle.Utility.SQL_SETTING import SQL_SERVER_ADDRESS
import configparser


class GetSQL:

    def __init__(self, sql_text: str, connect_info: configparser):
        self.conn = self.__connect_to_database(connect_info)
        self.data = pandas.read_sql(sql_text, self.conn)

    '''
    def __connect_to_database(self, _username="dbcm",
                              _passwd = 'dbcm',
                              _db_host = "172.19.44.12",
                              _db_port = 3306,
                              _database = 'dbcm'):
    '''

    def __connect_to_database(self, info:configparser):
        """
        mysql connect
        :return:
        """
        start_time = time.time()
        try:
            conn = mysql.connector.connect(user=info['DB']['USER'],
                                           password=info['DB']['PASSWORD'],
                                           host=info['DB']['HOST'],
                                           port=info['DB']['PORT'],
                                           database=info['DB']['NAME'])
            # print(f'connected cost:{elapsed_time:.2f}sec')
            return conn
        except mysql.connector.Error as e:
            last_error_message = "mysql connect error: {0}".format(e)
            print(last_error_message)
            raise
        except Exception as e:
            last_error_message = "mysql connect error: {0}".format(e)
            print(last_error_message)
            raise

    def get_data(self):
        return self.data

    def get_eigin(self):
        return self.conn

class ExecuteSQL:
    def __init__(self, sql_text:str, connect_info:configparser):
        self.__connect_to_database(connect_info)
        cursor = self.connect.cursor()
        cursor.execute(sql_text)
        self.connect.commit()

    def __connect_to_database(self, info):
        self.connect = mysql.connector.connect(user=info['DB']['USER'],
                                       password=info['DB']['PASSWORD'],
                                       host=info['DB']['HOST'],
                                       port=info['DB']['PORT'],
                                       database=info['DB']['NAME'])
class UpdateSQL:
    def __init__(self, sql_text, connect_info=SQL_SERVER_ADDRESS[3306]):
        self.__connect_to_database(connect_info)
        cursor = self.connect.cursor()
        cursor.execute(sql_text)
        self.connect.commit()

    def __connect_to_database(self, info):
        self.connect = \
            mysql.connector.connect(host=info['db_host'],
                                    user=info['username'],
                                    passwd=info['passwd'],
                                    port=info['db_port'],
                                    charset='utf8')

if __name__ == '__main__':
    server = SQL_SERVER_ADDRESS[3306]
    sql = 'truncate table dbcm.ai_sr_sql_detail_sample'

    UpdateSQL(sql,server)