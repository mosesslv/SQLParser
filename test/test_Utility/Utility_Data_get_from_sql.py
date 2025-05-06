# created by bohuai jiang
# on 2019/5/29

import pandas
import mysql.connector
import time
from test.test_Utility.SQL_SETTING import SQL_SERVER_ADDRESS

class GetSQL:

    def __init__(self, sql_text,connect_info= SQL_SERVER_ADDRESS[3306]):
        self.conn = self.__connect_to_database(connect_info)
        self.data = pandas.read_sql(sql_text,self.conn)

    '''
    def __connect_to_database(self, _username="dbcm",
                              _passwd = 'dbcm',
                              _db_host = "172.19.44.12",
                              _db_port = 3306,
                              _database = 'dbcm'):
    '''
    def __connect_to_database(self, info):
            """
            mysql connect
            :return:
            """
            start_time = time.time()
            try:
                conn = mysql.connector.connect(user=info['username'],
                                               password=info['passwd'],
                                               host=info['db_host'],
                                               port=info['db_port'],
                                               database=info['database'])
                elapsed_time = time.time() - start_time
                #print(f'connected cost:{elapsed_time:.2f}sec')
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
        return self.data # return a pandas-dataframe like data strucutre

    def get_eigin(self):
        return self.conn


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


# if __name__ == '__main__':
    from pandas.io.sql import DatabaseError
    from mysql.connector.errors import DatabaseError
    # sql = 'select * from test'
    #GetSQL(sql_text=sql).get_data()
    # try:
    #     GetSQL(sql_text=sql,connect_info= SQL_SERVER_ADDRESS[3307]).get_data()
    # except DatabaseError as e:
    #     print('here')
    # except Exception as e:
    # #     print('e')
    # sql = 'select * from ai_sr_sql_detail where id = 1 '
    # v = GetSQL(sql).get_data()
    #
    # print(v)
    # print(v.empty)