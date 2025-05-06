# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/9/20
# LAST MODIFIED ON:
# AIM: addition handler

"""
为 AI 提供附加信息数据, 可扩展
"""
from common.MysqlHandle import MysqlHandle, MysqlHandleInfo
from service.AISQLReview.sql_abstract import MysqlSQLStruct

class MysqlAddition:
    """
    为 AI 提供 mysql 附加数据
    """
    def __init__(self, mysql_sql_struct):
        if mysql_sql_struct is None:
            raise Exception("Mysql sql struct invalid")

        assert isinstance(mysql_sql_struct, MysqlSQLStruct)
        self._mysql_sql_struct = mysql_sql_struct
        self.last_error_info = ""

    def get_column_discrimination_data(self, data_dict):
        """
        获取单个字典数据的区分度信息
        :param data_dict: {"columns": ["a", "b"], "sql_text": "select * from dual"}
        :return: int (-1:无效值)
        """
        try:
            columns = data_dict["columns"]
            sql_text = data_dict["sql_text"]
        except Exception as ex:
            self.last_error_info = "[{0}] data invalid, {1}".format(data_dict, ex)
            return -1

        assert isinstance(self._mysql_sql_struct.mysql_conn, MysqlHandle)
        disc_result = self._mysql_sql_struct.mysql_conn.mysql_execute_query_get_all_data(sql_text)
        assert isinstance(disc_result, MysqlHandleInfo)
        if not disc_result.result:
            self.last_error_info = "[{0}] data failed, {1}".format(data_dict, disc_result.message)
            return -1

        value = disc_result.data[0][0]

        # 附加信息也记录在 MysqlSQLStruct 对象
        mysql_addition_dict = self._mysql_sql_struct.addition
        if mysql_addition_dict is None:
            mysql_addition_col_disc = []
            mysql_addition_dict["COLUMNS_DISCRIMINATION"] = mysql_addition_col_disc
        else:
            try:
                mysql_addition_col_disc = mysql_addition_dict["COLUMNS_DISCRIMINATION"]
            except KeyError:
                mysql_addition_col_disc = []
                mysql_addition_dict["COLUMNS_DISCRIMINATION"] = mysql_addition_col_disc
            except Exception as ex:
                self.last_error_info = "[{0}] handle key [COLUMNS_DISCRIMINATION] exception [{1}]".\
                    format(mysql_addition_dict, ex)
                return -1

        addition_data = {
            "columns": columns,
            "sql_text": str(sql_text),
            "value": disc_result.data
        }
        mysql_addition_col_disc.append(addition_data)
        return value