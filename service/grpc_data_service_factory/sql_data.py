# -*- coding:utf-8 -*-

"""
data_type: GET_SQL_DATA

input: {"sql_sequence": "xxx"}
output:

{
    "db_type": "ORACLE",
    "sql_text": "xxxxxxx",
    "db_conn_url": "xxxxxxx",
    "statement": "SELECT | INSERT | UPDATE | DELETE",
    "dynamic_mosaicking": "xxx",
    "table_names": ["table1", "table2"],
    "plan_text": "xxxxx",
    "table_info": {xxxxxxxxxxxxxxxxxx},
    "histogram": [tuple()],

}

"""


from service.grpc_data_service_factory.grpc_data_factory import GRpcAbstractBaseObject
from service.ai_sr_models import AISrSQLDetail
import json
import common.utils as utils


class SQLData(GRpcAbstractBaseObject):
    """
    oracle index 统计数据
    """
    def handle(self):
        """
        数据处理
        :return: boolean, json_string
        """
        try:
            data_dict = json.loads(self._json_string)
            sql_sequence = data_dict["sql_sequence"]
        except Exception as ex:
            msg = "json load exception [{0}]\n[{1}]".format(ex, self._json_string)
            return False, msg

        try:
            sql_detail = AISrSQLDetail.objects.get(sequence=sql_sequence)
            assert isinstance(sql_detail, AISrSQLDetail)
        except Exception as ex:
            msg = "[{0}] can not find sql data [{1}]".format(sql_sequence, ex)
            return False, msg

        db_type = sql_detail.db_type
        sql_text = sql_detail.sql_text
        db_conn_url = sql_detail.db_conn_url
        statement = sql_detail.statement
        dynamic_mosaicking = sql_detail.dynamic_mosaicking
        table_names = sql_detail.table_names
        plan_text = sql_detail.plan_text
        sql_data = sql_detail.sql_data

        if utils.str_is_none_or_empty(sql_data):
            table_info_dict = {}
            histogram = []
        else:
            try:
                sql_data_dict = json.loads(sql_data)
                table_info_dict = sql_data_dict["TABLE_INFO"]
                histogram = sql_data_dict["HISTOGRAM"]
            except Exception as ex:
                table_info_dict = {}
                histogram = []

        response_dict = {
            "db_type": str(db_type),
            "sql_text": str(sql_text),
            "db_conn_url": str(db_conn_url),
            "statement": str(statement),
            "dynamic_mosaicking": str(dynamic_mosaicking),
            "table_names": [] if utils.str_is_none_or_empty(table_names) else json.loads(table_names),
            "plan_text": str(plan_text),
            "table_info": table_info_dict,
            "histogram": histogram
        }
        return True, response_dict
