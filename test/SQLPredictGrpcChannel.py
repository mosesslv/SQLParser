# -*- coding: UTF-8 -*-

from service.grpc_channel.oracle_sql_predict_request import OracleSQLPredictGrpc
import common.utils as utils
import common.common_time as common_time
from django.conf import settings
import logging
logger = logging.getLogger("")

GRPC_SERVER = "127.0.0.1"
GRPC_PORT = 50055
GRPC_AUTH_STRING = "lufax_grpc_auth"
GRPC_AUTH_CODE = "804"


class OracleSQLPredictGRpcChannel:
    """
    oracle sql predict 数据grpc渠道方法
    class构建时建立RPC连接
    """
    def __init__(self):
        server_ip = GRPC_SERVER
        server_port = GRPC_PORT
        grpc_auth_string = GRPC_AUTH_STRING
        grpc_auth_code = GRPC_AUTH_CODE
        self._sqlexec_request = OracleSQLPredictGrpc(server_ip, server_port, grpc_auth_string, grpc_auth_code)

    def sql_predict(self, data_dict):
        """
        通过 grpc CHANNEL 发送 sql 对象在生产执行
        :param data_dict:
        ["tenant", "host", "port", "username", "passwd", "instance_name", "schema", "sql_text", "sequence"]
        :return: dict
        """
        return self._sqlexec_request.sql_predict_request(data_dict)
