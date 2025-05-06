# -*- coding:utf-8 -*-

import datetime
from common.grpc.grpc_handle import GRpcClinet
from service.grpc_channel.lufax_grpc_service_define import LufaxGRpcService
import service.grpc_channel.oracle_sql_predict_pb2 as oracle_sql_predict_pb2
import service.grpc_channel.oracle_sql_predict_pb2_grpc as oracle_sql_predict_pb2_grpc
import common.utils as utils
import common.common_time as common_time
import logging
logger = logging.getLogger("grpc")


class OracleSQLPredictGrpc:
    """
    sql predict request
    """
    def __init__(self, server_ip, server_port, grpc_auth_string, grpc_auth_code, grpc_timeout=60*5):
        self._server_ip = server_ip
        self._server_port = server_port
        self._grpc_auth_string = grpc_auth_string
        self._grpc_auth_code = grpc_auth_code

        self._grpc_client = GRpcClinet(
            self._server_ip, self._server_port, "OracleSQLPredict", LufaxGRpcService, self._grpc_auth_string,
            self._grpc_auth_code, rpc_keepalive_timeout_ms=grpc_timeout*1000)
        assert isinstance(self._grpc_client.stub, oracle_sql_predict_pb2_grpc.OracleSQLPredictRequesterStub)

    def sql_predict_request(self, parameter_dict) -> dict:
        """
        根据sql predict request 对象数据获取执行计划对象
        :param parameter_dict:
        [ "userid", "tenant", "profile_name", "schema", "sql_text", "sequence" ]
        :return: dict
        """
        oracle_sql_predict_request = oracle_sql_predict_pb2.OracleSQLPredictRequest()
        oracle_sql_predict_request.rpc_sequence = utils.get_uuid()
        oracle_sql_predict_request.sql_sequence = parameter_dict["sequence"]
        oracle_sql_predict_request.tenant = parameter_dict["tenant"]
        oracle_sql_predict_request.userid = parameter_dict["userid"]

        # oracle_sql_predict_request.host = parameter_dict["host"]
        # oracle_sql_predict_request.port = parameter_dict["port"]
        # oracle_sql_predict_request.username = parameter_dict["username"]
        # oracle_sql_predict_request.passwd = parameter_dict["passwd"]
        # oracle_sql_predict_request.instance_name = parameter_dict["instance_name"]
        oracle_sql_predict_request.profile_name = parameter_dict["profile_name"]

        oracle_sql_predict_request.schema = parameter_dict["schema"]
        oracle_sql_predict_request.sql_text = parameter_dict["sql_text"]
        oracle_sql_predict_request.addition_json_string = ""

        oracle_sql_predict_response_protobuf = self._grpc_client.stub.SQLPredict(oracle_sql_predict_request)

        logger.info("[{0}] get response sql predict data [{1}] ......".
                    format(utils.get_current_class_methord_name(self),
                           oracle_sql_predict_response_protobuf.rpc_sequence))

        ai_dict = {
            "AI_RESULT": oracle_sql_predict_response_protobuf.ai_result,
            "AI_RECOMMEND": oracle_sql_predict_response_protobuf.ai_recommend,
            "MESSAGE": oracle_sql_predict_response_protobuf.message
        }
        return ai_dict
