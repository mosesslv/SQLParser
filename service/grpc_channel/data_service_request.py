# -*- coding:utf-8 -*-

import datetime
from common.grpc.grpc_handle import GRpcClinet
from service.grpc_channel.lufax_grpc_service_define import LufaxGRpcService
import service.grpc_channel.data_service_pb2 as data_service_pb2
import service.grpc_channel.data_service_pb2_grpc as data_service_pb2_grpc
import common.utils as utils
import common.common_time as common_time
import json
import logging
logger = logging.getLogger("grpc")


class DataService:
    """
    data service request
    """
    def __init__(self, server_ip, server_port, grpc_auth_string, grpc_auth_code, grpc_timeout=60*5):
        self._server_ip = server_ip
        self._server_port = server_port
        self._grpc_auth_string = grpc_auth_string
        self._grpc_auth_code = grpc_auth_code

        self._grpc_client = GRpcClinet(
            self._server_ip, self._server_port, "DataService", LufaxGRpcService, self._grpc_auth_string,
            self._grpc_auth_code, rpc_keepalive_timeout_ms=grpc_timeout*1000)
        assert isinstance(self._grpc_client.stub, data_service_pb2_grpc.DataServiceRequesterStub)

    def data_service_request(self, servicetype, data_dict) -> dict:
        """
        数据服务请求
        :param servicetype:
        :param data_dict: 根据servicetype自定义数据结构
        :return: dict
        """
        data_service_requester = data_service_pb2.DataServiceRequest()
        data_service_requester.sequence = utils.get_uuid()
        data_service_requester.servicetype = servicetype
        data_service_requester.json_string = json.dumps(data_dict, cls=utils.CJsonEncoder)

        data_service_responser_protobuf = self._grpc_client.stub.SendDataServiceRequest(data_service_requester)
        handle_result = data_service_responser_protobuf.handle_result
        json_string = data_service_responser_protobuf.json_string

        logger.info("[{0}] get response data [{1}] ......".
                    format(utils.get_current_class_methord_name(self),
                           data_service_responser_protobuf.sequence))

        if not handle_result:
            return None

        data_dict = json.loads(json_string)
        return data_dict
