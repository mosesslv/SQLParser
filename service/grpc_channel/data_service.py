# -*- coding:utf-8 -*-

import service.grpc_channel.data_service_pb2_grpc as data_service_pb2_grpc
import service.grpc_channel.data_service_pb2 as data_service_pb2
import common.common_time
import json
from service.grpc_data_service_factory.grpc_data_factory import GRpcFactory
from service.django_decorator.orm_decorator import decorator_close_db_connections
import common.utils as utils
import logging
logger = logging.getLogger("grpc")


class DataServiceRequester(data_service_pb2_grpc.DataServiceRequesterServicer):

    @decorator_close_db_connections
    def SendDataServiceRequest(self, request, context):
        assert isinstance(request, data_service_pb2.DataServiceRequest)
        logger.info("[{0}] receive data_service request data [{1}\n\n{2}\n\n{3}]".
                    format(common.utils.get_current_class_methord_name(self),
                           request.sequence, request.servicetype, request.json_string))

        try:
            handle_class = GRpcFactory(request.servicetype, request.json_string).get_handle_class()
            response_result, response_dict = handle_class.handle()
            result = response_result
        except Exception as ex:
            msg = "handle class exception [{0}] [{1}\n\n{2}\n{3}]".\
                format(ex, request.sequence, request.servicetype, request.json_string)
            response_dict = {'content': msg}
            result = False

        data_service_response_protobuf = data_service_pb2.DataServiceResponse()
        data_service_response_protobuf.sequence = request.sequence
        data_service_response_protobuf.servicetype = request.servicetype
        data_service_response_protobuf.handle_result = "SUCCESS" if result else "FAILED"
        data_service_response_protobuf.json_string = json.dumps(response_dict, cls=utils.CJsonEncoder)
        return data_service_response_protobuf
