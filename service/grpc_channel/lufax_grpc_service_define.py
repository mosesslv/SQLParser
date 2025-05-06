# -*- coding:utf-8 -*-

import enum


@enum.unique
class LufaxGRpcService(enum.Enum):
    """
    regist lufax grpc service
    """
    HELLOWORLD = {
        "protobuf": "service.grpc_channel.helloworld_pb2",
        "grpc_service": "service.grpc_channel.helloworld_pb2_grpc",
        "grpc_regist": "add_GreeterServicer_to_server",
        "grpc_handle_file": "service.grpc_channel.helloworld_service",
        "grpc_handle_class": "Greeter",
        "grpc_service_define_class": "GreeterStub"
    }

    OracleSQLPredict = {
        "protobuf": "service.grpc_channel.oracle_sql_predict_pb2",
        "grpc_service": "service.grpc_channel.oracle_sql_predict_pb2_grpc",
        "grpc_regist": "add_OracleSQLPredictRequesterServicer_to_server",
        "grpc_handle_file": "service.grpc_channel.oracle_sql_predict_service",
        "grpc_handle_class": "OracleSQLPredictRequester",
        "grpc_service_define_class": "OracleSQLPredictRequesterStub"
    }

    DataService = {
        "protobuf": "service.grpc_channel.data_service_pb2",
        "grpc_service": "service.grpc_channel.data_service_pb2_grpc",
        "grpc_regist": "add_DataServiceRequesterServicer_to_server",
        "grpc_handle_file": "service.grpc_channel.data_service",
        "grpc_handle_class": "DataServiceRequester",
        "grpc_service_define_class": "DataServiceRequesterStub"
    }
