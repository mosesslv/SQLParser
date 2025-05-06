# -*- coding:utf-8 -*-

# this is demo client
from common.grpc.grpc_handle import GRpcClinet
import common.utils
from service.grpc_channel.lufax_grpc_service_define import LufaxGRpcService
import service.grpc_channel.helloworld_pb2_grpc as helloworld_pb2_grpc
import service.grpc_channel.helloworld_pb2 as helloworld_pb2
import time
import logging
logger = logging.getLogger("grpc")


def lufax_grpc_client():
    grpc_client = GRpcClinet("localhost", 50051, "HELLOWORLD", LufaxGRpcService, "lufax_grpc_auth", "804")
    assert isinstance(grpc_client.stub, helloworld_pb2_grpc.GreeterStub)
    while True:
        response = grpc_client.stub.SayHello(helloworld_pb2.HelloRequest(name='you'))
        print("{0} -> Greeter client received: {1}".
              format(common.utils.get_current_class_methord_name(None), response.message))

        response = grpc_client.stub.SayHelloAgain(helloworld_pb2.HelloRequest(name='you again'))
        print("{0} -> Greeter client received again: {1}".
              format(common.utils.get_current_class_methord_name(None), response.message))

        time.sleep(0.1)


if __name__ == '__main__':
    grpc_client = GRpcClinet("localhost", 50051, "HELLOWORLD", LufaxGRpcService, "lufax_grpc_auth", "804")
    # assert isinstance(grpc_client.stub, helloworld_pb2_grpc.GreeterStub)
    while True:
        response = grpc_client.stub.SayHello(helloworld_pb2.HelloRequest(name='you'))
        print("{0} -> Greeter client received: {1}".
              format(common.utils.get_current_class_methord_name(None), response.message))

        response = grpc_client.stub.SayHelloAgain(helloworld_pb2.HelloRequest(name='you again'))
        print("{0} -> Greeter client received again: {1}".
              format(common.utils.get_current_class_methord_name(None), response.message))

        time.sleep(0.1)
