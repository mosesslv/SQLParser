# -*- coding:utf-8 -*-

import time
import service.grpc_channel.helloworld_pb2_grpc as helloworld_pb2_grpc
import service.grpc_channel.helloworld_pb2 as helloworld_pb2
import common.common_time

class Greeter(helloworld_pb2_grpc.GreeterServicer):

    def SayHello(self, request, context):
        assert isinstance(request, helloworld_pb2.HelloRequest)
        print("{1} -> receive: {0}".format(request.name, common.common_time.current_datetime_serial_format()))

        hr = helloworld_pb2.HelloReply()
        hr.message = 'Hello, %s!' % request.name

        # return helloworld_pb2.HelloReply(message='Hello, %s!' % request.name)
        return hr

    def SayHelloAgain(self, request, context):
        print("{1} -> receive again: {0}".format(request.name, common.common_time.current_datetime_serial_format()))
        return helloworld_pb2.HelloReply(message='Hello, %s!' % request.name)
