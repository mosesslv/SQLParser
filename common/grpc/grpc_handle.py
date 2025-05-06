# -*- coding:utf-8 -*-

from concurrent import futures
import grpc
import traceback
import importlib
from grpc_reflection.v1alpha import reflection
import collections
import common.grpc.generic_client_interceptor as generic_client_interceptor
import enum


# service enum 在业务实现中重新定义, 以参数的放式提供给SERVER
@enum.unique
class GRpcService(enum.Enum):
    # demo
    # HELLOWORLD = {
    #     "protobuf": "Common.grpcservice.helloworld_pb2",                # protobuf file
    #     "grpc_service": "Common.grpcservice.helloworld_pb2_grpc",       # grpc define name
    #     "grpc_regist": "add_GreeterServicer_to_server",                 # regist grpc methord
    #     "grpc_handle_file": "Common.grpcservice.service1",              # handle file
    #     "grpc_handle_class": "Greeter"                                  # handle class name
    #     "grpc_service_define_class": "GreeterStub",                     # grpc define service class name
    # }
    """
    regist grpc service
    """


def _unary_unary_rpc_terminator(code, details):

    def terminate(ignored_request, context):
        context.abort(code, details)

    return grpc.unary_unary_rpc_method_handler(terminate)


class RequestHeaderValidatorInterceptor(grpc.ServerInterceptor):

    def __init__(self, header, value, code, details):
        self._header = header
        self._value = value
        self._terminator = _unary_unary_rpc_terminator(code, details)

    def intercept_service(self, continuation, handler_call_details):
        if (self._header,
                self._value) in handler_call_details.invocation_metadata:
            return continuation(handler_call_details)
        else:
            return self._terminator


class _ClientCallDetails(
        collections.namedtuple(
            '_ClientCallDetails',
            ('method', 'timeout', 'metadata', 'credentials')),
        grpc.ClientCallDetails):
    pass


def header_adder_interceptor(header, value):

    def intercept_call(client_call_details, request_iterator, request_streaming, response_streaming):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        metadata.append((
            header,
            value,
        ))
        client_call_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata,
            client_call_details.credentials)
        return client_call_details, request_iterator, None

    return generic_client_interceptor.create(intercept_call)


class GRpcServer:
    """
    grpc server implementation
    """
        
    def __init__(self, ip_address, port, service_enum, request_header, request_code, max_workers=10):
        self._ip_address = ip_address
        self._port = port
        self._max_workers = max_workers
        self._service_enum = service_enum
        self._request_header = request_header
        self._request_code = request_code
        self._grpc_server = self._create_server()

    def _create_server(self):
        """
        create server
        :return:
        """
        header_validator = RequestHeaderValidatorInterceptor(self._request_header,
                                                             self._request_code,
                                                             grpc.StatusCode.UNAUTHENTICATED,
                                                             'Access denied!')
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=self._max_workers),
                             interceptors=(header_validator,))
        return server

    def __get_grpc_data_from_enum(self, service_name):
        """
        get grpc data from enum
        :param service_name:
        :return: value tuple
        """
        try:
            grpc_data = self._service_enum[service_name]
            return grpc_data
        except Exception as ex:
            msg = "[{0}] get grpc data by enum exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

    def __regist_grpc_service_by_eval_noused(self, service_name):
        """
        动态注册GRPC服务, 方法不能使用, 作为案例存在
        -- 不可扩展; 在调用非本路径的类时, 找不到模块名称
        :param service_name: GRpcService -> key
        :return:
        """
        try:
            grpc_data = self.__get_grpc_data_from_enum(service_name)
            grpc_define = grpc_data.value[0]
            grpc_regist_methor = grpc_data.value[1]
            grpc_handle_class = grpc_data.value[2]
        except Exception as ex:
            msg = "[{0}] get grpc data by enum exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

        # import grpc_define
        try:
            # module = importlib.import_module(grpc_define)
            module = __import__(grpc_define)
        except Exception as ex:
            msg = "[{0}] import grpc define exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

        try:
            __import__(grpc_define)
            grcp_define_name = grpc_define.split(".")[-1]
            methord = "{0}.{1}".format(grcp_define_name, grpc_regist_methor)
            eval(methord)(grpc_handle_class, self._grpc_server)
        except Exception as ex:
            msg = "[{0}] eval regist grpc exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

    def __regist_grpc_service_by_getattr(self, service_name):
        """
        动态注册GRPC服务
        :param service_name: GRpcService -> key
        :return:
        """
        try:
            grpc_data = self.__get_grpc_data_from_enum(service_name)
            protobuf_file = grpc_data.value["protobuf"]
            grpc_define = grpc_data.value["grpc_service"]
            grpc_regist_methor = grpc_data.value["grpc_regist"]
            grpc_handle_filepath = grpc_data.value["grpc_handle_file"]
            grpc_handle_class_name = grpc_data.value["grpc_handle_class"]
        except Exception as ex:
            msg = "[{0}] get grpc data by enum exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

        # import grpc_define
        try:
            grpc_service_module = importlib.import_module(grpc_define)
            protobuf_module = importlib.import_module(protobuf_file)
            grpc_handle_module = importlib.import_module(grpc_handle_filepath)
            grpc_handle_obj = getattr(grpc_handle_module, grpc_handle_class_name)()
            # module = __import__(grpc_define)
        except Exception as ex:
            msg = "[{0}] import grpc define exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

        try:
            getattr(grpc_service_module, grpc_regist_methor)(grpc_handle_obj, self._grpc_server)
        except Exception as ex:
            msg = "[{0}] getattr regist grpc exception [{1}\n{2}]".format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

        try:
            full_name = getattr(getattr(protobuf_module, "DESCRIPTOR"),
                                "services_by_name")["{0}".format(grpc_handle_class_name)].full_name
            return full_name
        except Exception as ex:
            msg = "[{0}] getattr get service full name exception [{1}\n{2}]".\
                format(service_name, ex, traceback.format_exc())
            raise Exception(msg)

    def _regist_grpc_service(self, service_name):
        """
        regist service
        :param service_name: GRpcService -> key
        :return:
        """
        return self.__regist_grpc_service_by_getattr(service_name)

    def get_service_name(self):
        """
        get service
        :return: list
        """
        service_names = []
        for name, member in self._service_enum.__members__.items():
            service_names.append(name)
        return service_names

    def start(self):
        """
        start grpc service, don't block
        """
        try:
            service_full_names = []
            for name, member in self._service_enum.__members__.items():
                full_name = self._regist_grpc_service(name)
                service_full_names.append(full_name)

        except Exception as ex:
            msg = "regist grpc service exception [{0}\n{1}]".format(ex, traceback.format_exc())
            raise Exception(msg)

        # set reflection
        service_names = (
            ",".join(map(str, service_full_names)),
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(service_names, self._grpc_server)
        self._grpc_server.add_insecure_port("{0}:{1}".format(self._ip_address, self._port))
        self._grpc_server.start()

    def stop(self):
        """
        stop grpc server
        """
        self._grpc_server.stop(0)


class GRpcClinet:
    """
    grpc client implementation
    """

    def __init__(self, ip_address, port, service_name, service_enum, request_header, request_value,
                 rpc_keepalive_timeout_ms=60000, grpc_retries=0):
        self._ip_address = ip_address
        self._port = port
        self._grpc_keepalive_timeout_ms = rpc_keepalive_timeout_ms
        self._grpc_retries = grpc_retries
        self._service_enum = service_enum
        self._service_name = service_name
        self._request_header = request_header
        self._request_value = request_value

        self._channel = self._connect()
        self.stub = self._create_client_service_obj()

    def _connect(self):
        header_interceptor = header_adder_interceptor(
            self._request_header, self._request_value)

        channel = grpc.insecure_channel(
            target="{0}:{1}".format(self._ip_address, self._port),
            options=[('grpc.lb_policy_name', 'pick_first'),
                     ('grpc.enable_retries', self._grpc_retries),
                     ('grpc.keepalive_timeout_ms', self._grpc_keepalive_timeout_ms)])
        intercept_channel = grpc.intercept_channel(channel, header_interceptor)
        return intercept_channel

    def _create_client_service_obj(self):
        """
        获取grpc 定义对象
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        :return: service object
        """
        try:
            service_data = self._service_enum[self._service_name]
            grpc_define = service_data.value["grpc_service"]
            grpc_service_define_class = service_data.value["grpc_service_define_class"]
        except Exception as ex:
            msg = "[{0}] get service define exception [{1}\n{2}]".format(self._service_name, ex, traceback.format_exc())
            raise Exception(msg)

        # import module
        try:
            grpc_service_module = importlib.import_module(grpc_define)
            grpc_service_define_obj = getattr(grpc_service_module, grpc_service_define_class)(self._channel)
        except Exception as ex:
            msg = "[{0}] import grpc define exception [{1}\n{2}]".format(self._service_name, ex, traceback.format_exc())
            raise Exception(msg)

        return grpc_service_define_obj
