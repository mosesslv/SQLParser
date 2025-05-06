# -*- coding: UTF-8 -*-

import common.utils as utils
from service.grpc_data_service_factory.grpc_factory_common import GRpcAbstractBaseObject
from service.grpc_data_service_factory.sql_data import SQLData
import json
import logging
logger = logging.getLogger("grpc")


class GRpcFactory(object):
    """
    HTTP 接口工厂, 所有的处理类必须在此注册
    """
    def __init__(self, service_type, json_string):
        self._service_type = service_type
        self._json_string = json_string

    # noinspection PyMethodMayBeStatic
    def get_handle_class(self):
        """
        得到处理类, 类必须继承 GRpcAbstractBaseObject , 且实现 handle 方法
        处理类必须注册到MAP
        """
        map_ = {
            "TEST": HttpTest(json.dumps({"key": "TEST"})),
            "GET_SQL_DATA": SQLData(self._json_string),
        }
        return map_[self._service_type]


class HttpTest(GRpcAbstractBaseObject):
    """
    test class
    """
    def handle(self):
        data_dict = json.loads(self._json_string)
        assert isinstance(data_dict, dict)
        return self._json_string
