# -*- coding: UTF-8 -*-

import abc
import six


@six.add_metaclass(abc.ABCMeta)
class HttpAbstractBaseObject(object):
    """
    接口抽象基类, 所有处理类集成此类, 且实现 handle 方法
    """
    # __metaclass__ = abc.ABCMeta

    def __init__(self, http_dict_data):
        assert isinstance(http_dict_data, dict)
        self._http_data = http_dict_data

    @abc.abstractmethod
    def handle(self):
        """
        处理方法
        """
        pass
