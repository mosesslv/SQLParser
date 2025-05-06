# -*- coding: UTF-8 -*-

import abc
import six


@six.add_metaclass(abc.ABCMeta)
class GRpcAbstractBaseObject(object):
    """
    接口抽象基类, 所有处理类集成此类, 且实现 handle 方法
    """
    def __init__(self, json_string):
        self._json_string = json_string

    @abc.abstractmethod
    def handle(self):
        """
        处理方法
        """
        pass
