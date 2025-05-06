# -*- coding:utf-8 -*-

from django.db import close_old_connections


def decorator_close_db_connections(method):
    """
    关闭无效的DB连接; 用于非HTTP请求的ORM可能产生的MYSQL非法连接;
    :param method:
    :return:
    """
    def _wrapper(self, *args):
        try:
            close_old_connections()
            method_result = method(self, *args)
        except Exception as e:
            raise Exception(e)
        else:
            return method_result
        finally:
            close_old_connections()
    return _wrapper
