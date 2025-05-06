# -*- coding: UTF-8 -*-
from django.core.cache import cache


def redis_set_key(key, value, time_out=3600):
    """
    set
    :param key:
    :param value:
    :param time_out:
    :return:
    """
    try:
        cache.set(key, value, time_out)
        return True
    except Exception as e:
        return False


def redis_get_key(key):
    """
    set
    :param key:
    :return:
    """
    try:
        return cache.get(key)
    except Exception as e:
        return ""


def redis_has_key(key):
    """
    set
    :param key:
    :return:
    """
    try:
        value = cache.get(key)
        if value is not None and len(value) > 0:
            return True
    except Exception as e:
        return False
