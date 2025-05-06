# -*- coding: UTF-8 -*-

import redis
from django.conf import settings
import time
import logging
logger = logging.getLogger("")


def _get_redis_client(redis_url=None):
    _redis_url = redis_url or settings.LOCK_REDIS
    return redis.from_url(_redis_url)


def acquire(lockname, blocking=True, timeout=60):
    """
    使用 Redis 实现的互斥锁，默认 60 秒超时
    :param lockname:
    :param blocking:
    :param timeout:
    :return:
    """
    if lockname is None:
        raise ValueError("lockname 不能为 None")
    if timeout <= 0:
        raise ValueError("timeout 不能小于0，任何锁都需要有超时间，默认60秒")
    redis_client = _get_redis_client()
    # 如果声明了 blocking，但是又没有锁定
    while True:
        locked = redis_client.setnx(lockname, "dpaacc")
        if locked:
            break
        else:
            if blocking:
                logging.info("acquire {0} blocking".format(lockname))
                time.sleep(1)
            else:
                break

    # 如果锁定成功，就设定超时时间
    if locked:
        logging.info("抢占锁 {0} 成功".format(lockname))
        redis_client.expire(lockname, timeout)
    else:
        logging.info("抢占锁 {0} 失败".format(lockname))
    return locked


def release(lockname):
    """
    释放锁
    :param lockname:
    :return:
    """
    if lockname is None:
        raise ValueError("lockname 不能为 None")
    redis_client = _get_redis_client()
    logging.info("释放锁 {0} ".format(lockname))
    redis_client.delete(lockname)


if __name__ == '__main__':
    lock_name = "repos-acquire:{0}".format("list-app")
    locked = acquire(lock_name)
    if locked:
        print("{0} locked".format(lock_name))
        release(lock_name)
