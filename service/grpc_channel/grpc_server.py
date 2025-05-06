# -*- coding:utf-8 -*-

from django.db import connection, connections
from django.conf import settings
from common.grpc.grpc_handle import GRpcServer
import common.utils
import time
import os
from service.grpc_channel.lufax_grpc_service_define import LufaxGRpcService
import datetime
import logging
logger = logging.getLogger("grpc")


def lufax_grpc_server(listen_port, request_header, request_code):
    """
    lufax start grpc server
    :return:
    """
    host = "0.0.0.0"
    grpc_server = GRpcServer(host, listen_port, LufaxGRpcService, request_header, request_code)
    grpc_server.start()

    db_ping_time = datetime.datetime.now()
    # connection.cursor()
    try:
        while True:
            logging.info("[{0}] [{1}] grpc service running ......".
                         format(common.utils.get_current_class_methord_name(None),
                                ",".join(map(str, grpc_server.get_service_name()))))

            # current_time = datetime.datetime.now()

            # 定时清理废弃的DB连接, 防止因为DJANGO的CONNECTION无效而导致的ORM失败
            # if (current_time - db_ping_time).seconds >= mysql_ping_time_interval:
            #     logger.info("[lufax_grpc_server] close_old_connections {0}".format((current_time - db_ping_time).seconds))
            #     close_old_connections()
            #     db_ping_time = current_time

            time.sleep(30)
    except KeyboardInterrupt:
        grpc_server.stop()
