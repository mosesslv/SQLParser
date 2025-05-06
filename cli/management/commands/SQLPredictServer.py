# coding:utf-8

from django.core.management.base import BaseCommand
import service.grpc_channel.grpc_server
from django.conf import settings
import os
import logging
logger = logging.getLogger("grpc")


class Command(BaseCommand):
    # def add_arguments(self, parser):
    #     parser.add_argument('parameters', nargs='+', type=str)

    def handle(self, *args, **options):
        try:
            # argv = options['parameters']
            # listen_port = int(argv[0])
            # grpc_auth = argv[1]
            # grpc_code = argv[2]

            sql_predict_server_port = settings.SQLPREDICT_GRPC_PORT
            sql_predict_server_auth_string = settings.SQLPREDICT_GRPC_AUTH_STRING
            sql_predict_server_code = settings.SQLPREDICT_GRPC_AUTH_CODE

            service.grpc_channel.grpc_server.lufax_grpc_server(
                sql_predict_server_port, sql_predict_server_auth_string, sql_predict_server_code)
        except KeyboardInterrupt:
            logger.warning("[grpc] receive keyboard interrupt message")
            os._exit(1)
