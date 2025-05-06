# -*- coding: UTF-8 -*-

from django.core.management.base import BaseCommand
from common.DjangoMysqlHandle import DjMysqlHandle
from common.MysqlHandle import MysqlHandle
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.mysql_meta.mysql_meta_handle import MysqlMetaHandle
import json
import common.utils as utils

DBCM_DB_ADDRESS = "30.94.5.7"
DBCM_DB_PORT = 3308
DBCM_DB_DATABASE = "dbcm"
DBCM_DB_USERNAME = "dbcm"
DBCM_DB_PASSWD = "dbcmlufaxcom"


class Command(BaseCommand):

    def handle(self, *args, **options):
        """
        启动服务
        :return:
        """
        import pdb;pdb.set_trace()
        start_date = "20190801"
        end_date = "20190831"

        # self.handle_detail_id(391456)
        # self.batch_handle_request(start_date, end_date)

