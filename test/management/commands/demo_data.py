# -*- coding: UTF-8 -*-

from django.core.management.base import BaseCommand
from service.ai_sr_models import AISrSQLDetail, AISrTask, AISrTaskSql
from common.DjangoMysqlHandle import DjMysqlHandle
from common.MysqlHandle import MysqlHandle
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.mysql_meta.mysql_meta_handle import MysqlMetaHandle
import json
import datetime
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
        self.handle_sqlreview_request_sql()


    def handle_single_sql(self):

        sql_text = "select * from ai_sr_sql_detail where db_type='ORACLE' order by id desc limit 1024"
        dj_my_conn = DjMysqlHandle()
        dj_result = dj_my_conn.mysql_execute_query_get_all_data(sql_text)

        # ai_detail_data = AISrSQLDetail.objects.filter(db_type="ORACLE").order_by("-id")

        count = 0
        for ai_detail in dj_result.data:
            ai_error_code = ai_detail[19]
            ai_result_sql = ai_detail[17]

            if ai_error_code in ["AIErr_Oracle_99999", "AIErr_Oracle_99998", "AIErr_Oracle_99997"]:
                continue

            ai_result_dict = json.loads(ai_result_sql)

            ai_data_result = ai_result_dict["AI_RESULT"]
            if ai_data_result not in ["PASS", "NOPASS"]:
                continue

            count += 1
            task_sql_total = 1
            task_sql_need_optimize = 0 if ai_data_result == "PASS" else 1
            ai_task_id = utils.get_uuid()

            ai_task = AISrTask()
            ai_task.created_at = datetime.datetime.now()
            ai_task.created_by = "sys"
            ai_task.updated_at = datetime.datetime.now()
            ai_task.updated_by = "sys"
            ai_task.task_id = ai_task_id
            ai_task.task_type = "SINGLE"
            ai_task.task_status = "SUCCESS"
            ai_task.task_message = ""
            ai_task.comment = ""
            ai_task.sql_total = task_sql_total
            ai_task.sql_need_optimize = task_sql_need_optimize
            ai_task.save()

            ai_task_sql = AISrTaskSql()
            ai_task_sql.created_at = datetime.datetime.now()
            ai_task_sql.created_by = "sys"
            ai_task_sql.updated_at = datetime.datetime.now()
            ai_task_sql.updated_by = "sys"

            ai_task_sql.task_id = ai_task_id
            ai_task_sql.task_type = "SINGLE"
            ai_task_sql.sql_id_alias = utils.get_uuid()
            ai_task_sql.sql_sequence = ai_detail[5]
            ai_task_sql.db_type = ai_detail[7]
            ai_task_sql.schema_name = ai_detail[9]
            ai_task_sql.sql_text = ai_detail[10]
            ai_task_sql.ref_id = ""
            ai_task_sql.ai_result = ai_result_dict["AI_RESULT"]
            ai_task_sql.ai_recommend = ai_result_dict["AI_RECOMMEND"]
            ai_task_sql.ai_message = ai_result_dict["MESSAGE"]
            ai_task_sql.ai_error_code = ai_error_code
            ai_task_sql.ai_error_type = ai_detail[20]
            ai_task_sql.save()

    def handle_sqlreview_request_sql(self):
        request_id = 59392      # 57313 59392

        ai_task_id = utils.get_uuid().replace("-", "")

        sql_text_sr_detail = """select b.sequence from (
select id from sr_review_detail where review_request_id={0}) a
left join sr_review_detail_extend b
on a.id = b.detail_id
        """.format(request_id)

        dbcm_handle = MysqlHandle(DBCM_DB_USERNAME, DBCM_DB_PASSWD, DBCM_DB_ADDRESS, port=DBCM_DB_PORT, database=DBCM_DB_DATABASE)
        # dj_my_conn = DjMysqlHandle()
        dj_result = dbcm_handle.mysql_execute_query_get_all_data(sql_text_sr_detail)

        count = 0
        task_sql_total = 0
        task_sql_need_optimize = 0

        ai_task = AISrTask()
        ai_task.created_at = datetime.datetime.now()
        ai_task.created_by = "sys"
        ai_task.updated_at = datetime.datetime.now()
        ai_task.updated_by = "sys"
        ai_task.task_id = ai_task_id
        ai_task.task_type = "GITREPOSITORY"
        ai_task.task_status = "SUCCESS"
        ai_task.task_message = ""
        ai_task.comment = ""
        ai_task.sql_total = task_sql_total
        ai_task.sql_need_optimize = task_sql_need_optimize
        ai_task.delete_mark = "1"
        ai_task.save()

        for row in dj_result.data:
            sequence = row[0]

            try:
                ai_detail = AISrSQLDetail.objects.get(sequence=sequence)
            except:
                continue

            ai_error_code = ai_detail.ai_error_code
            ai_result_sql = ai_detail.ai_result

            ai_result_dict = json.loads(ai_result_sql)

            ai_data_result = ai_result_dict["AI_RESULT"]
            if ai_data_result not in ["PASS", "NOPASS"]:
                continue

            try:
                ai_task_sql = AISrTaskSql.objects.get(sql_sequence=ai_detail.sequence)
                continue
            except:
                ai_task_sql = AISrTaskSql()

            count += 1
            task_sql_total += 1

            if ai_data_result != "PASS":
                task_sql_need_optimize += 1

            ai_task_sql.created_at = datetime.datetime.now()
            ai_task_sql.created_by = "sys"
            ai_task_sql.updated_at = datetime.datetime.now()
            ai_task_sql.updated_by = "sys"

            ai_task_sql.task_id = ai_task_id
            ai_task_sql.task_type = "GITREPOSITORY"
            ai_task_sql.sql_id_alias = utils.get_uuid()
            ai_task_sql.sql_sequence = ai_detail.sequence
            ai_task_sql.db_type = ai_detail.db_type
            ai_task_sql.schema_name = ai_detail.schema_name
            ai_task_sql.sql_text = ai_detail.sql_text
            ai_task_sql.ref_id = ""
            ai_task_sql.ai_result = ai_result_dict["AI_RESULT"]
            ai_task_sql.ai_recommend = ai_result_dict["AI_RECOMMEND"]
            ai_task_sql.ai_message = ai_result_dict["MESSAGE"]
            ai_task_sql.ai_error_code = ai_error_code
            ai_task_sql.ai_error_type = ai_detail.ai_error_type
            ai_task.delete_mark = "1"
            ai_task_sql.save()
        # end for

        ai_task.sql_total = task_sql_total
        ai_task.sql_need_optimize = task_sql_need_optimize
        ai_task.save()
