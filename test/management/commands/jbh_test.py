# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2019/12/25 上午10:22
# LAST MODIFIED ON:
# AIM:

# -*- coding: UTF-8 -*-

from django.core.management.base import BaseCommand
from django.db import models
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.AISQLReview.ai_process import OracleAISQLReview
from service.AISQLReview.sql_abstract import OracleSQLStruct
from common.OracleHandle import OracleHandle
import service.AISQLReview.AIError as AIError
import common.utils as utils


class TestAiReview(models.Model):
    # 所有表的公共字段
    id = models.AutoField(primary_key=True)
    instance_name = models.CharField(max_length=128, null=False, blank=True)
    sql_text = models.TextField(help_text="")
    schema_name = models.CharField(max_length=128, null=False, blank=True)
    ai_result = models.CharField(max_length=128, null=False, blank=True)
    ai_recommend = models.TextField(help_text="")
    ai_message = models.CharField(max_length=1024, null=False, blank=True)

    class Meta:
        db_table = "test_ai_review"
        verbose_name = 'test_ai_review'
        verbose_name_plural = 'test_ai_review'


class SharePoolHandle:
    def __init__(self, username, password, instance_name, get_data_ip, port, exec_ai_ip, record_instance_name):
        self._username = username
        self._password = password
        self._instance_name = instance_name
        self._get_data_ip = get_data_ip
        self._port = port
        self._exec_ai_ip = exec_ai_ip
        self._record_instance_name = record_instance_name

    def get_oracle_owner_by_table_name(self, exec_schema_name, table_name, sql_statement):
        """
        通过表名查找表的OWNER
        :param table_name:
        :return: string
        """
        find_sql_text = """
        select * from (
select '', OWNER, TABLE_NAME from dba_tables where owner = '{0}'
union
select GRANTEE, OWNER, TABLE_NAME from dba_tab_privs where privilege = '{1}' and grantee = '{0}')
where TABLE_NAME = '{2}'
        """.format(exec_schema_name.upper(), sql_statement, table_name.upper())

        _bank_oracle_handle = OracleHandle(
            self._username, self._password, self._instance_name, self._exec_ai_ip, self._port)

        sql_result = _bank_oracle_handle.ora_execute_query_get_all_data(find_sql_text)
        if not sql_result.result:
            return ""

        owner = ""
        for row in sql_result.data:
            owner = row[1]
            break

        return owner

   # def bank_share_pool(self, bank_ip, bank_port, bank_instance_name, bank_username, bank_password):
    def handle_share_pool(self):
        print("============================== get [{0}] share pool sql =================================".format(
            self._instance_name))

        bank_oracle_handle = OracleHandle(
            self._username, self._password, self._instance_name, self._get_data_ip, self._port)

        share_pool_sql = """SELECT parsing_schema_name,SQL_ID,SQL_FULLTEXT,SQL_TEXT,EXECUTIONS total_count,ELAPSED_TIME/1000 total_time, 
ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time 
FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
order by avg_time desc"""

        bank_result = bank_oracle_handle.ora_execute_query_get_all_data(share_pool_sql)

        if not bank_result.result:
            return

        for row in bank_result.data:

            exec_schema_name = row[0]
            sql_id = row[1]
            sql_text = row[2].read()

            print("============================== handle sql id [{0}] =================================".format(sql_id))

            try:
                lup = LuSQLParser(sql_text)
            except Exception as ex:
                print(ex)
                continue

            sql_statement = lup.sql_statement()

            if sql_statement.upper() not in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
                continue

            table_names = lup.get_table_name()
            if len(table_names) <= 0:
                continue

            # table_name = table_names[0].replace("\"", "")
            table_name = table_names[0]

            owner_schema = self.get_oracle_owner_by_table_name(exec_schema_name, table_name, sql_statement)

            if len(owner_schema) <= 0:
                continue

            oracle_handle = OracleHandle(self._username, self._password, self._instance_name, self._exec_ai_ip, self._port)
            oracle_sql_struct = OracleSQLStruct()
            oracle_sql_struct.sequence = utils.get_uuid()
            oracle_sql_struct.tenant_code = "LUFAX"
            oracle_sql_struct.data_handle_result = False
            oracle_sql_struct.message = ""
            oracle_sql_struct.oracle_conn = oracle_handle
            oracle_sql_struct.schema_name = owner_schema.upper()
            oracle_sql_struct.sql_text = sql_text

            try:
                oracle_ai_sqlreview = OracleAISQLReview(oracle_sql_struct)
                ai_result = oracle_ai_sqlreview.sql_predict()
                type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)

                if isinstance(oracle_sql_struct.ai_recommend, tuple):
                    ai_recomm = oracle_sql_struct.ai_recommend[0]
                else:
                    ai_recomm = oracle_sql_struct.ai_recommend

                if not ai_result:
                    result = {
                        "AI_RESULT": oracle_sql_struct.ai_result,
                        "AI_RECOMMEND": ai_recomm,
                        "MESSAGE": desc_str
                    }
                else:
                    result = {
                        "AI_RESULT": oracle_sql_struct.ai_result,
                        "AI_RECOMMEND": ai_recomm,
                        "MESSAGE": desc_str
                    }

            except Exception as ex:
                ai_err_code = "AIErr_Oracle_99999"
                type_str, desc_str = AIError.get_error_type_description(ai_err_code)
                result = {
                    "AI_RESULT": "INVALID",
                    "AI_RECOMMEND": "",
                    "MESSAGE": desc_str
                }

            try:
                test_ai = TestAiReview()
                test_ai.instance_name = self._record_instance_name
                test_ai.sql_text = sql_text
                test_ai.ai_result = result["AI_RESULT"]
                test_ai.ai_recommend = result["AI_RECOMMEND"]
                test_ai.ai_message = result["MESSAGE"]

                test_ai.save()
            except:
                pass


            # out_str = "--------------------------------------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n{2}\n\n{3}\n\n".format(
            #     sql_text, data["AI_RESULT"], data["AI_RECOMMEND"], data["MESSAGE"])
            # print(out_str)
            # f = open('/tmp/test_crm.txt', 'a')
            # f.write(out_str)
            # f.close()

        print("============================== finished [{0}] =================================".format(self._instance_name))



class Command(BaseCommand):
    def handle(self, *args, **options):
        import pdb;pdb.set_trace()
        # username = args[0]
        # password = args[1]
        # instance_name = args[2]
        # get_data_ip = args[3]
        # port = args[4]
        # exec_ai_ip = args[5]
        # record_instance_name = args[6]

        # sph = SharePoolHandle(username, password, instance_name, get_data_ip, port, exec_ai_ip, record_instance_name)
        sph = SharePoolHandle("system", "sqlrush", "ods", "31.67.72.85", "1525", "31.67.72.85", "ods")
        sph.handle_share_pool()

