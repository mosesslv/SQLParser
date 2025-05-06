# -*- coding: UTF-8 -*-

# demo call
# python manage.py pingan_bank_auth --username=system --password=oracle123 --instance=lumeta --address=172.168.71.55 --port=1521 --number=100

from django.core.management.base import BaseCommand
from common.OracleHandle import OracleHandle
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.predict_table_strategy_oracle.Table_Strate import Table_Strategy
import requests
import json

API_ADDRESS = "127.0.0.1:8011"


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('-u', '--username', dest='username', default=False, help='username')
        parser.add_argument('-p', '--password', dest='password', default=False, help='password')
        parser.add_argument('-i', '--instance', dest='instance', default=False, help='instance')
        parser.add_argument('-a', '--address', dest='address', default=False, help='address')
        parser.add_argument('-o', '--port', dest='port', default=False, help='port')
        parser.add_argument('-n', '--number', dest='number', default=False, help='number')

    def call_http(self):
        review_url = "http://{0}/pallas/common/review".format(API_ADDRESS)

        body_json = {
            "db_type": "oracle",
            "host": "172.168.71.55",
            "port": "1521",
            "instance_name": "lumeta",
            "username": "system",
            "passpord": "oracle123",
            "schema_name": "bdcdata",
            "sql_text": "select * from bdc_fund_info where id > 1000"
        }

        api_result = requests.post(review_url, json=body_json)

        if api_result.status_code != 200:
            print("call api url failed [{0}]".format(api_result.status_code))
            return

        result = json.loads(api_result.text)
        if result["status"].lower() != "success":
            print("api handle failed [{0}]".format(result["status"]))
            return

        print(result["data"]["AI_RESULT"])
        print(result["data"]["AI_RECOMMEND"])
        print(result["data"]["MESSAGE"])

    def handle_share_pool(self, username, password, instance_name, host, port, batchnum):
        banksp = BankSharePoolHandle(username, password, instance_name, host, port, batch_number=batchnum)
        banksp.bank_share_pool()

    def handle(self, *args, **options):
        # import pdb;pdb.set_trace()
        username = options.get('username')
        password = options.get('password')
        instance_name = options.get('instance')
        address = options.get('address')
        port = options.get('port')
        batch_number = options.get('number')
        self.handle_share_pool(username, password, instance_name, address, port, batch_number)


class BankSharePoolHandle:
    def __init__(self, bank_username, bank_password, bank_instance_name, bank_ip, bank_port, batch_number):
        self._bank_username = bank_username
        self._bank_password = bank_password
        self._bank_instance_name = bank_instance_name
        self._bank_ip = bank_ip
        self._bank_port = bank_port
        self._batch_number = batch_number

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
        """.format(exec_schema_name.upper(), sql_statement.upper(), table_name.upper())

        _bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)

        sql_result = _bank_oracle_handle.ora_execute_query_get_all_data(find_sql_text)
        if not sql_result.result:
            return ""

        owner = ""
        for row in sql_result.data:
            owner = row[1]
            break

        return owner

    def call_http_api(self, db_type, host, port, instance_name, username, password, schema_name, sql_text):
        review_url = "http://{0}/pallas/common/review".format(API_ADDRESS)

        body_json = {
            "db_type": db_type,
            "host": host,
            "port": port,
            "instance_name": instance_name,
            "username": username,
            "passpord": password,
            "schema_name": schema_name,
            "sql_text": sql_text
        }

        api_result = requests.post(review_url, json=body_json)

        if api_result.status_code != 200:
            print("call api url failed [{0}]".format(api_result.status_code))
            return False, None

        result = json.loads(api_result.text)
        if result["status"].lower() != "success":
            print("api handle failed [{0}]".format(result["status"]))
            return False, None

        print(result["data"]["AI_RESULT"])
        print(result["data"]["AI_RECOMMEND"])
        print(result["data"]["MESSAGE"])

        return True, result["data"]

    # def bank_share_pool(self, bank_ip, bank_port, bank_instance_name, bank_username, bank_password):
    def bank_share_pool(self):
        ts = Table_Strategy()

        bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)

        #         share_pool_sql = """
        #         select * from (
        # SELECT parsing_schema_name, SQL_FULLTEXT, SQL_TEXT,EXECUTIONS total_count,ELAPSED_TIME/1000 total_time,
        # ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time
        # FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
        # order by avg_time desc)
        # where rownum < {0}
        #         """.format(sql_number)

        share_pool_sql = """
        SELECT parsing_schema_name, SQL_FULLTEXT, SQL_ID, SQL_TEXT,EXECUTIONS total_count,ELAPSED_TIME/1000 total_time, 
        ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time 
        FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM') and rownum < {0}
                """.format(self._batch_number)

        bank_result = bank_oracle_handle.ora_execute_query_get_all_data(share_pool_sql)

        if not bank_result.result:
            return

        i = 0
        for row in bank_result.data:
            exec_schema_name = row[0]
            sql_text = row[1].read()
            sql_id = row[2]

            try:
                lup = LuSQLParser(sql_text)
            except:
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

            api_result, data = self.call_http_api(
                "oracle", self._bank_ip, self._bank_port, self._bank_instance_name,
                self._bank_username, self._bank_password, owner_schema, sql_text)

            if not api_result:
                print("HTTP failed")
                continue

            ai_recomm = data["AI_RECOMMEND"]

            # call index Strategy
            ts.collect(ai_recomm)

            i += 1

            out_str = "---------------------------- {4} [{5}]----------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n{2}\n\n{3}\n\n".format(
                sql_text, data["AI_RESULT"], data["AI_RECOMMEND"], data["MESSAGE"], i, sql_id)
            print(out_str)

            f = open('/tmp/bank_statistics.txt', 'a')
            f.write(out_str)
            f.close()
        # end for

        table_stra = "---------------------------- TABLE STATISTICS ---------------------------- \n\n\n"
        f = open('/tmp/bank_statistics.txt', 'a')
        f.write(table_stra)
        f.close()

        try:
            tab_dict = ts.get_strategy()
            for key, value in tab_dict.items():
                buffer = key + '\n'
                for v in value:
                    buffer += '\t' + v.__str__() + '\n'

                print(buffer)
                with open('/tmp/bank_statistics.txt', 'a') as f:
                    f.write(buffer)

            ts.clear()
        except Exception as ex:
            pass
