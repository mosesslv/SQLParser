# -*- coding: UTF-8 -*-

# demo call
# python manage.py pingan_bank_auth --username=system --password=oracle123 --instance=lumeta --address=172.168.71.55 --port=1521 --number=100
import multiprocessing

from django.core.management.base import BaseCommand
from common.OracleHandle import OracleHandle
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.predict_table_strategy_oracle.Table_Strate import Table_Strategy
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.predict_table_strategy_oracle.InfoHandler import Handler_pingan
from service.predict_table_strategy_oracle.InfoExtraction import Extract
from service.predict_table_strategy_oracle.StrategyBuilder import StrategyBuilder
# from service.AISQLReview.ai_process import OracleAISQLReview
from service.AISQLReview.ai_process_for_tab_strategy import OracleAISQLReview
import common.utils as utils
import common.common_time as common_time
import requests
import json
from multiprocessing import Pool
import math
import datetime

API_ADDRESS = "127.0.0.1:8011"


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('-u', '--username', dest='username', default=False, help='username')
        parser.add_argument('-p', '--password', dest='password', default=False, help='password')
        parser.add_argument('-i', '--instance', dest='instance', default=False, help='instance')
        parser.add_argument('-a', '--address', dest='address', default=False, help='address')
        parser.add_argument('-o', '--port', dest='port', default=False, help='port')
        parser.add_argument('-n', '--number', dest='number', default=False, help='number')
        parser.add_argument('-l', '--parallel', dest='parallel', default=False, help='parallel')

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

    def handle_share_pool(self, username, password, instance_name, host, port, batchnum, parallel_number):
        banksp = BankSharePoolHandle(username, password, instance_name, host, port, batch_number=batchnum)
        # banksp.bank_share_pool()
        total_num = banksp.get_count_num()
        print(total_num)
        print(datetime.datetime.now())
        p = Pool(int(parallel_number))
        _t = multiprocessing.Manager().Value(int, total_num)
        ret_list = []
        for i in range(0, int(math.ceil(total_num / float(batchnum)))):  # 开7个任务
            ret = p.apply_async(banksp.bank_share_pool, args=(), kwds={"limit": int(batchnum) * (i + 1), "offset": i * int(batchnum), "total_num": _t})
            ret_list.append(ret)
        p.close()
        p.join()
        for _ in ret_list:
            print(_.successful())
        print(datetime.datetime.now())

        try:
            print("=================== StrategyBuilder =======================")
            # builder 是个独立类不需要传惨
            builder = StrategyBuilder()
            builder.run()

        except Exception as ex:
            print("StrategyBuilder handle exception [{0}]".format(ex))

    def handle_share_pool_test(self, username, password, instance_name, host, port, batchnum):
        banksp = BankSharePoolHandle(username, password, instance_name, host, port, batch_number=batchnum)
        banksp.bank_share_pool(1000, 1)

        try:
            print("=================== StrategyBuilder =======================")
            # builder 是个独立类不需要传惨
            builder = StrategyBuilder()
            builder.run()

        except Exception as ex:
            print("StrategyBuilder handle exception [{0}]".format(ex))

    def handle(self, *args, **options):
        # import pdb;pdb.set_trace()
        username = options.get('username')
        password = options.get('password')
        instance_name = options.get('instance')
        address = options.get('address')
        port = options.get('port')
        batch_number = options.get('number')
        parallel = options.get('parallel')
        self.handle_share_pool(username, password, instance_name, address, port, batch_number, parallel)
        # self.handle_share_pool_test(username, password, instance_name, address, port, batch_number)


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

        bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)

        sql_result = bank_oracle_handle.ora_execute_query_get_all_data(find_sql_text)
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

    def call_tab_strategy(self, owner_schema_name, sql_text, sql_frequency):
        """
        调用表策略核心算法
        :param sql_text:
        :param sql_frequency:
        :return:
        """
        oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)
        oracle_sql_struct = OracleSQLStruct()
        oracle_sql_struct.sequence = utils.get_uuid().replace("-", "")
        oracle_sql_struct.tenant_code = "BANK"
        oracle_sql_struct.data_handle_result = False
        oracle_sql_struct.message = ""
        oracle_sql_struct.oracle_conn = oracle_handle
        oracle_sql_struct.schema_name = owner_schema_name.upper()
        oracle_sql_struct.sql_text = sql_text

        ora_ai_review = OracleAISQLReview(oracle_sql_struct)
        handle_result = ora_ai_review.handle_struct_data(need_explain_handle=False, need_histogram=False)
        if not handle_result:
            print("[{0}] handle failed".format(sql_text))
            return False

        collector = Extract()
        handle_bank = Handler_pingan(oracle_sql_struct)
        # handler 会将信息打包,
        # 下一步将handle类传给collector
        collector.run(handle_bank, sql_frequency)
        return True

    def get_count_num(self):
        count_text = """
                SELECT count(0) FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
                """
        bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)
        count = bank_oracle_handle.ora_execute_query_get_count(count_text)
        total_num = count.data[0] if len(count.data) > 0 else 0
        return total_num

    # def bank_share_pool(self, bank_ip, bank_port, bank_instance_name, bank_username, bank_password):
    def bank_share_pool(self, limit=None, offset=None, total_num=None):
        # ts = Table_Strategy()
        #         share_pool_sql = """
        #         select * from (
        # SELECT parsing_schema_name, SQL_FULLTEXT, SQL_TEXT,EXECUTIONS total_count,ELAPSED_TIME/1000 total_time,
        # ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time
        # FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
        # order by avg_time desc)
        # where rownum < {0}
        #         """.format(sql_number)
        #SELECT parsing_schema_name, SQL_FULLTEXT, SQL_ID, SQL_TEXT,EXECUTIONS total_count,ELAPSED_TIME/1000 total_time,
        #ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time
        #FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM') and rownum < {1} and rownum >= {2} order by SQL_ID asc

        bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)

        share_pool_sql = """
        SELECT * FROM (SELECT tt.*, ROWNUM AS rowno
        FROM (SELECT parsing_schema_name, SQL_FULLTEXT, SQL_ID, SQL_TEXT,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
        ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
        FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM') order by SQL_ID) tt 
        WHERE ROWNUM < {0}) table_alias WHERE table_alias.rowno >= {1}
                """.format(limit, offset)

        bank_result = bank_oracle_handle.ora_execute_query_get_all_data(share_pool_sql)

        if not bank_result.result:
            return

        i = 0
        for row in bank_result.data:
            i += 1
            if str(total_num.value).isdigit():
                total_num.value -= 1
                print(total_num.value)
            exec_schema_name = row[0]
            sql_text = row[1].read()
            sql_id = row[2]
            executions = row[4]
            first_load_time = row[7]
            last_load_time = row[8]

            print("======================= {0} -> [{1}] ==========================".format(i, sql_id))

            try:
                exec_num = int(executions)
                first_load_time_date = datetime.datetime.strptime(first_load_time, "%Y-%m-%d/%H:%M:%S")
                last_load_time_date = datetime.datetime.strptime(last_load_time, "%Y-%m-%d/%H:%M:%S")
                day_frequency = (last_load_time_date - first_load_time_date).days

                if day_frequency <= 0:
                    sql_frequency = 1.0
                else:
                    sql_frequency = float(exec_num) / float(day_frequency)

            except:
                continue

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

            try:
                self.call_tab_strategy(owner_schema, sql_text, sql_frequency=sql_frequency)
            except Exception as ex:
                print("[{0}] exception => {1}".format(sql_text, ex))

            # api_result, data = self.call_http_api(
            #     "oracle", self._bank_ip, self._bank_port, self._bank_instance_name,
            #     self._bank_username, self._bank_password, owner_schema, sql_text)
            #
            # if not api_result:
            #     print("HTTP failed")
            #     continue
            #
            # ai_recomm = data["AI_RECOMMEND"]
            #
            # # call index Strategy
            # ts.collect(ai_recomm)

            # out_str = "---------------------------- {4} [{5}]----------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n{2}\n\n{3}\n\n".format(
            #     sql_text, data["AI_RESULT"], data["AI_RECOMMEND"], data["MESSAGE"], i, sql_id)
            # print(out_str)

            # f = open('/tmp/bank_statistics.txt', 'a')
            # f.write(out_str)
            # f.close()
        # end for

        # try:
        #     tab_dict = ts.run()
        #     for key, value in tab_dict.items():
        #         buffer = key + '\n'
        #         for v in value:
        #             buffer += '\t' + v.__str__() + '\n'
        #
        #         print(buffer)
        #         with open('/tmp/bank_statistics.txt', 'a') as f:
        #             f.write(buffer)
        #
        #     ts.clear()
        # except Exception as ex:
        #     pass
