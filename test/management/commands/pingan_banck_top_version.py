# -*- coding:utf-8 -*-
# CREATED BY: shenhaiming
# CREATED ON: 2020/1/14
# LAST MODIFIED ON:
# AIM:

# demo call
# python manage.py pingan_bank_auth --username=system --password=oracle123 --instance=lumeta --address=172.168.71.55 --port=1521 --type=AVGTIME --count=100
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
from service.predict_table_strategy_oracle.Utility.Utility_Data_get_from_sql import GetSQL
from service.ai_sr_models import AISrViewedSql
import common.utils as utils
import common.common_time as common_time
from typing import List, Tuple
import requests
import json
from multiprocessing import Pool
import math
import datetime
import os
import timeit
import copy

API_ADDRESS = "127.0.0.1:8011"


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('-u', '--username', dest='username', default=False, help='username')
        parser.add_argument('-p', '--password', dest='password', default=False, help='password')
        parser.add_argument('-i', '--instance', dest='instance', default=False, help='instance')
        parser.add_argument('-a', '--address', dest='address', default=False, help='address')
        parser.add_argument('-o', '--port', dest='port', default=False, help='port')
        # parser.add_argument('-n', '--number', dest='number', default=False, help='number')
        # parser.add_argument('-l', '--parallel', dest='parallel', default=False, help='parallel')
        parser.add_argument('-t', '--type', dest='type', default=False, help='type include [AVGTIME | CPU | IO]')
        parser.add_argument('-c', '--count', dest='count', default=False, help='count in [1~1000]')

    def handle_share_pool(self, username, password, instance_name, host, port, batchnum, parallel_number):
        banksp = BankSharePoolHandle(username, password, instance_name, host, port, batch_number=batchnum)
        # banksp.bank_share_pool()
        total_num = banksp.get_count_num()
        remand_num = copy.copy(total_num)
        print(total_num)
        print(datetime.datetime.now())
        p = Pool(int(parallel_number))
        _t = multiprocessing.Manager().Value(int, remand_num)
        ret_list = []

        process_start = timeit.default_timer()
        for i in range(0, int(math.ceil(total_num / float(batchnum)))):  # 开7个任务
            ret = p.apply_async(banksp.bank_share_pool, args=(),
                                kwds={"limit": int(batchnum) * (i + 1),
                                      "offset": i * int(batchnum),
                                      "total_num": total_num,
                                      "remand_num":_t,
                                      "start_time": process_start})
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

    def handle_share_pool_top_version(self, username, password, instance_name, host, port, type, count):
        banksp = BankSharePoolHandle(username, password, instance_name, host, port)
        process_start = timeit.default_timer()
        result = banksp.bank_share_pool_top_version(type, count, process_start)

        if not result:
            return

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
        # batch_number = options.get('number')
        # parallel = options.get('parallel')
        type = options.get('type')
        count = int(options.get('count'))
        self.handle_share_pool_top_version(username, password, instance_name, address, port, type, count)


class BankSharePoolHandle:
    def __init__(self, bank_username, bank_password, bank_instance_name, bank_ip, bank_port):
        self._bank_username = bank_username
        self._bank_password = bank_password
        self._bank_instance_name = bank_instance_name
        self._bank_ip = bank_ip
        self._bank_port = bank_port

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

    def save_to_AISrViewedSql(self, sql_id: str, info: Handler_pingan, tab_names: List[str], sql_freq: float):
        try:
            ai_sr_viewed_sql = AISrViewedSql.objects.get(sql_id=sql_id)

        except AISrViewedSql.DoesNotExist:
            ai_sr_viewed_sql = AISrViewedSql()
            ai_sr_viewed_sql.updated_at = datetime.datetime.now()
            ai_sr_viewed_sql.updated_by = "sys"
            ai_sr_viewed_sql.created_at = datetime.datetime.now()
            ai_sr_viewed_sql.created_by = "sys"

            ai_sr_viewed_sql.sql_id = sql_id
            ai_sr_viewed_sql.instance_name = info.instance_name
            ai_sr_viewed_sql.schema_name = info.schema_name
            ai_sr_viewed_sql.table_names = json.dumps(tab_names)
            ai_sr_viewed_sql.sql_text = info.sematic_info.origin_sql
            ai_sr_viewed_sql.luparser = info.sematic_info.elements.dumps()
            ai_sr_viewed_sql.apperance = sql_freq
        except Exception as ex:
            raise Exception('AISrViewedSQL初始失败')

        old_freq = ai_sr_viewed_sql.apperance
        ai_sr_viewed_sql.apperance = max(old_freq, sql_freq)
        ai_sr_viewed_sql.save()

    def call_tab_strategy(self, sql_result: Tuple):
        """
        调用表策略核心算法
        :param sql_text:
        :param sql_frequency:
        :return:
        """
        sql_text = sql_result[0].read()
        sql_id = sql_result[1]
        exec_schema_name = sql_result[2]
        executions = sql_result[3]
        total_time = sql_result[4]
        avg_time = sql_result[5]
        first_load_time = sql_result[6]
        last_load_time = sql_result[7]

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
            raise Exception('executions 获取错误')

        try:
            Parser = LuSQLParser(sql_text)
        except:
            raise Exception('parser 解析异常')

        sql_statement = Parser.sql_statement()

        if sql_statement.upper() not in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
            return False

        table_names = Parser.get_table_name()
        if len(table_names) <= 0:
            return False

        table_name = table_names[0]
        owner_schema = self.get_oracle_owner_by_table_name(exec_schema_name, table_name, sql_statement)

        if len(owner_schema) <= 0:
            return False

        oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)
        oracle_sql_struct = OracleSQLStruct()
        oracle_sql_struct.sequence = utils.get_uuid().replace("-", "")
        oracle_sql_struct.tenant_code = "BANK"
        oracle_sql_struct.data_handle_result = False
        oracle_sql_struct.message = ""
        oracle_sql_struct.oracle_conn = oracle_handle
        oracle_sql_struct.schema_name = owner_schema.upper()
        oracle_sql_struct.sql_text = sql_text

        ora_ai_review = OracleAISQLReview(oracle_sql_struct)
        handle_result = ora_ai_review.handle_struct_data(need_explain_handle=False, need_histogram=False)
        if not handle_result:
            print("[{0}] handle failed".format(sql_text))
            return False

        collector = Extract()
        handle_bank = Handler_pingan(oracle_sql_struct, sql_freq=sql_frequency)
        # handler 会将信息打包,
        # 下一步将handle类传给collector
        collector.run(handle_bank)
        self.save_to_AISrViewedSql(sql_id=sql_id, info=handle_bank, tab_names=table_names, sql_freq=sql_frequency)
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

    def bank_share_pool(self, limit=None, offset=None, total_num=None, remand_num=None, start_time=None):

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
            if str(remand_num.value).isdigit():
                remand_num.value -= 1
                #print(remand_num.value)
            try:
                self.call_tab_strategy(row)
                cnt = total_num - remand_num.value
                process_end = timeit.default_timer()
                process_lapses = process_end - start_time
                average_cost = process_lapses / float(cnt + 1)
                process_remand_time = round((total_num - cnt) * average_cost, 2)
                print(f'{cnt:d}|{total_num:d},remand time:',
                      str(datetime.timedelta(seconds=process_remand_time)),
                      'average time consume: ', str(datetime.timedelta(seconds=average_cost)))
            except Exception as ex:
                print("exception => {0}".format(ex))

    def bank_share_pool_top_version(self, type="AVG", data_count=1, start_time=None):
        """
        银行需求，按TOP SQL进行处理
        :param total_num:
        :param type:
        :param data_count:
        :return:
        """
        try:
            datac = int(data_count)
            if datac < 1 or datac > 1000:
                print("parameter invalid [data_count={0}]".format(data_count))
                return False
        except Exception as ex:
            print("parameter invalid [data_count={0}]".format(data_count))
            return False

        if type.upper().strip() == "AVGTIME":
            share_pool_sql = """SELECT * FROM (
            SELECT SQL_FULLTEXT,SQL_ID, parsing_schema_name,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
            ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
            FROM V$SQL
            WHERE EXECUTIONS > 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
            ORDER BY avg_time DESC)
            WHERE ROWNUM<={0}            
            """.format(data_count)

        elif type.upper().strip() == "CPU":
            share_pool_sql = """SELECT * FROM (
            SELECT SQL_FULLTEXT,SQL_ID, parsing_schema_name,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
            ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
            FROM V$SQL
            WHERE EXECUTIONS > 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
            ORDER BY buffer_gets DESC)
            WHERE ROWNUM<={0}
            """.format(data_count)

        elif type.upper().strip() == "IO":
            share_pool_sql = """SELECT * FROM (
            SELECT SQL_FULLTEXT,SQL_ID, parsing_schema_name,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
            ELAPSED_TIME/nvl(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
            FROM V$SQL
            WHERE EXECUTIONS > 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
            ORDER BY disk_reads DESC)
            WHERE ROWNUM<={0}
            """.format(data_count)

        else:
            print("UNKNOWN SHARE POOL SELECT TYPE [{0}]".format(type))
            return False

        bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip, self._bank_port)

        bank_result = bank_oracle_handle.ora_execute_query_get_all_data(share_pool_sql)

        if not bank_result.result:
            return False

        i = 0
        for row in bank_result.data:

            i += 1
            try:
                self.call_tab_strategy(row)
                # cnt = data_count - i
                cnt = i
                process_end = timeit.default_timer()
                process_lapses = process_end - start_time
                average_cost = process_lapses / float(cnt + 1)
                process_remand_time = round((data_count - cnt) * average_cost, 2)
                print(f'{cnt:d}|{data_count:d},remand time:',
                      str(datetime.timedelta(seconds=process_remand_time)),
                      'average time consume: ', str(datetime.timedelta(seconds=average_cost)))
            except Exception as ex:
                print("exception => {0}".format(ex))

        return True