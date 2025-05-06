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
from common.DjangoMysqlHandle import DjMysqlHandle
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.predict_table_strategy_oracle.InfoHandler import Handler_pingan
from service.predict_table_strategy_oracle.Collector import Extract
from service.predict_table_strategy_oracle.Builder import StrategyBuilder
from service.predict_table_strategy_oracle.SQLEvaluator import Eval
from service.predict_table_strategy_oracle.Reporter import Reporter
from service.AISQLReview.ai_process_for_tab_strategy import OracleAISQLReview
from service.ai_sr_models import AISrViewedSql, AISrOracleSharepoolSrc
from service.predict_table_strategy_oracle.Utility.DataHandle import OracleSharePoolHandle
import service.predict_table_strategy_oracle.Utility.htmlGenerator as htmlg
import common.utils as utils
from typing import List, Tuple
import requests
import json
import datetime
import os
import timeit

API_ADDRESS = "127.0.0.1:8011"

SQL_FREQ = {}


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
        parser.add_argument('-c', '--count', dest='count', default=False, help='count in [1~6000]')
        parser.add_argument('-f', '--flushsharepool', dest='flushsharepool', default="yes", help='flushsharepool')
        parser.add_argument('-s', '--snapshot', dest='snapshot', default="", help='use with --flushsharepool=no')
        parser.add_argument('-e', '--execaddr', dest='execaddr', default="", help='execaddr')

        parser.add_argument('-cl', '--cleantab', dest='cleantab', default='y', help='clean tabStrategy [ y | n ]')

    def handle_snapshot_v14(self, username, password, instance_name, host_exc, host_actual, port, type, count,
                            snapshot):
        verbose = {'AVGTIME': f'选择前{count}个平均运行时间最久的sql',
                   'CPU': f'选择前{count}个CPU占用最大的sql',
                   'IO': f'选择前{count}个IO最频繁的sql'}

        banksp = BankSharePoolHandle(username=username,
                                     password=password,
                                     instance_name=instance_name,
                                     ip_exec=host_exc,
                                     ip_actual=host_actual,
                                     port=port,
                                     clean=self.clean)
        process_start = timeit.default_timer()
        banksp.bank_share_pool_v14(type, count, process_start, snapshot)

        from service.predict_table_strategy_oracle import Utility
        try:
            print("=================== StrategyBuilder =======================")

            handle = OracleSharePoolHandle(username, password, instance_name, host_exc, host_actual, port, snapshot)
            # 修改输出流
            curtime = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
            Utility.print_screen_and_file(filename='/tmp/{0}_{1}_summary.txt'.format(instance_name,
                                                                                     curtime))

            html_text = htmlg.g_begining() + htmlg.g_css() + htmlg.g_head('报告')
            html_text += htmlg.g_verbose(verbose[type])
            try:
                # 构建索引策略
                builder_pack = StrategyBuilder(instance_name, html_text=html_text).run()

                try:

                    # 计算索引策略性能
                    eval_pack = Eval(handle, builder_pack).run()

                    try:
                        # 生成报告O(3n)
                        Reporter(builder_pack, eval_pack, SQL_FREQ).run()

                    except Exception as ex:
                        with open('/tmp/error_log.txt', 'a') as f:
                            f.write("[{0}] {1}\n".format('Reporter', str(ex)))
                except Exception as ex:
                    with open('/tmp/error_log.txt', 'a') as f:
                        f.write("[{0}] {1}\n".format('Eval', str(ex)))

            except Exception as ex:
                with open('/tmp/error_log.txt', 'a') as f:
                    f.write("[{0}] {1}\n".format('StrategyBuilder', str(ex)))
        except Exception as ex:
            with open('/tmp/error_log.txt', 'a') as f:
                f.write("[{0}] {1}\n".format('OracleSharePoolHandle', str(ex)))
        # 将输出刘改回来
        Utility.print_screen_only()

    def handle(self, *args, **options):

        username = options.get('username')
        password = options.get('password')
        instance_name = options.get('instance')
        address = options.get('address')
        port = options.get('port')
        # batch_number = options.get('number')
        # parallel = options.get('parallel')
        type = options.get('type')
        count = int(options.get('count'))
        flushsharepool = options.get('flushsharepool')
        snapshot = options.get('snapshot')
        execaddr = options.get('execaddr')

        # --
        self.clean = True if options.get('cleantab') == 'y' else False

        try:
            os.remove("/tmp/error_log.txt")
        except:
            pass

        if flushsharepool.upper() == "YES":
            print("================================= start collect sharepool =====================================")
            oh = OracleHandle(username, password, instance_name, address, port)
            osp2m = OracleSharePool2Mysql(oh)
            version = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            osp2m.handle_sharepool(version)
        else:
            version = snapshot

        print("============================== start handle sharepool snapshot data ==================================")
        self.handle_snapshot_v14(username=username,
                                 password=password,
                                 instance_name=instance_name,
                                 host_exc=execaddr,
                                 host_actual=address,
                                 port=port,
                                 type=type,
                                 count=count,
                                 snapshot=version)


class OracleSharePool2Mysql:
    """
    oracle sharepool 数据抽到mysql
    """

    def __init__(self, OraHandle):
        self._oracle_handle = OraHandle

    def handle_sharepool_v1(self, snapshot_version):
        # sharepool_sql_text = """select sql_id, sql_fulltext, sharable_mem, persistent_mem, runtime_mem, sorts,
        # loaded_versions, open_versions, users_opening, fetches, executions, end_of_fetch_count, users_executing,
        # loads, first_load_time, invalidations, parse_calls, disk_reads, direct_writes, buffer_gets,
        # application_wait_time, concurrency_wait_time, cluster_wait_time, user_io_wait_time, plsql_exec_time,
        # java_exec_time, rows_processed, command_type, optimizer_mode, optimizer_cost, optimizer_env_hash_value,
        # parsing_user_id, parsing_schema_id, parsing_schema_name, kept_versions, hash_value, old_hash_value,
        # plan_hash_value, child_number, module, module_hash, action, action_hash, serializable_aborts,
        # outline_category, cpu_time, elapsed_time, outline_sid, sqltype, remote, object_status, literal_hash_value,
        # last_load_time, is_obsolete, child_latch, sql_profile, program_id, parsing_schema_name
        # from v$sql where parsing_schema_name not in ('SYS', 'SYSTEM')
        # """

        sharepool_sql_text = """select sql_id, sql_fulltext, sharable_mem, persistent_mem, runtime_mem, sorts, 
                loaded_versions, open_versions, users_opening, fetches, executions, end_of_fetch_count, users_executing, 
                loads, first_load_time, invalidations, parse_calls, disk_reads, direct_writes, buffer_gets, 
                application_wait_time, concurrency_wait_time, cluster_wait_time, user_io_wait_time, plsql_exec_time, 
                java_exec_time, rows_processed, command_type, optimizer_mode, optimizer_cost, optimizer_env_hash_value, 
                parsing_user_id, parsing_schema_id, parsing_schema_name, kept_versions, hash_value, old_hash_value, 
                plan_hash_value, child_number, module, module_hash, action, action_hash, serializable_aborts,
                outline_category, cpu_time, elapsed_time, outline_sid, sqltype, remote, object_status, literal_hash_value, 
                last_load_time, is_obsolete, child_latch, sql_profile, program_id, parsing_schema_name
                from v$sql where parsing_schema_name not in ('SYS', 'SYSTEM')
                """

        instance_url = "{0}:{1}/{2} {3}/{4}".format(self._oracle_handle.get_address(),
                                                    self._oracle_handle.get_port(),
                                                    self._oracle_handle.get_instance_name(),
                                                    self._oracle_handle.get_username(),
                                                    self._oracle_handle.get_passwd())

        sharepool_sql_result = self._oracle_handle.ora_execute_query_get_all_data(sharepool_sql_text)

        if not sharepool_sql_result.result:
            print("sharepool sql running failed ......")
            return False

        i = 0
        total = len(sharepool_sql_result.data)
        for row in sharepool_sql_result.data:
            i += 1
            print("========================== handle {0}/{1} ==================================".format(i, total))
            sql_id = "" if row[0] is None else row[0]
            sql_fulltext = row[1].read()
            sharable_mem = 0 if row[2] is None else row[2]
            persistent_mem = 0 if row[3] is None else row[3]
            runtime_mem = 0 if row[4] is None else row[4]
            sorts = 0 if row[5] is None else row[5]
            loaded_versions = 0 if row[6] is None else row[6]
            open_versions = 0 if row[7] is None else row[7]
            users_opening = 0 if row[8] is None else row[8]
            fetches = 0 if row[9] is None else row[9]
            executions = 0 if row[10] is None else row[10]
            end_of_fetch_count = 0 if row[11] is None else row[11]
            users_executing = 0 if row[12] is None else row[12]
            loads = 0 if row[13] is None else row[13]
            first_load_time = row[14]
            invalidations = 0 if row[15] is None else row[15]
            parse_calls = 0 if row[16] is None else row[16]
            disk_reads = 0 if row[17] is None else row[17]
            direct_writes = 0 if row[18] is None else row[18]
            buffer_gets = 0 if row[19] is None else row[19]
            application_wait_time = 0 if row[20] is None else row[20]
            concurrency_wait_time = 0 if row[21] is None else row[21]
            cluster_wait_time = 0 if row[22] is None else row[22]
            user_io_wait_time = 0 if row[23] is None else row[23]
            plsql_exec_time = 0 if row[24] is None else row[24]
            java_exec_time = 0 if row[25] is None else row[25]
            rows_processed = 0 if row[26] is None else row[26]
            command_type = row[27]
            optimizer_mode = row[28]
            optimizer_cost = 0 if row[29] is None else row[29]
            optimizer_env_hash_value = row[30]
            parsing_user_id = row[31]
            parsing_schema_id = row[32]
            parsing_schema_name = row[33]
            kept_versions = row[34]
            hash_value = row[35]
            old_hash_value = row[36]
            plan_hash_value = row[37]
            child_number = row[38]
            module = row[39]
            module_hash = row[40]
            action = row[41]
            action_hash = row[42]
            serializable_aborts = row[43]
            outline_category = row[44]
            cpu_time = 0 if row[45] is None else row[45]
            elapsed_time = 0 if row[46] is None else row[46]
            outline_sid = row[47]
            sqltype = row[48]
            remote = row[49]
            object_status = row[50]
            literal_hash_value = row[51]
            last_load_time = row[52]
            is_obsolete = row[53]
            child_latch = row[54]
            sql_profile = row[55]
            program_id = row[56]
            parsing_schema_name = row[57]

            try:
                sharepool_data = AISrOracleSharepoolSrc.objects.get(
                    instance_url=instance_url, snapshot=snapshot_version, sql_id=sql_id)

                data_is_exists = True
            except AISrOracleSharepoolSrc.DoesNotExist:
                sharepool_data = AISrOracleSharepoolSrc()
                sharepool_data.created_by = "sys"
                sharepool_data.created_at = datetime.datetime.now()
                sharepool_data.updated_by = "sys"
                sharepool_data.updated_at = datetime.datetime.now()

                data_is_exists = False
            except Exception as ex:
                print("[{0}] get AISrOracleSharepoolSrc exception [{1}]".format(sql_id, ex))
                continue

            if data_is_exists:
                if sharable_mem > sharepool_data.sharable_mem:
                    sharepool_data.sharable_mem = sharable_mem

                if persistent_mem > sharepool_data.persistent_mem:
                    sharepool_data.persistent_mem = persistent_mem

                if runtime_mem > sharepool_data.runtime_mem:
                    sharepool_data.runtime_mem = runtime_mem

                if fetches > sharepool_data.fetches:
                    sharepool_data.fetches = fetches

                if end_of_fetch_count > sharepool_data.end_of_fetch_count:
                    sharepool_data.end_of_fetch_count = end_of_fetch_count

                if loads > sharepool_data.loads:
                    sharepool_data.loads = loads

                if parse_calls > sharepool_data.parse_calls:
                    sharepool_data.parse_calls = parse_calls

                if disk_reads > sharepool_data.disk_reads:
                    sharepool_data.disk_reads = disk_reads

                if buffer_gets > sharepool_data.buffer_gets:
                    sharepool_data.buffer_gets = buffer_gets

                if application_wait_time > sharepool_data.application_wait_time:
                    sharepool_data.application_wait_time = application_wait_time

                if concurrency_wait_time > sharepool_data.concurrency_wait_time:
                    sharepool_data.concurrency_wait_time = concurrency_wait_time

                if user_io_wait_time > sharepool_data.user_io_wait_time:
                    sharepool_data.user_io_wait_time = user_io_wait_time

                if plsql_exec_time > sharepool_data.plsql_exec_time:
                    sharepool_data.plsql_exec_time = plsql_exec_time

                if java_exec_time > sharepool_data.java_exec_time:
                    sharepool_data.java_exec_time = java_exec_time

                if rows_processed > sharepool_data.rows_processed:
                    sharepool_data.rows_processed = rows_processed

                if optimizer_cost > sharepool_data.optimizer_cost:
                    sharepool_data.optimizer_cost = optimizer_cost

                if cpu_time > sharepool_data.cpu_time:
                    sharepool_data.cpu_time = cpu_time

                if elapsed_time > sharepool_data.elapsed_time:
                    sharepool_data.elapsed_time = elapsed_time

                try:
                    exec_num = int(executions)
                    first_load_time_date = datetime.datetime.strptime(first_load_time, "%Y-%m-%d/%H:%M:%S")
                    last_load_time_date = datetime.datetime.strptime(last_load_time, "%Y-%m-%d/%H:%M:%S")
                    day_frequency = (last_load_time_date - first_load_time_date).days

                    if day_frequency <= 0:
                        sql_frequency = float(exec_num) / 1.0
                    else:
                        sql_frequency = float(exec_num) / float(day_frequency)

                    exec_num_old = int(sharepool_data.executions)
                    first_load_time_date_old = datetime.datetime.strptime(sharepool_data.first_load_time,
                                                                          "%Y-%m-%d/%H:%M:%S")
                    last_load_time_date_old = datetime.datetime.strptime(sharepool_data.last_load_time,
                                                                         "%Y-%m-%d/%H:%M:%S")
                    day_frequency_old = (last_load_time_date_old - first_load_time_date_old).days

                    if day_frequency_old <= 0:
                        sql_frequency_old = float(exec_num_old) / 1.0
                    else:
                        sql_frequency_old = float(exec_num_old) / float(day_frequency)

                    if sql_frequency_old > sql_frequency:
                        sharepool_data.executions = executions
                        sharepool_data.first_load_time = first_load_time
                        sharepool_data.last_load_time = last_load_time
                except:
                    continue

                sharepool_data.save()

            else:
                sharepool_data.instance_url = instance_url
                sharepool_data.snapshot = snapshot_version
                sharepool_data.sql_id = sql_id
                sharepool_data.sql_text = sql_fulltext
                sharepool_data.sharable_mem = sharable_mem
                sharepool_data.persistent_mem = persistent_mem
                sharepool_data.runtime_mem = runtime_mem
                sharepool_data.sorts = sorts
                sharepool_data.loaded_versions = loaded_versions
                sharepool_data.open_versions = open_versions
                sharepool_data.users_opening = users_opening
                sharepool_data.fetches = fetches
                sharepool_data.executions = executions
                sharepool_data.end_of_fetch_count = end_of_fetch_count
                sharepool_data.loads = loads
                sharepool_data.first_load_time = first_load_time
                sharepool_data.invalidations = invalidations
                sharepool_data.parse_calls = parse_calls
                sharepool_data.disk_reads = disk_reads
                sharepool_data.buffer_gets = buffer_gets
                sharepool_data.application_wait_time = application_wait_time
                sharepool_data.concurrency_wait_time = concurrency_wait_time
                sharepool_data.cluster_wait_time = cluster_wait_time
                sharepool_data.user_io_wait_time = user_io_wait_time
                sharepool_data.plsql_exec_time = plsql_exec_time
                sharepool_data.java_exec_time = java_exec_time
                sharepool_data.rows_processed = rows_processed
                sharepool_data.command_type = command_type
                sharepool_data.optimizer_mode = optimizer_mode
                sharepool_data.optimizer_cost = optimizer_cost
                sharepool_data.optimizer_env_hash_value = optimizer_env_hash_value
                sharepool_data.parsing_user_id = parsing_user_id
                sharepool_data.parsing_schema_id = parsing_schema_id
                sharepool_data.kept_versions = kept_versions
                sharepool_data.hash_value = hash_value
                sharepool_data.old_hash_value = old_hash_value
                sharepool_data.plan_hash_value = plan_hash_value
                sharepool_data.child_number = child_number
                sharepool_data.module = module
                sharepool_data.module_hash = module_hash
                sharepool_data.action = action
                sharepool_data.action_hash = action_hash
                sharepool_data.serializable_aborts = serializable_aborts
                sharepool_data.outline_category = outline_category
                sharepool_data.cpu_time = cpu_time
                sharepool_data.elapsed_time = elapsed_time
                sharepool_data.outline_sid = outline_sid
                sharepool_data.sqltype = sqltype
                sharepool_data.remote = remote
                sharepool_data.object_status = object_status
                sharepool_data.literal_hash_value = literal_hash_value
                sharepool_data.last_load_time = last_load_time
                sharepool_data.is_obsolete = is_obsolete
                sharepool_data.child_latch = child_latch
                sharepool_data.sql_profile = sql_profile
                sharepool_data.program_id = program_id
                sharepool_data.parsing_schema_name = parsing_schema_name

                sharepool_data.save()

    def handle_sharepool(self, snapshot_version):
        total_sql_text = "select count(*) from v$sql where parsing_schema_name not in ('SYS', 'SYSTEM')"
        total_sql_result = self._oracle_handle.ora_execute_query_get_all_data(total_sql_text)

        if not total_sql_result.result:
            print("get total sharepool failed")
            return False

        total = total_sql_result.data[0][0]

        sharepool_sql_text = """select sql_id, sql_fulltext, sharable_mem, persistent_mem, runtime_mem, sorts, 
                loaded_versions, open_versions, users_opening, fetches, executions, end_of_fetch_count, users_executing, 
                loads, first_load_time, invalidations, parse_calls, disk_reads, direct_writes, buffer_gets, 
                application_wait_time, concurrency_wait_time, cluster_wait_time, user_io_wait_time, plsql_exec_time, 
                java_exec_time, rows_processed, command_type, optimizer_mode, optimizer_cost, optimizer_env_hash_value, 
                parsing_user_id, parsing_schema_id, parsing_schema_name, kept_versions, hash_value, old_hash_value, 
                plan_hash_value, child_number, module, module_hash, action, action_hash, serializable_aborts,
                outline_category, cpu_time, elapsed_time, outline_sid, sqltype, remote, object_status, literal_hash_value, 
                last_load_time, is_obsolete, child_latch, sql_profile, program_id, parsing_schema_name
                from v$sql where parsing_schema_name not in ('SYS', 'SYSTEM')
                """

        instance_url = "{0}:{1}/{2} {3}/{4}".format(self._oracle_handle.get_address(),
                                                    self._oracle_handle.get_port(),
                                                    self._oracle_handle.get_instance_name(),
                                                    self._oracle_handle.get_username(),
                                                    self._oracle_handle.get_passwd())

        _db_cursor = self._oracle_handle.connection.cursor()
        exec_result = _db_cursor.execute(sharepool_sql_text)

        i = 0

        try:
            while True:
                try:
                    row = exec_result.fetchone()
                except:
                    continue

                if not row:
                    break
                else:
                    i += 1

                    if i % 100 == 0:
                        print("========================== handle {0}/{1} ==================================".
                              format(i, total))

                    sql_id = "" if row[0] is None else row[0]
                    sql_fulltext = row[1].read()
                    sharable_mem = 0 if row[2] is None else row[2]
                    persistent_mem = 0 if row[3] is None else row[3]
                    runtime_mem = 0 if row[4] is None else row[4]
                    sorts = 0 if row[5] is None else row[5]
                    loaded_versions = 0 if row[6] is None else row[6]
                    open_versions = 0 if row[7] is None else row[7]
                    users_opening = 0 if row[8] is None else row[8]
                    fetches = 0 if row[9] is None else row[9]
                    executions = 0 if row[10] is None else row[10]
                    end_of_fetch_count = 0 if row[11] is None else row[11]
                    users_executing = 0 if row[12] is None else row[12]
                    loads = 0 if row[13] is None else row[13]
                    first_load_time = row[14]
                    invalidations = 0 if row[15] is None else row[15]
                    parse_calls = 0 if row[16] is None else row[16]
                    disk_reads = 0 if row[17] is None else row[17]
                    direct_writes = 0 if row[18] is None else row[18]
                    buffer_gets = 0 if row[19] is None else row[19]
                    application_wait_time = 0 if row[20] is None else row[20]
                    concurrency_wait_time = 0 if row[21] is None else row[21]
                    cluster_wait_time = 0 if row[22] is None else row[22]
                    user_io_wait_time = 0 if row[23] is None else row[23]
                    plsql_exec_time = 0 if row[24] is None else row[24]
                    java_exec_time = 0 if row[25] is None else row[25]
                    rows_processed = 0 if row[26] is None else row[26]
                    command_type = row[27]
                    optimizer_mode = row[28]
                    optimizer_cost = 0 if row[29] is None else row[29]
                    optimizer_env_hash_value = row[30]
                    parsing_user_id = row[31]
                    parsing_schema_id = row[32]
                    parsing_schema_name = row[33]
                    kept_versions = row[34]
                    hash_value = row[35]
                    old_hash_value = row[36]
                    plan_hash_value = row[37]
                    child_number = row[38]
                    module = row[39]
                    module_hash = row[40]
                    action = row[41]
                    action_hash = row[42]
                    serializable_aborts = row[43]
                    outline_category = row[44]
                    cpu_time = 0 if row[45] is None else row[45]
                    elapsed_time = 0 if row[46] is None else row[46]
                    outline_sid = row[47]
                    sqltype = row[48]
                    remote = row[49]
                    object_status = row[50]
                    literal_hash_value = row[51]
                    last_load_time = row[52]
                    is_obsolete = row[53]
                    child_latch = row[54]
                    sql_profile = row[55]
                    program_id = row[56]
                    parsing_schema_name = row[57]

                    try:
                        sharepool_data = AISrOracleSharepoolSrc.objects.get(
                            instance_url=instance_url, snapshot=snapshot_version, sql_id=sql_id)

                        data_is_exists = True
                    except AISrOracleSharepoolSrc.DoesNotExist:
                        sharepool_data = AISrOracleSharepoolSrc()
                        sharepool_data.created_by = "sys"
                        sharepool_data.created_at = datetime.datetime.now()
                        sharepool_data.updated_by = "sys"
                        sharepool_data.updated_at = datetime.datetime.now()

                        data_is_exists = False
                    except Exception as ex:
                        print("[{0}] get AISrOracleSharepoolSrc exception [{1}]".format(sql_id, ex))
                        continue

                    if data_is_exists:
                        if sharable_mem > sharepool_data.sharable_mem:
                            sharepool_data.sharable_mem = sharable_mem

                        if persistent_mem > sharepool_data.persistent_mem:
                            sharepool_data.persistent_mem = persistent_mem

                        if runtime_mem > sharepool_data.runtime_mem:
                            sharepool_data.runtime_mem = runtime_mem

                        if fetches > sharepool_data.fetches:
                            sharepool_data.fetches = fetches

                        if end_of_fetch_count > sharepool_data.end_of_fetch_count:
                            sharepool_data.end_of_fetch_count = end_of_fetch_count

                        if loads > sharepool_data.loads:
                            sharepool_data.loads = loads

                        if parse_calls > sharepool_data.parse_calls:
                            sharepool_data.parse_calls = parse_calls

                        if disk_reads > sharepool_data.disk_reads:
                            sharepool_data.disk_reads = disk_reads

                        if buffer_gets > sharepool_data.buffer_gets:
                            sharepool_data.buffer_gets = buffer_gets

                        if application_wait_time > sharepool_data.application_wait_time:
                            sharepool_data.application_wait_time = application_wait_time

                        if concurrency_wait_time > sharepool_data.concurrency_wait_time:
                            sharepool_data.concurrency_wait_time = concurrency_wait_time

                        if user_io_wait_time > sharepool_data.user_io_wait_time:
                            sharepool_data.user_io_wait_time = user_io_wait_time

                        if plsql_exec_time > sharepool_data.plsql_exec_time:
                            sharepool_data.plsql_exec_time = plsql_exec_time

                        if java_exec_time > sharepool_data.java_exec_time:
                            sharepool_data.java_exec_time = java_exec_time

                        if rows_processed > sharepool_data.rows_processed:
                            sharepool_data.rows_processed = rows_processed

                        if optimizer_cost > sharepool_data.optimizer_cost:
                            sharepool_data.optimizer_cost = optimizer_cost

                        if cpu_time > sharepool_data.cpu_time:
                            sharepool_data.cpu_time = cpu_time

                        if elapsed_time > sharepool_data.elapsed_time:
                            sharepool_data.elapsed_time = elapsed_time

                        try:
                            exec_num = int(executions)
                            first_load_time_date = datetime.datetime.strptime(first_load_time, "%Y-%m-%d/%H:%M:%S")
                            last_load_time_date = datetime.datetime.strptime(last_load_time, "%Y-%m-%d/%H:%M:%S")
                            day_frequency = (last_load_time_date - first_load_time_date).days

                            if day_frequency <= 0:
                                sql_frequency = float(exec_num) / 1.0
                            else:
                                sql_frequency = float(exec_num) / float(day_frequency)

                            exec_num_old = int(sharepool_data.executions)
                            first_load_time_date_old = datetime.datetime.strptime(sharepool_data.first_load_time,
                                                                                  "%Y-%m-%d/%H:%M:%S")
                            last_load_time_date_old = datetime.datetime.strptime(sharepool_data.last_load_time,
                                                                                 "%Y-%m-%d/%H:%M:%S")
                            day_frequency_old = (last_load_time_date_old - first_load_time_date_old).days

                            if day_frequency_old <= 0:
                                sql_frequency_old = float(exec_num_old) / 1.0
                            else:
                                sql_frequency_old = float(exec_num_old) / float(day_frequency)

                            if sql_frequency_old > sql_frequency:
                                sharepool_data.executions = executions
                                sharepool_data.first_load_time = first_load_time
                                sharepool_data.last_load_time = last_load_time
                        except:
                            continue

                        sharepool_data.save()

                    else:
                        sharepool_data.instance_url = instance_url
                        sharepool_data.snapshot = snapshot_version
                        sharepool_data.sql_id = sql_id
                        sharepool_data.sql_text = sql_fulltext
                        sharepool_data.sharable_mem = sharable_mem
                        sharepool_data.persistent_mem = persistent_mem
                        sharepool_data.runtime_mem = runtime_mem
                        sharepool_data.sorts = sorts
                        sharepool_data.loaded_versions = loaded_versions
                        sharepool_data.open_versions = open_versions
                        sharepool_data.users_opening = users_opening
                        sharepool_data.fetches = fetches
                        sharepool_data.executions = executions
                        sharepool_data.end_of_fetch_count = end_of_fetch_count
                        sharepool_data.loads = loads
                        sharepool_data.first_load_time = first_load_time
                        sharepool_data.invalidations = invalidations
                        sharepool_data.parse_calls = parse_calls
                        sharepool_data.disk_reads = disk_reads
                        sharepool_data.buffer_gets = buffer_gets
                        sharepool_data.application_wait_time = application_wait_time
                        sharepool_data.concurrency_wait_time = concurrency_wait_time
                        sharepool_data.cluster_wait_time = cluster_wait_time
                        sharepool_data.user_io_wait_time = user_io_wait_time
                        sharepool_data.plsql_exec_time = plsql_exec_time
                        sharepool_data.java_exec_time = java_exec_time
                        sharepool_data.rows_processed = rows_processed
                        sharepool_data.command_type = command_type
                        sharepool_data.optimizer_mode = optimizer_mode
                        sharepool_data.optimizer_cost = optimizer_cost
                        sharepool_data.optimizer_env_hash_value = optimizer_env_hash_value
                        sharepool_data.parsing_user_id = parsing_user_id
                        sharepool_data.parsing_schema_id = parsing_schema_id
                        sharepool_data.kept_versions = kept_versions
                        sharepool_data.hash_value = hash_value
                        sharepool_data.old_hash_value = old_hash_value
                        sharepool_data.plan_hash_value = plan_hash_value
                        sharepool_data.child_number = child_number
                        sharepool_data.module = module
                        sharepool_data.module_hash = module_hash
                        sharepool_data.action = action
                        sharepool_data.action_hash = action_hash
                        sharepool_data.serializable_aborts = serializable_aborts
                        sharepool_data.outline_category = outline_category
                        sharepool_data.cpu_time = cpu_time
                        sharepool_data.elapsed_time = elapsed_time
                        sharepool_data.outline_sid = outline_sid
                        sharepool_data.sqltype = sqltype
                        sharepool_data.remote = remote
                        sharepool_data.object_status = object_status
                        sharepool_data.literal_hash_value = literal_hash_value
                        sharepool_data.last_load_time = last_load_time
                        sharepool_data.is_obsolete = is_obsolete
                        sharepool_data.child_latch = child_latch
                        sharepool_data.sql_profile = sql_profile
                        sharepool_data.program_id = program_id
                        sharepool_data.parsing_schema_name = parsing_schema_name

                        sharepool_data.save()
            # end while
            # print("========================== handle {0}/{1} ==================================".format(i, total))

        except:
            pass
        finally:
            _db_cursor.close()
            return True


class BankSharePoolHandle:
    def __init__(self, username, password, instance_name, ip_exec, ip_actual, port, clean):
        self._bank_username = username
        self._bank_password = password
        self._bank_instance_name = instance_name
        self._bank_ip_exec = ip_exec
        self._bank_ip_actual = ip_actual
        self._bank_port = port
        self.clean = clean

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
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip_exec, self._bank_port)

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
        sql_text = sql_result[0]
        sql_id = sql_result[1]
        exec_schema_name = sql_result[2]
        executions = sql_result[3]
        total_time = sql_result[4]
        avg_time = sql_result[5]
        first_load_time = sql_result[6]
        last_load_time = sql_result[7]
        
        # ---- add schema name --- #
        import service.sql_parser_graph.SQLParser as parser
        sp = parser.SQLParser(sql_text)
        sql_text = sp.add_schema_name(exec_schema_name)
        # --- #

        try:
            exec_num = int(executions)
            first_load_time_date = datetime.datetime.strptime(first_load_time, "%Y-%m-%d/%H:%M:%S")
            last_load_time_date = datetime.datetime.strptime(last_load_time, "%Y-%m-%d/%H:%M:%S")
            day_frequency = (last_load_time_date - first_load_time_date).days

            # from termcolor import colored
            # print(colored(exec_num, 'yellow'))
            # print(colored(sql_id, 'yellow'))
            #
            # if exec_num > 100000:
            #     import pdb
            #     pdb.set_trace()

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
            # print('not sql')
            return False

        table_names = Parser.get_table_name()
        if len(table_names) <= 0:
            # print('cannot find table name')
            return False

        table_name = table_names[0]
        owner_schema = self.get_oracle_owner_by_table_name(exec_schema_name, table_name, sql_statement)

        if len(owner_schema) <= 0:
            # print('schema not exist')
            return False

        oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip_actual, self._bank_port)
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
            # print("[{0}] handle failed".format(sql_text))
            return False

        collector = Extract()
        handle_bank = Handler_pingan(oracle_sql_struct, sql_freq=sql_frequency, sql_id=sql_id)
        SQL_FREQ[sql_id] = sql_frequency
        # handler 会将信息打包,
        # 下一步将handle类传给collector
        try:
            collector.run(handle_bank)
        except Exception as es:
            # print('collector　报错 ', es)
            # print(str(es))
            # if "must" in str(es).split():
            #     print('I am in XXXXXXXXXXXXXXXXXXXXX')
            #     import pdb
            #     pdb.set_trace()
            #     collector.run(handle_bank)
            pass
        # self.save_to_AISrViewedSql(sql_id=sql_id, info=handle_bank, tab_names=table_names, sql_freq=sql_frequency)
        return True

    def get_count_num(self):
        count_text = """
                SELECT count(0) FROM v$SQL where EXECUTIONS <> 0 and parsing_schema_name not in ('SYS', 'SYSTEM')
                """
        bank_oracle_handle = OracleHandle(
            self._bank_username, self._bank_password, self._bank_instance_name, self._bank_ip_actual, self._bank_port)
        count = bank_oracle_handle.ora_execute_query_get_count(count_text)
        total_num = count.data[0] if len(count.data) > 0 else 0
        return total_num

    def bank_share_pool_v14(self, type="AVG", data_count=1, start_time=None, snapshot=""):
        """
        银行需求，按TOP SQL进行处理
        :param total_num:
        :param type:
        :param data_count:
        :return:
        """
        if self.clean:
            Extract().truncate_tab()

        col_url = "{0}:{1}/{2} {3}/{4}".format(self._bank_ip_actual, self._bank_port, self._bank_instance_name,
                                               self._bank_username, self._bank_password)

        try:
            datac = int(data_count)
            if datac < 1 or datac > 6000:
                print("parameter invalid [data_count={0}]".format(data_count))
                return False
        except Exception as ex:
            print("parameter invalid [data_count={0}]".format(data_count))
            return False

        if type.upper().strip() == "AVGTIME":
            share_pool_sql = """SELECT SQL_TEXT,SQL_ID, parsing_schema_name,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
ELAPSED_TIME/ifnull(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
FROM ai_sr_oracle_sharepool_src
WHERE instance_url='{1}' and snapshot='{2}' and parsing_schema_name not in ('SYS', 'SYSTEM')
ORDER BY avg_time DESC
limit {0}       
            """.format(data_count, col_url, snapshot)

        elif type.upper().strip() == "CPU":
            share_pool_sql = """SELECT SQL_TEXT,SQL_ID, parsing_schema_name,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
ELAPSED_TIME/ifnull(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
FROM ai_sr_oracle_sharepool_src
WHERE instance_url='{1}' and snapshot='{2}' and parsing_schema_name not in ('SYS', 'SYSTEM')
ORDER BY buffer_gets DESC
limit {0}
            """.format(data_count, col_url, snapshot)

        elif type.upper().strip() == "IO":
            share_pool_sql = """SELECT SQL_TEXT,SQL_ID, parsing_schema_name,EXECUTIONS,ELAPSED_TIME/1000 total_time, 
ELAPSED_TIME/ifnull(EXECUTIONS,1)/1000 avg_time , first_load_time, last_load_time
FROM ai_sr_oracle_sharepool_src
WHERE instance_url='{1}' and snapshot='{2}' and parsing_schema_name not in ('SYS', 'SYSTEM')
ORDER BY disk_reads DESC
limit {0}
            """.format(data_count, col_url, snapshot)

        else:
            print("UNKNOWN SHARE POOL SELECT TYPE [{0}]".format(type))
            return False

        bank_mysql_handle = DjMysqlHandle()

        bank_result = bank_mysql_handle.mysql_execute_query_get_all_data(share_pool_sql)

        if not bank_result.result:
            return False

        i = 0
        for row in bank_result.data:

            sql_id = row[1]
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
                # print("exception => {0}".format(ex))
                with open('/tmp/error_log.txt', 'a') as f:
                    f.write("[{0}] {1}\n".format(sql_id, str(ex)))
                pass

        return True
