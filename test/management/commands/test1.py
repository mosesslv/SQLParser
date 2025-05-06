# -*- coding: UTF-8 -*-

from django.core.management.base import BaseCommand


def test1():
    from service.oracle_meta.oracle_meta_abstract import OracleTableMeta
    from service.oracle_meta.oracle_meta_handle import OracleMetaHandle
    from common.OracleHandle import OracleHandle
    import json
    oracle_handle = OracleHandle("system", "oracle123", "lumeta", "172.168.71.55", 1521)
    oracle_meta_handle = OracleMetaHandle(oracle_handle)
    ora_tab_meta = OracleTableMeta()
    ora_tab_meta.info_result = False
    ora_tab_meta.schema_name = "KEFDATA"
    ora_tab_meta.table_name = "BECS_MQ_CLIENT_MSG"

    result = oracle_meta_handle.get_oracle_tab_meta(ora_tab_meta)

    d = ora_tab_meta.__dict__
    sd = json.dumps(d)

    t1 = json.loads(sd)
    print(sd)

    stu = OracleTableMeta()
    stu.__dict__ = t1

    print(result)


def test2():
    from service.AISQLReview.service_enter import ServiceEnter
    d = {
        "tenant": "LUFAX",
        "host": "172.168.71.55",
        "port": 1521,
        "username": "system",
        "passwd": "oracle123",
        "instance_name": "lumeta",
        "schema": "kefdata",
        "sql_text": "select * from BECS_MQ_CLIENT_MSG",
        "sequence": ""
    }

    se = ServiceEnter(d)
    r = se.predict()
    print(r)


def test_service_enter():
    from service.AISQLReview.service_enter import ServiceEnter
    parameter_dict = {
        "userid": "shenhaiming804",
        "profile_name": "lufax",
        "sequence": "",
        "tenant": "LUFAX",
        "schema": "lfdusrdata",
        "sql_text": "select count(*) from lfd_usr_artificial where rownum<2"
    }

    try:
        se = ServiceEnter(parameter_dict)
    except Exception as ex:
        print(ex)

    print("hello")


def batch_handle_0611_to_new_db():
    from common.MysqlHandle import MysqlHandle
    from common.DjangoMysqlHandle import DjMysqlHandle
    from service.AISQLReview.service_enter import ServiceEnter
    from service.sql_parser_graph.SQLParser import SQLParser

    dbcm_conn = MysqlHandle("dbcm", "dbcmlufaxcom", "31.94.5.7", port=3308, database="dbcm")
    dpaa_conn = DjMysqlHandle()
    sql_0611 = "select id, sql_text from sr_sql_special_value_0611 where db_type='ORACLE' and id > order by id"

    data_0611 = dbcm_conn.mysql_execute_query_get_all_data(sql_0611)

    print("========================== start handle ================================")
    for row in data_0611.data:
        rowid = row[0]
        sql_text = row[1]

        print("========================== handle -> {0} ================================".format(rowid))

        sqlparser = SQLParser(sql_text)
        table_names = sqlparser.get_table_name()

        if len(table_names) <= 0:
            print("can not find table name")
            continue

        table_name = table_names[0]

        sql_get_schema = "select data_user, instance_type from db_schema_group "\
                         "where id = (select schema_group_id from dba_table where name = '{0}')".\
            format(table_name.upper())
        schema_result = dbcm_conn.mysql_execute_query_get_all_data(sql_get_schema)
        if not schema_result.result:
            print("find schema data failed [{0}]".format(table_name))
            continue

        if len(schema_result.data) == 1:
            schema_name = schema_result.data[0][0]
            db_type = schema_result.data[0][1]
        else:
            print("schema data invalid [{0}]".format(schema_result.data))
            continue

        if db_type.upper() != "ORACLE":
            print("[{0}] schema is not oracle [{1}]".format(schema_name, db_type))
            continue

        sql_get_phy = "select instance_name, master_ip, master_port, master_username, master_passwd "\
                      "from dpaa_instance_conn_info where instance_name = "\
                      "(SELECT instance_name FROM dpaacc.dpaa_instance_schema_rel "\
                      "where schema_name = '{0}' and instance_type = 'ORACLE')".format(schema_name.upper())
        phy_result = dpaa_conn.mysql_execute_query_get_all_data(sql_get_phy)
        if not phy_result.result:
            print("get phy info failed [{0}]".format(schema_name))
            continue

        if len(phy_result.data) == 1:
            instance_name = phy_result.data[0][0]
            master_ip = phy_result.data[0][1]
            master_port = phy_result.data[0][2]
            master_username = phy_result.data[0][3]
            master_passwd = phy_result.data[0][4]
        else:
            print("phy info invalid [{0}]".format(phy_result.data))
            continue

        d = {
            "tenant": "0001",
            "host": master_ip,
            "port": master_port,
            "username": master_username,
            "passwd": master_passwd,
            "instance_name": instance_name,
            "schema": schema_name.upper(),
            "sql_text": sql_text,
            "sequence": ""
        }

        se = ServiceEnter(d)
        r = se.predict()
        print(r)

    print("========================== finished ================================")


def test_char_utf():
    from common.OracleHandle import OracleHandle
    from service.oracle_opt.oracle_common import OracleCommon
    import chardet

    # a = """PROGRAM����Ͷ��/RESERVE(ԤԼ)"""


    oh = OracleHandle("system", "oracle123", "lumeta", "172.168.71.55", 1521)
    oc = OracleCommon(oh)

    col_dict = oc.get_view_tab_columns("PLANDATA", "INVESTMENTS")


    sql_text = """
    select col.* ,comments.comments from
(select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,
DATA_SCALE,NULLABLE,COLUMN_ID,DEFAULT_LENGTH,DATA_DEFAULT,NUM_DISTINCT,DENSITY,
NUM_NULLS, NUM_BUCKETS,to_char(LAST_ANALYZED,'yyyymmddhh24miss') LAST_ANALYZED,SAMPLE_SIZE,HISTOGRAM
from dba_tab_columns where owner='PLANDATA' and table_name = 'INVESTMENTS' order by column_id) col
left join dba_col_comments comments
on col.owner = comments.owner
and col.table_name = comments.table_name
and col.column_name = comments.column_name
    """
    result = oh.ora_execute_query_get_all_data(sql_text)

    print(result.data)
    return result.data
    print("hello")


def batch_handle_ai_sr_sql_detail():
    from test.SQLPredictGrpcChannel import OracleSQLPredictGRpcChannel
    from common.DjangoMysqlHandle import DjMysqlHandle
    dpaa_conn = DjMysqlHandle()

    ora_sql_grpc = OracleSQLPredictGRpcChannel()
    sql_detail = "select * from ai_sr_sql_detail where id >= 1 order by id"

    data_detail = dpaa_conn.mysql_execute_query_get_all_data(sql_detail)

    print("========================== start handle ================================")
    for row in data_detail.data:
        rowid = row[0]
        sequence = row[5]
        db_conn_url = row[8]
        schema_name = row[9]
        sql_text = row[10]

        print("\n")
        print("========================== handle -> {0} ================================".format(rowid))

        try:
            addr = db_conn_url.split("/")[0]
            instance_name = db_conn_url.split("/")[1]
            host = addr.split(":")[0]
            port = int(addr.split(":")[1])
        except Exception as ex:
            print("[{0}] -> [{1}] split exception : {2}".format(rowid, db_conn_url, ex))
            continue

        data_dict = {
            "tenant": "0001",
            "host": host,
            "port": port,
            "username": "system",
            "passwd": "sqlrush",
            "instance_name": instance_name,
            "schema": schema_name,
            "sql_text": sql_text,
            "sequence": sequence
        }

        result = ora_sql_grpc.sql_predict(data_dict)
        print(result)
        print("\n")

    print("========================== finished ================================")


def batch_handle_redo_data():
    from common.MysqlHandle import MysqlHandle
    from common.DjangoMysqlHandle import DjMysqlHandle
    from service.AISQLReview.service_enter import ServiceEnter
    from service.sql_parser_graph.SQLParser import SQLParser

    dbcm_conn = MysqlHandle("dbcm", "dbcmlufaxcom", "31.94.5.7", port=3308, database="dbcm")
    dpaa_conn = DjMysqlHandle()
    sql_redo = """select id, sequence, tenant_code, db_conn_url, schema_name, sql_text
from ai_sr_sql_detail where db_type='ORACLE' and id >= 1 order by id"""

    data_redo = dpaa_conn.mysql_execute_query_get_all_data(sql_redo)

    print("========================== start handle ================================")
    for row in data_redo.data:
        rowid = row[0]
        sequence = row[1]
        tenant_code = row[2]
        db_conn_url = row[3]
        schema_name = row[4]
        sql_text = row[5]

        print("========================== handle -> {0} ================================".format(rowid))

        sqlparser = SQLParser(sql_text)
        table_names = sqlparser.get_table_name()

        if len(table_names) <= 0:
            print("can not find table name")
            continue

        table_name = table_names[0]

        sql_get_schema = "select data_user, instance_type from db_schema_group "\
                         "where id = (select schema_group_id from dba_table where name = '{0}')".\
            format(table_name.upper())
        schema_result = dbcm_conn.mysql_execute_query_get_all_data(sql_get_schema)
        if not schema_result.result:
            print("find schema data failed [{0}]".format(table_name))
            continue

        if len(schema_result.data) == 1:
            schema_name = schema_result.data[0][0]
            db_type = schema_result.data[0][1]
        else:
            print("schema data invalid [{0}]".format(schema_result.data))
            continue

        if db_type.upper() != "ORACLE":
            print("[{0}] schema is not oracle [{1}]".format(schema_name, db_type))
            continue

        sql_get_phy = "select instance_name, master_ip, master_port, master_username, master_passwd "\
                      "from dpaa_instance_conn_info where instance_name = "\
                      "(SELECT instance_name FROM dpaacc.dpaa_instance_schema_rel "\
                      "where schema_name = '{0}' and instance_type = 'ORACLE')".format(schema_name.upper())
        phy_result = dpaa_conn.mysql_execute_query_get_all_data(sql_get_phy)
        if not phy_result.result:
            print("get phy info failed [{0}]".format(schema_name))
            continue

        if len(phy_result.data) == 1:
            instance_name = phy_result.data[0][0]
            master_ip = phy_result.data[0][1]
            master_port = phy_result.data[0][2]
            master_username = phy_result.data[0][3]
            master_passwd = phy_result.data[0][4]
        else:
            print("phy info invalid [{0}]".format(phy_result.data))
            continue

        d = {
            "userid": "shm",
            "tenant": "LUFAX",
            "profile_name": "LUFAX",
            "schema": schema_name.upper(),
            "sql_text": sql_text,
            "sequence": str(sequence)
        }

        se = ServiceEnter(d)
        r = se.predict()
        print(r)

    print("========================== finished ================================")


def test_mysql_ai_process():
    from service.AISQLReview.mysql_ai_process import MysqlAISQLReview
    from service.AISQLReview.sql_abstract import MysqlSQLStruct
    from common.MysqlHandle import MysqlHandle
    from service.mysql_opt.mysql_common import MysqlCommon

    my_handle = MysqlHandle("mysqladmin", "mysqladmin", "172.168.71.55", port=3306)
    # mc = MysqlCommon(my_handle)
    # pd = mc.get_explain_from_pandas("LDGDATA", "select * from LDG_JOURNAL")



    my_sql_struct = MysqlSQLStruct()
    my_sql_struct.tenant_code = "lufax"
    my_sql_struct.schema_name = "LDGDATA"
    my_sql_struct.sql_text = "select * from LDG_JOURNAL"
    my_sql_struct.mysql_conn = my_handle

    my_ai_sql = MysqlAISQLReview(my_sql_struct)
    result = my_ai_sql.sql_predict()

    print("hello")

#--------------------------------------------#
#       created by bohuai jiang              #
#--------------------------------------------#
def acquire_AI_input():
    from service.AISQLReview.mysql_ai_process import MysqlAISQLReview
    from service.AISQLReview.sql_abstract import MysqlSQLStruct
    from common.MysqlHandle import MysqlHandle

    my_handle = MysqlHandle("mysqladmin", "mysqladmin", "172.168.71.55", port=3306)

    my_sql_struct = MysqlSQLStruct()
    my_sql_struct.tenant_code = "lufax"
    my_sql_struct.schema_name = "LDGDATA"
    my_sql_struct.sql_text = "select * from LDG_JOURNAL where posted = 'Dummy' and RETRY_COUNT = 2 and partition_key <> 10"
    my_sql_struct.mysql_conn = my_handle

    my_ai_sql = MysqlAISQLReview(my_sql_struct)
    result = my_ai_sql.sql_predict()

    print("hello")


def test_exception():
    from service.AISQLReview.handle_exception import DBConnectException

    ai_err_code = "AIErr_Oracle_00004"
    message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
    raise DBConnectException(message_dict)


def test_mysql_explain():
    from service.mysql_opt.mysql_common import MysqlCommon, MysqlHandle
    # my_handle = MysqlHandle("mysqladmin", "mysqladmin", "172.168.71.55", port=3306)
    my_handle = MysqlHandle("dba", "dba", "172.23.30.48", port=3306)
    mc = MysqlCommon(my_handle)
    schema = "lmimydata"
    sql_text = """
    update
        /*+index(msc_points_trade_success PK_MSC_PONTS_TRD_SUC_ID)*/
        msc_points_trade_success t set
        t.userName = 'dummy',
        t.virtual_sms_times = 1,
        t.logistice_id = 1,
        t.receiver_name = 'dummy',
        t.receiver_province = 'dummy',
        t.receiver_city = 'dummy',
        t.receiver_country = 'dummy',
        t.encode_receiver_address='dummy',
        t.receiver_post = 'dummy',
        t.encode_receiver_mobile='dummy',
        t.remark = 'dummy',
        t.updated_by = 'dummy',
        t.updated_at=sysdate,
        t.version=t.version+1
        where t.id = 1 and t.version=1 and partition_key>= to_number(to_char(sysdate-100,'yyyyMM'))
    """
    d = mc.get_explain(schema, sql_text)

    print("aaaaa")


def test_ai_sqlreview_handle(seq):
    from service.GitSQLReview.sqlreview_handle import SQLReviewHandle, SQLReviewStruct
    from api_service.pallas_api.models import AiSrReviewDetail

    sql_detail = AiSrReviewDetail.objects.get(sequence=seq)

    sql_struct = SQLReviewStruct()
    sql_struct.sql_text = sql_detail.sql_new_text
    sql_struct.sqlmap_filename = sql_detail.sqlmap_files
    sql_struct.namespace = sql_detail.namespace
    sql_struct.sqlmap_id = sql_detail.sqlmap_id
    sql_struct.review_request_id = sql_detail.review_request_id
    sql_struct.sql_sequence = sql_detail.sequence
    sql_handle = SQLReviewHandle(sql_struct)
    sql_handle.get_ai_predict()

    result = sql_handle.write_database()

    print("hello")


def test_utf8():
    import cx_Oracle
    import os
    # os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 或者
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'

    # from common.OracleHandle import OracleHandle
    # from service.oracle_opt.oracle_common import OracleCommon
    # oh = OracleHandle("system", "sqlrush", "bdp", "31.99.72.61", port=1525)
    # oc = OracleCommon(oh)
    # result = oc.get_view_dba_histogram("FP", "FP_FINANCE_TRADE")
    # print("hello")

    sql_text = """select COLUMN_NAME,ENDPOINT_NUMBER,ENDPOINT_NUMBER,ENDPOINT_ACTUAL_VALUE 
from dba_histograms where owner='FP' and table_name='FP_FINANCE_TRADE'
    """

    ora_url = "{0}:{1}/{2}".format("31.99.72.61", 1525, "bdp")
    conn = cx_Oracle.Connection("system", "sqlrush", ora_url, events=True)
    _db_cursor = conn.cursor()  # 获取cursor
    exec_result = _db_cursor.execute(sql_text)

    while True:
        # 获取一行记录，每行数据都是一个元组
        row = exec_result.fetchone()
        # 如果抓取的row为None，退出循环
        if not row:
            break

    # data = _db_cursor.fetchall()
    print("hello")


def test_orm_all():
    from common.MysqlHandle import MysqlHandle
    import json
    #
    # a = """[[null, 4016, '2019-11-28 14:16:54', null, 'SELECT STATEMENT', null, null, null, null, null, null, null, 'ALL_ROWS', null, 0, null, 0, 2, 2, 1, 47, null, null, null, null, null, null, 16153, 2, null, null, null, null, 1, null], [null, 4016, '2019-11-28 14:16:54', null, 'TABLE ACCESS', 'BY INDEX ROWID', null, 'CFGDATA', 'BIZ_PARAMETERS', 'BP@SEL$1', 1, 'TABLE', 'ANALYZED', null, 1, 0, 1, 1, 2, 1, 47, null, null, null, null, null, null, 16153, 2, null, null, null, ''BP'.'ID'[NUMBER,22], 'PARAMETER_CODE'[VARCHAR2,512], 'BP'.'PARAMETER_VALUE'[NUMBER,22], 'BP'.'VALUE'[VARCHAR2,4000]', 1, 'SEL$1'], [null, 4016, '2019-11-28 14:16:54', null, 'INDEX', 'RANGE SCAN', null, 'CFGDATA', 'UK_BIZ_PARAMETERS_PCODE', 'BP@SEL$1', null, 'INDEX', 'ANALYZED', 1, 2, 1, 2, 1, 1, 1, null, null, null, null, null, null, null, 8371, 1, null, ''PARAMETER_CODE'='product.category.invest.withholding.config'', null, ''BP'.ROWID[ROWID,10], 'PARAMETER_CODE'[VARCHAR2,512]', 1, 'SEL$1']]"""
    # aa = json.loads(a)

    mh = MysqlHandle("dpaacc", "dpaacc", "172.19.44.12", port=3368, database="dpaacc")
    result = mh.mysql_execute_query_get_all_data("select * from shm_test_2")
    raw_str = result.data[0][0]
    b = json.loads(raw_str)


    print("hello")


def test_ai():
    from service.GitSQLReview.sqlreview_handle import SingleSQLHandleAndWrite
    import common.utils as utils

    uuid = utils.get_uuid()

    sql_handle_and_write = SingleSQLHandleAndWrite(
        tenant="pallas",
        userid="shenhaiming804",
        profile_name="",
        sql_text="select * from AI_SR_APP_REPOSITORY",
        sql_sequence=uuid,
        db_type="MYSQL",
        schema_name="shm")
    result = sql_handle_and_write.sql_handle_and_write()


def shm_test():
    from service.AISQLReview.ai_process import OracleAISQLReview
    from service.AISQLReview.sql_abstract import OracleSQLStruct
    from common.OracleHandle import OracleHandle
    import service.AISQLReview.AIError as AIError
    import common.utils as utils

    ods_select_data_handle = OracleHandle("system", "sqlrush", "ods", "31.67.72.85", port=1525)
    ods_update_handle = OracleHandle("system", "sqlrush", "ods", "31.67.72.85", port=1525)
    select_sql = "select * from comopr.shm_test order by id"

    ods_result = ods_select_data_handle.ora_execute_query_get_all_data(select_sql)

    for row in ods_result.data:
        id = row[0]
        schema_name = row[1]
        sql_text = row[2]

        oracle_handle = OracleHandle("system", "sqlrush", "ods", "31.67.72.85", port=1525)
        oracle_sql_struct = OracleSQLStruct()
        oracle_sql_struct.sequence = utils.get_uuid()
        oracle_sql_struct.tenant_code = "LUFAX"
        oracle_sql_struct.data_handle_result = False
        oracle_sql_struct.message = ""
        oracle_sql_struct.oracle_conn = oracle_handle
        oracle_sql_struct.schema_name = schema_name.upper()
        oracle_sql_struct.sql_text = sql_text

        try:
            oracle_ai_sqlreview = OracleAISQLReview(oracle_sql_struct)
            ai_result = oracle_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            if not ai_result:
                result = {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }
            else:
                result = {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
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

        # update_sql = "update comopr.shm_test set AI_RESULT='{0}', AI_RECOMM='{1}' where id = {2}".format(result["AI_RESULT"], result["AI_RECOMMEND"], id)
        # ods_update_handle.ora_execute_dml_sql(update_sql)

        # out_str = "--------------------------------------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n\n".format(sql_text, result)
        # print(out_str)
        # f = open('/tmp/test.txt', 'a')
        # f.write(out_str)
        # f.close()

        print("================= {0} {1}\n {2} =================".format(id, type(oracle_sql_struct.ai_recommend),
                                                                         oracle_sql_struct.ai_recommend))
        if oracle_sql_struct.ai_result.upper() == "NOPASS":
            try:
                out_str = "--------------------------------------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n{2}\n\n".format(
                    sql_text, oracle_sql_struct.ai_result, oracle_sql_struct.ai_recommend[0])
                print(out_str)
                f = open('/tmp/test_crm.txt', 'a')
                f.write(out_str)
                f.close()
            except:
                pass


def shm_test_ods():
    from service.AISQLReview.ai_process import OracleAISQLReview
    from service.AISQLReview.sql_abstract import OracleSQLStruct
    from common.OracleHandle import OracleHandle
    import service.AISQLReview.AIError as AIError
    import common.utils as utils

    ods_select_data_handle = OracleHandle("system", "sqlrush", "ods", "31.67.72.85", port=1525)
    ods_update_handle = OracleHandle("system", "sqlrush", "ods", "31.67.72.85", port=1525)
    select_sql = "select * from comopr.shm_test order by id"

    ods_result = ods_select_data_handle.ora_execute_query_get_all_data(select_sql)

    for row in ods_result.data:
        id = row[0]
        schema_name = row[1]
        sql_text = row[2]

        oracle_handle = OracleHandle("system", "sqlrush", "ods", "31.67.72.85", port=1525)
        oracle_sql_struct = OracleSQLStruct()
        oracle_sql_struct.sequence = utils.get_uuid()
        oracle_sql_struct.tenant_code = "LUFAX"
        oracle_sql_struct.data_handle_result = False
        oracle_sql_struct.message = ""
        oracle_sql_struct.oracle_conn = oracle_handle
        oracle_sql_struct.schema_name = schema_name.upper()
        oracle_sql_struct.sql_text = sql_text

        try:
            oracle_ai_sqlreview = OracleAISQLReview(oracle_sql_struct)
            ai_result = oracle_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            if not ai_result:
                result = {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }
            else:
                result = {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
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

        # update_sql = "update comopr.shm_test set AI_RESULT='{0}', AI_RECOMM='{1}' where id = {2}".format(result["AI_RESULT"], result["AI_RECOMMEND"], id)
        # ods_update_handle.ora_execute_dml_sql(update_sql)

        # out_str = "--------------------------------------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n\n".format(sql_text, result)
        # print(out_str)
        # f = open('/tmp/test.txt', 'a')
        # f.write(out_str)
        # f.close()

        print("================= {0} {1}\n {2} =================".format(id, type(oracle_sql_struct.ai_recommend),
                                                                         oracle_sql_struct.ai_recommend))
        if oracle_sql_struct.ai_result.upper() == "NOPASS":
            try:
                out_str = "--------------------------------------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n{2}\n\n".format(
                    sql_text, oracle_sql_struct.ai_result, oracle_sql_struct.ai_recommend[0])
                print(out_str)
                f = open('/tmp/test_crm.txt', 'a')
                f.write(out_str)
                f.close()
            except:
                pass





def shm_test_crm():
    from service.AISQLReview.ai_process import OracleAISQLReview
    from service.AISQLReview.sql_abstract import OracleSQLStruct
    from common.OracleHandle import OracleHandle
    import service.AISQLReview.AIError as AIError
    import common.utils as utils

    crm_select_data_handle = OracleHandle("system", "sqlrush", "crm", "31.67.72.81", port=1525)
    select_sql = "select * from crmopr.shm_test where id > 0 order by id"

    ods_result = crm_select_data_handle.ora_execute_query_get_all_data(select_sql)

    for row in ods_result.data:
        id = row[0]
        schema_name = row[1]
        sql_text = row[2]

        oracle_handle = OracleHandle("system", "sqlrush", "crm", "31.67.72.81", port=1525)
        oracle_sql_struct = OracleSQLStruct()
        oracle_sql_struct.sequence = utils.get_uuid()
        oracle_sql_struct.tenant_code = "LUFAX"
        oracle_sql_struct.data_handle_result = False
        oracle_sql_struct.message = ""
        oracle_sql_struct.oracle_conn = oracle_handle
        oracle_sql_struct.schema_name = schema_name.upper()
        oracle_sql_struct.sql_text = sql_text

        try:
            oracle_ai_sqlreview = OracleAISQLReview(oracle_sql_struct)
            ai_result = oracle_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            if not ai_result:
                result = {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }
            else:
                result = {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
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
        print("================= {0} {1}\n {2} =================".format(id, type(oracle_sql_struct.ai_recommend), oracle_sql_struct.ai_recommend))
        # update_sql = "update comopr.shm_test set AI_RESULT='{0}', AI_RECOMM='{1}' where id = {2}".format(result["AI_RESULT"], result["AI_RECOMMEND"], id)
        # ods_update_handle.ora_execute_dml_sql(update_sql)

        if oracle_sql_struct.ai_result.upper() == "NOPASS":
            try:
                out_str = "--------------------------------------------------\n{0}\n\n++++++++++++++++++++++++++++++++++++++++++++++++++\n\n{1}\n\n{2}\n\n".format(
                    sql_text, oracle_sql_struct.ai_result, oracle_sql_struct.ai_recommend[0])
                print(out_str)
                f = open('/tmp/test_crm.txt', 'a')
                f.write(out_str)
                f.close()
            except:
                pass


def single_ai_predict():
    from service.AISQLReview.sql_abstract import OracleSQLStruct
    from common.OracleHandle import OracleHandle
    from service.AISQLReview.ai_process import OracleAISQLReview
    import service.AISQLReview.AIError as AIError
    import common.utils as utils

    sql_text = """SELECT  *  FROM  (   
SELECT  SUM((CC1403497422)) T4 ,  SUM((CC1397497310)) T5 ,  SUM((CC1603603611)) T6 ,  SUM((CC1597603499)) T7 ,  
SUM((C1948592346)) T8 ,  SUM((C1954592458)) T9 ,  SUM((C2041963219)) T2 ,  SUM((CC24877953)) T3 FROM  (  
SELECT  *  FROM  (  
SELECT  *  FROM  (  
SELECT "获客渠道" C807349589 , "申请笔数" C2041963219 , "申请提款笔数" CC1603603611 , 
"推荐人所属支行名称" C1557163809 , "业务子类型" CC816450280 , "机构名称" CC1548889882 , 
"渠道来源" C397943381 , "申请金额" C2047963331 , (IS_HEKH) C1933911260 , "申请提款金额" CC1597603499 , 
"一类户成功还款笔数" C1038155204 , "分行" CC2125023795 , "贷款用途" CC1120841952 , "KB码分类" CC1935613902 , 
"推荐人编码" C1070234612 , "推荐人名称" C1059662620 , (OUTERID) CC1283978621 , "二类户成功还款笔数" CC703931720 , 
"二类户成功提款笔数" C1771041456 , (OUTERSOURCE) CC580097661 , "机构编码" CC1538317890 , "贷款期限" CC1124245349 , 
"一类户成功提款笔数" CC781838916 , "推荐人所属支行编码" C1567735801 , "成功提款金额" C1954592458 , (CID) C762083051 , 
"统计口径" C21383184 , "推荐人代码" C1058376833 , "贷款城市" CC1128460264 , "分行编码" CC2006216510 , 
"渠道来源编码" C203684682 , "子产品分类" C517742450 , "审批笔数" CC24877953 , "区域" CC2125112114 , 
"业务品种" C941837056 , "审批通过笔数" CC1403497422 , "方案编号" CC1888320445 , "切换日期" C2001817886 , 
"统计日期" C25873751 , "获客渠道编码" CC1493940662 , "审批通过金额" CC1397497310 , "成功提款笔数" C1948592346 , 
"切换维度" C2007915462 FROM  ( 
select
  DATA_TYPE               AS 统计口径
  ,case when '6'=1 then coalesce(CITY_DESC_CN,'未知')
        when '6'=2 then coalesce(BRANCH03_NAME,'未知') 
          when '6'=3 then coalesce(CHANNEL_SOURCE,'未知') 
      when '6'=4 then coalesce(SOURCE,'未知') 
      when '6'=5 then coalesce(OUTERSOURCE,'未知') 
      when '6'=6 then coalesce(OUTERID,'未知') 
      when '6'=7 then coalesce(CID,'未知') 
      when '6'=8 then coalesce(CHANNEL_SOURCE_NAME||' '||SOURCE_NAME,'未知')
      end AS 切换维度 
    ,case when '1'=1 then  DATA_DATE
          else TO_CHAR( TO_DATE(('2019-12-12') ,'yyyy-MM-dd'),'yyyyMMdd')||'-'||TO_CHAR( TO_DATE( ('2019-12-13'),'yyyy-MM-dd'),'yyyyMMdd')
           end as 切换日期
  ,to_date(DATA_DATE,'YYYY-MM-DD')              AS 统计日期
  ,JJ_CHANNEL             AS KB码分类
  ,REC_NO                 AS 推荐人代码
  ,CHANNEL_SOURCE         AS 渠道来源编码
  ,CHANNEL_SOURCE_NAME    AS 渠道来源
  ,SOURCE                 AS 获客渠道编码
  ,SOURCE_NAME            AS 获客渠道
  ,CITY_DESC_CN           AS 贷款城市
  ,ORG_ID                 AS 机构编码
  ,ORG_NAME               AS 机构名称
  ,BRANCH03_NO            AS 分行编码
  ,BRANCH03_NAME          AS 分行
  ,BRANCH02_NAME          AS 区域
  ,BUSINESSTYPE           AS 业务品种
  ,BUSINESSSUBTYPE        AS 业务子类型
  ,SCHEMENO               AS 方案编号
  ,PURPOSE_NAME           AS 贷款用途
  ,LOANTERM               AS 贷款期限
  ,SQ_NUM                 AS 申请笔数
  ,SQ_AMT                 AS 申请金额
  ,SP_NUM                 AS 审批笔数
  ,SP_TG_NUM              AS 审批通过笔数
  ,SP_TG_AMT              AS 审批通过金额
  ,SQ_TK_NUM              AS 申请提款笔数
  ,SQ_TK_AMT              AS 申请提款金额
  ,TK_NUM                 AS 成功提款笔数
  ,TK_AMT                 AS 成功提款金额
  
    ,CASE WHEN SALERNAME IS NULL THEN NULL ELSE REC_NO_ZH END  推荐人编码
    ,SALERNAME  推荐人名称
    ,SALERNAME_BRANCH03_NO     推荐人所属支行编码
    ,SALERNAME_BRANCH03_NAME   推荐人所属支行名称
  
  ,YLH_TK_NUM 一类户成功提款笔数
    ,ELH_TK_NUM 二类户成功提款笔数
    ,YLH_HK_NUM 一类户成功还款笔数
    ,ELH_HK_NUM 二类户成功还款笔数
  
  ,OUTERSOURCE -- 获客路径
    ,OUTERID -- 外媒参数一级渠道
    ,CID -- 外媒参数二级渠道

  ,IS_HEKH -- 是否核额客户
  ,PRODUCT_NAME 子产品分类
from BRRPSDATA.XJ_RPT_D_PAZDXE_KJHZ
where data_dt = (select max(data_dt) from BRRPSDATA.XJ_RPT_D_PAZDXE_KJHZ)
and to_date(DATA_DATE,'YYYY-MM-DD')>=TO_DATE(('2019-12-12'),'yyyy-MM-dd')
AND to_date(DATA_DATE,'YYYY-MM-DD') <=TO_DATE(('2019-12-13'),'yyyy-MM-dd')) T_T1150656837 )  T_T1396198938 )  T_T1396198938  

WHERE  (  (C21383184) IN ('企划口径') ) )  T_T_T1396198938  INNER JOIN   (  SELECT  SUM((C2041963219)) C_2 ,  SUM((CC24877953)) C_3 ,  SUM((CC1403497422)) C_4 ,  SUM((CC1397497310)) C_5 ,  SUM((CC1603603611)) C_6 ,  SUM((CC1597603499)) C_7 ,  SUM((C1948592346)) C_8 ,  SUM((C1954592458)) C_9,(C2001817886) C2001817886__tablecol_ , (C2007915462) C2007915462__tablecol_ FROM  (  SELECT "申请笔数" C2041963219 , "申请提款笔数" CC1603603611 , "申请提款金额" CC1597603499 , "成功提款金额" C1954592458 , "统计口径" C21383184 , "审批笔数" CC24877953 , "审批通过笔数" CC1403497422 , "切换日期" C2001817886 , "审批通过金额" CC1397497310 , "成功提款笔数" C1948592346 , "切换维度" C2007915462 FROM  ( select
  DATA_TYPE               AS 统计口径
  ,case when '6'=1 then coalesce(CITY_DESC_CN,'未知')
        when '6'=2 then coalesce(BRANCH03_NAME,'未知') 
          when '6'=3 then coalesce(CHANNEL_SOURCE,'未知') 
      when '6'=4 then coalesce(SOURCE,'未知') 
      when '6'=5 then coalesce(OUTERSOURCE,'未知') 
      when '6'=6 then coalesce(OUTERID,'未知') 
      when '6'=7 then coalesce(CID,'未知') 
      when '6'=8 then coalesce(CHANNEL_SOURCE_NAME||' '||SOURCE_NAME,'未知')
      end AS 切换维度 
    ,case when '1'=1 then  DATA_DATE
          else TO_CHAR( TO_DATE(
 ('2019-12-12')
 ,
 'yyyy-MM-dd'
 
),'yyyyMMdd')||'-'||TO_CHAR( TO_DATE(
 ('2019-12-13')
 ,
 'yyyy-MM-dd'
 
),'yyyyMMdd') end as 切换日期
  ,to_date(DATA_DATE,'YYYY-MM-DD')              AS 统计日期
  ,JJ_CHANNEL             AS KB码分类
  ,REC_NO                 AS 推荐人代码
  ,CHANNEL_SOURCE         AS 渠道来源编码
  ,CHANNEL_SOURCE_NAME    AS 渠道来源
  ,SOURCE                 AS 获客渠道编码
  ,SOURCE_NAME            AS 获客渠道
  ,CITY_DESC_CN           AS 贷款城市
  ,ORG_ID                 AS 机构编码
  ,ORG_NAME               AS 机构名称
  ,BRANCH03_NO            AS 分行编码
  ,BRANCH03_NAME          AS 分行
  ,BRANCH02_NAME          AS 区域
  ,BUSINESSTYPE           AS 业务品种
  ,BUSINESSSUBTYPE        AS 业务子类型
  ,SCHEMENO               AS 方案编号
  ,PURPOSE_NAME           AS 贷款用途
  ,LOANTERM               AS 贷款期限
  ,SQ_NUM                 AS 申请笔数
  ,SQ_AMT                 AS 申请金额
  ,SP_NUM                 AS 审批笔数
  ,SP_TG_NUM              AS 审批通过笔数
  ,SP_TG_AMT              AS 审批通过金额
  ,SQ_TK_NUM              AS 申请提款笔数
  ,SQ_TK_AMT              AS 申请提款金额
  ,TK_NUM                 AS 成功提款笔数
  ,TK_AMT                 AS 成功提款金额  
    ,CASE WHEN SALERNAME IS NULL THEN NULL ELSE REC_NO_ZH END  推荐人编码
    ,SALERNAME  推荐人名称
    ,SALERNAME_BRANCH03_NO     推荐人所属支行编码
    ,SALERNAME_BRANCH03_NAME   推荐人所属支行名称  
  ,YLH_TK_NUM 一类户成功提款笔数
    ,ELH_TK_NUM 二类户成功提款笔数
    ,YLH_HK_NUM 一类户成功还款笔数
    ,ELH_HK_NUM 二类户成功还款笔数  
  ,OUTERSOURCE -- 获客路径
    ,OUTERID -- 外媒参数一级渠道
    ,CID -- 外媒参数二级渠道
  ,IS_HEKH -- 是否核额客户
  ,PRODUCT_NAME 子产品分类
from BRRPSDATA.XJ_RPT_D_PAZDXE_KJHZ
where data_dt = (select max(data_dt) from BRRPSDATA.XJ_RPT_D_PAZDXE_KJHZ)
and to_date(DATA_DATE,'YYYY-MM-DD')>=   TO_DATE(('2019-12-12'),'yyyy-MM-dd')
AND to_date(DATA_DATE,'YYYY-MM-DD') <=   TO_DATE( ('2019-12-13'),'yyyy-MM-dd'))  T_T1150656837 )  T_T1150656837  
WHERE  (  (C21383184) IN ('企划口径') ) 
GROUP BY (C2001817886) , (C2007915462)   )  T_T_2571410 
ON T_T_T1396198938.C2001817886  =  T_T_2571410.C2001817886__tablecol_ 
AND T_T_T1396198938.C2007915462  =  T_T_2571410.C2007915462__tablecol_   )  T_2571410 
    """

    oracle_handle = OracleHandle("system", "sqlrush", "t029", "172.23.31.175", 1525)
    oracle_sql_struct = OracleSQLStruct()
    oracle_sql_struct.sequence = utils.get_uuid()
    oracle_sql_struct.tenant_code = "LUFAX"
    oracle_sql_struct.data_handle_result = False
    oracle_sql_struct.message = ""
    oracle_sql_struct.oracle_conn = oracle_handle
    oracle_sql_struct.schema_name = "BRRPSDATA "
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

    print(result)

def test_server_v2():
    from service.AISQLReview.service_enter import ServiceEnterV2
    try:
        se2 = ServiceEnterV2("ORACLE1", "172.168.71.55", 1521, "lumeta", "system", "oracle123")
    except Exception as ex:
        print(ex)
        return

    schema_name = "bdcdata"
    sql_text = """select * from bdc_fund_info where id > 100"""

    result = se2.predict_sqltext(schema_name, sql_text)
    print(result)


class Command(BaseCommand):
    def handle(self, *args, **options):
        # import pdb;pdb.set_trace()
        # rowid = int(args[0])
        single_ai_predict()
