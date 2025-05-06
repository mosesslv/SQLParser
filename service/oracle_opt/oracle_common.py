# -*- coding: UTF-8 -*-
# created by lvao
# on 2020/05/26

from common.OracleHandle import OracleHandle
from typing import Dict
import common.utils
import traceback
import logging
logger = logging.getLogger("")


class OracleCommon:
    """
    ORACLE 管理, 完成ORACLE基本的操作方法
    """

    def __init__(self, oracle_handle_obj):
        """
        此类不关心创建连接, 连接对象需要有操作权限, 没有权限当失败处理
        :param oracle_handle_obj: oracle连接, 此类不关心DB建连
        :return:
        """
        assert isinstance(oracle_handle_obj, OracleHandle)
        if oracle_handle_obj is None:
            raise Exception("oracle connection invalid, object is none")

        self._oracle_handle_obj = oracle_handle_obj
        self.last_error_info = ""

    def check_tablespace_datafile(self, tablespace_datafile) -> bool:
        """
        select count（*）from dba_data_files where file_name='/opt/oracle/diag/ppidnkvsdata01.dbf';
        :param tablespace_datafile: 表空间文件全路径
        :return: boolean
        """
        sql_text = "select count(*) from dba_data_files where file_name='{0}'".format(tablespace_datafile)
        check_tablespace_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
        if not check_tablespace_result.result:
            msg = "check tablespace datafile [{0}] sql excute failed [{1}]".\
                format(tablespace_datafile, check_tablespace_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False
        if check_tablespace_result.data[0][0] > 0:
            msg = "tablespace datafile [{0}] is already exists,exception [{1}]".\
                format(tablespace_datafile, check_tablespace_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False

        return True

    def check_tablespace_user(self, tablespace_username) -> bool:
        """
         select username from all_users where username='PPIDNKVSDATA';
        :param tablespace_username:
        :return: boolean
        """
        sql_text = "select count(*) from all_users where username='{0}'".format(tablespace_username.upper())
        check_tabuser_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
        if not check_tabuser_result.result:
            msg = "check tablespace user [{0}] sql excute failed [{1}]".format(tablespace_username)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False
        if check_tabuser_result.data[0][0] > 0:
            msg = "tablespace user [{0}] is already exists,exception [{1}]".\
                format(tablespace_username, check_tabuser_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False

        return True

    def create_tablespace(self, tablespace_name, full_filepath, filesize="4g") -> bool:
        """
        创建表空间
        :param tablespace_name: 名称
        :param full_filepath: 文件全路径
        :param filesize: 文件容量, 默认4g
        :return: boolean
        """
        sql_text = "create tablespace {0} datafile '{1}' size {2}".format(tablespace_name, full_filepath, filesize)
        create_ts_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not create_ts_result.result:
            msg = "create tablespace [{0}] failed [{1}]".format(tablespace_name, create_ts_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False

        return True

    def create_user(self, username, passwd, default_tablespace="USERS", default_temp="TEMP"):
        """
        创建用户
        CREATE USER schdata IDENTIFIED BY schdata DEFAULT TABLESPACE schdata TEMPORARY TABLESPACE TEMP;
        :param username:
        :param passwd:
        :param default_tablespace:
        :param default_temp:
        :return: boolean
        """
        if len(username) <= 0 or len(passwd) <= 0:
            return False

        sql_text = "CREATE USER {0} IDENTIFIED BY {1} DEFAULT TABLESPACE {2} TEMPORARY TABLESPACE {3}". \
            format(username, passwd, default_tablespace, default_temp)
        create_user_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not create_user_result.result:
            msg = "create user [{0}] failed [{1}]".format(username, create_user_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False

        return True

    def get_dba_indexes_raw_data(self, schema_name, table_name):
        """
        获取INDEXES统计数据
        :param schema_name:
        :param table_name:
        :return: list
        """
        index_sql_text = "select index_name, uniqueness, distinct_keys "\
                         "from dba_indexes where table_owner='{0}' and table_name='{1}'".\
            format(schema_name.upper(), table_name.upper())
        ora_result = self._oracle_handle_obj.ora_execute_query_get_all_data(index_sql_text)

        if not ora_result.result:
            return None
        return ora_result.data

    # def get_table_histogram_raw_data(self, schema_name, table_name):
    #     """
    #     获取INDEXES统计数据
    #
    #     column : column_name, num_rows, Cardinality, selectivity, histogram, num_buckets
    #     :param schema_name:
    #     :param table_name:
    #     :return: list
    #     """
    #     histogram_sql_text = "select a.column_name,b.num_rows,a.num_distinct Cardinality, "\
    #                          "round(a.num_distinct / b.num_rows * 100, 2) selectivity,a.histogram,a.num_buckets "\
    #                          "from dba_tab_col_statistics a, dba_tables b "\
    #                          "where a.owner = b.owner and a.table_name = b.table_name "\
    #                          "and a.owner = '{0}' and a.table_name = '{1}'".\
    #         format(schema_name.upper(), table_name.upper())
    #
    #     histogram_result = self._oracle_handle_obj.ora_execute_query_get_all_data(histogram_sql_text)
    #     if not histogram_result.result:
    #         return None
    #     return histogram_result.data

    def tablespace_add_datafile(self, tablespace, datafile_path, default_size="4G"):
        """
        表空间添加数据文件
        alter tablespace dbadata add datafile '/opt/oracle/data1/dbats/dbadata02.dbf' size 100M;
        :param tablespace:
        :param datafile_path:
        :param default_size:
        :return: boolean
        """
        if len(tablespace) <= 0 or len(tablespace) <= 0:
            return False

        sql_text = "alter tablespace {0} add datafile '{1}' size {2}".format(tablespace, datafile_path, default_size)
        add_file_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not add_file_result.result:
            msg = "[{0}] add datafile [{1}] failed [{1}]".format(tablespace, datafile_path, add_file_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False
        return True

    def datafile_add_capacity(self, datafile_path, filesize):
        """
        表空间 数据文件 扩容
        alter database datafile xxxxxx resize 8g;
        :param datafile_path: 文件路径
        :param filesize: 容量 8G , 16G .....
        :return: boolean
        """
        sql_text = "alter database datafile '{0}' resize {1}".format(datafile_path, filesize)
        add_capacity_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not add_capacity_result.result:
            msg = "[{0}] add capacity [{1}] failed [{1}]".format(datafile_path, filesize, add_capacity_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False
        return True

    def get_tablespace_files(self, tablespace_name):
        """
        返回表空间文件列表
        :param tablespace_name:
        :return: list
        """
        sql_text = """
            select /*+ rule */ df.file#,df.rfile#,df.name,ts.name as tablespace_name,
            df.status,df.bytes/1024/1024 as "size(M)"
            from v$datafile df,v$tablespace ts
            where df.ts#=ts.ts# and ts.name=upper('{0}')
            order by df.name
        """.format(tablespace_name.upper())

        files = []
        query_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
        if not query_result.result:
            msg = "query failed [{0}] sql [{1}]".format(query_result.message, sql_text)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return None

        for row in query_result.data:
            filename = row[2]
            status = row[4]
            filesize = row[5]
            files.append({"filename": filename, "size": filesize})

        return files

    # def get_view_dba_tables(self, schema_name, table_name):
    #     """
    #     get dba_tables data
    #
    #     TABLESPACE_NAME, STATUS, NUM_ROWS, BLOCKS, AVG_ROW_LEN, SAMPLE_SIZE, LAST_ANALYZED, PARTITIONED
    #
    #     :param schema_name:
    #     :param table_name:
    #     :return: list
    #     """
    #     sql_text = "select TABLESPACE_NAME, STATUS, NUM_ROWS, BLOCKS, AVG_ROW_LEN, SAMPLE_SIZE, "\
    #         "to_char(LAST_ANALYZED, 'yyyymmddhh24miss'), PARTITIONED "\
    #         "from dba_tables where owner='{0}' and table_name='{1}'".\
    #         format(schema_name.upper(), table_name.upper())
    #     ora_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
    #
    #     if not ora_result.result:
    #         self.last_error_info = ora_result.message
    #         return None
    #     return ora_result.data

    # def get_view_dba_tab_col_statistics(self, schema_name, table_name):
    #     """
    #     get dba_tab_col_statistics
    #     :param schema_name:
    #     :param table_name:
    #     :return: list
    #     """
    #     sql_text = "select COLUMN_NAME,NUM_DISTINCT,'LOW_VALUE','HIGH_VALUE',DENSITY,NUM_NULLS, " \
    #                "NUM_BUCKETS, to_char(LAST_ANALYZED, 'yyyymmddhh24miss'),SAMPLE_SIZE,GLOBAL_STATS,"\
    #                "USER_STATS,AVG_COL_LEN,HISTOGRAM " \
    #                "from dba_tab_col_statistics where owner='{0}' and table_name='{1}'".\
    #         format(schema_name.upper(), table_name.upper())
    #     ora_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
    #
    #     if not ora_result.result:
    #         self.last_error_info = ora_result.message
    #         return None
    #     return ora_result.data

    # def get_view_dba_histogram(self, schema_name, table_name):
    #     """
    #     get dba_tab_col_statistics
    #     :param schema_name:
    #     :param table_name:
    #     :return: list
    #     """
    #     sql_text = "select COLUMN_NAME,ENDPOINT_NUMBER,ENDPOINT_NUMBER,ENDPOINT_ACTUAL_VALUE " \
    #                "from dba_histograms where owner='{0}' and table_name='{1}'".\
    #         format(schema_name.upper(), table_name.upper())
    #     ora_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
    #
    #     if not ora_result.result:
    #         self.last_error_info = ora_result.message
    #         return None
    #     return ora_result.data

    def excute_open_audit_sql(self, owner, tablename):
        """
         开启表审计
        :param owner: 用户
        :param tablename: 表名
        :return: boolean
        """
        sql_text = "audit select,update,insert,delete on {0}.{1} by access".format(owner, tablename)
        get_audit_sqltext_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not get_audit_sqltext_result.result:
            msg = "[{0}] excute open audit sql failed [{1}]".format(tablename, get_audit_sqltext_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False, get_audit_sqltext_result.message
        return get_audit_sqltext_result.result, get_audit_sqltext_result.message

    def excute_close_audit_sql(self, owner, tablename):
        """
         关闭表审计
        :param owner: 用户
        :param tablename: 表名
        :return: boolean
        """
        sql_text = "NOAUDIT ALL ON {0}.{1}".format(owner, tablename)
        get_audit_sqltext_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not get_audit_sqltext_result.result:
            msg = "[{0}] excute close audit sql failed [{1}]".format(tablename, get_audit_sqltext_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False, get_audit_sqltext_result.message
        return get_audit_sqltext_result.result, get_audit_sqltext_result.message

    def excute_get_dump_dir_path_sql(self):
        """
         获取dump_dir的路径
        :return: boolean
        """
        sql_text = "select DIRECTORY_PATH from dba_directories where DIRECTORY_NAME='DUMP_DIR'"
        get_dumpdir_path_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
        if not get_dumpdir_path_result.result:
            msg = " excute get dump_dir path sql failed [{0}]".format(get_dumpdir_path_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False, msg
        return True, get_dumpdir_path_result.data

    def excute_table_rename_sql(self, owner, source_tablename, update_tablename):
        """
        执行表重命名的sql语句
        :param source_tablename: 原表名
        :param update_tablename: 修改表名
        :return: boolean
        """

        sql_text = "alter table {0}.{1} rename to {2}".format(owner, source_tablename, update_tablename)
        sqltext_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not sqltext_result.result:
            msg = "[{0}]  excute table rename sql failed [{1}]".format(source_tablename, sqltext_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False, sqltext_result.message
        return sqltext_result.result, sqltext_result.message

    def excute_drop_table_sql(self, owner, old_table_name):
        """
        执行删除old表名sql语句
        :param owner: user
        :param old_table_name: 表名
        :return: boolean
        """
        sql_text = "drop table {0}.{1}".format(owner, old_table_name)
        sqltext_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not sqltext_result.result:
            msg = "[{0}] excute drop table sql failed [{1}]".format(old_table_name, sqltext_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False, sqltext_result.message
        return sqltext_result.result, sqltext_result.message

    def excute_drop_partition_table_sql(self, owner, table_name, partition_name):
        """
        执行删除分区表的sql语句
        alter table ARKDATA.ACK_BALFUND drop partition SYS_P1161;
        :param owner: schema
        :param table_name: 表名
        :param partition_name: 分区表名
        :return: boolean
        """
        sql_text = "alter table {0}.{1} drop partition {2}".format(owner, table_name, partition_name)
        sqltext_result = self._oracle_handle_obj.ora_execute_dml_sql(sql_text)
        if not sqltext_result.result:
            msg = "[{0}.{1}] excute drop partition table [{2}] sql failed :[{3}]".\
                format(owner, table_name, partition_name, sqltext_result.message)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return False, sqltext_result.message
        return sqltext_result.result, sqltext_result.message

    def excute_select_index_name_by_tablename_sql(self, table_name):
        """
        执行根据表名查询索引名的sql语句
        SELECT index_name FROM dba_indexes a WHERE a.table_name  = 'S_TEST';
        :param table_name: 表名
        :return: boolean
        """
        sql_text = "select index_name from dba_indexes a where a.table_name='{0}'".format(table_name)
        query_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
        if not query_result.result:
            msg = "query failed [{0}] sql [{1}]".format(query_result.message, sql_text)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return None

        index_list = []
        for row in query_result.data:
            index_name = row[0]
            index_list.append(index_name)

        return index_list

    def excute_select_locality_by_indexname_sql(self, index_name):
        """
        执行根据锁i索引名查询索引是否是local的sql语句;若不存在；则为全局索引
        select table_name,index_name,LOCALITY from dba_part_indexes where index_name='S_TEST_IDX' ;
        :param index_name:
        :return: boolean
        """
        sql_text = "select table_name,index_name,LOCALITY from dba_part_indexes " \
                   "where index_name='{0}'".format(index_name)
        query_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
        if not query_result.result:
            msg = "query failed [{0}] sql [{1}]".format(query_result.message, sql_text)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return None

        locality_list = []
        for row in query_result.data:
            locality = row[2]
            locality_list.append({"locality": locality})

        return locality_list

    def get_explain(self, schema_name, sql_text) -> dict:
        """
        获取TABLE META 数据
        :param schema_name:
        :param sql_text:
        :return: dict {"result": True|False, "explaindata": [], "explaindesc":xxx, "errorinfo": "xxx" }
        """
        instance_name = self._oracle_handle_obj.get_instance_name()
        explain_sql = "explain plan for {0}".format(sql_text)
        try:
            ora_data = self._oracle_handle_obj.ora_execute_dml_sql(explain_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, explain_sql, ora_data.message)
                self.last_error_info = msg
                logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
                return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

        except Exception as ex:
            msg = "[{0}\n{1}] exec explain exception [{2}]".format(schema_name, explain_sql, ex)
            self.last_error_info = msg
            logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

        # select * from table(dbms_xplan.display_plan) 在plan_table只取最新的数据;
        # 获取最新的EXPLAIN数据
        plan_id_sql = "select max(plan_id) from plan_table"
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(plan_id_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, plan_id_sql, ora_data.message)
                self.last_error_info = msg
                logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
                return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

            plan_id = ora_data.data[0][0]

        except Exception as ex:
            msg = "[{0}\n{1}] get plan id exception [{2}]".format(schema_name, plan_id_sql, ex)
            self.last_error_info = msg
            logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

        # get plan_table data
        plan_table_sql = """select STATEMENT_ID, PLAN_ID, to_char(TIMESTAMP,'yyyy-mm-dd hh24:mi:ss'),
                         REMARKS, OPERATION, OPTIONS, OBJECT_NODE,
                         OBJECT_OWNER, OBJECT_NAME, OBJECT_ALIAS, OBJECT_INSTANCE, OBJECT_TYPE, OPTIMIZER,
                         SEARCH_COLUMNS, ID, PARENT_ID, DEPTH, POSITION, COST, CARDINALITY, BYTES, OTHER_TAG,
                         PARTITION_START, PARTITION_STOP, PARTITION_ID, OTHER, DISTRIBUTION, CPU_COST, IO_COST,
                         TEMP_SPACE, ACCESS_PREDICATES, FILTER_PREDICATES, PROJECTION, TIME, QBLOCK_NAME
                         from plan_table where plan_id={0} order by id""".format(plan_id)
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(plan_table_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, plan_table_sql, ora_data.message)
                self.last_error_info = msg
                logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
                return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

            plan_table_data = ora_data.data

        except Exception as ex:
            msg = "[{0}\n{1}] get plan_table exception [{2}]".format(schema_name, plan_table_sql, ex)
            self.last_error_info = msg
            logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

        # select * from table(dbms_xplan.display)
        explain_desc_sql = "select plan_table_output from table(dbms_xplan.display)"
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(explain_desc_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, explain_desc_sql, ora_data.message)
                self.last_error_info = msg
                logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
                return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

            explain_desc = ""
            for row in ora_data.data:
                explain_desc += row[0]
                explain_desc += "\n"

        except Exception as ex:
            msg = "[{0}\n{1}] get plan_table exception [{2}]".format(schema_name, explain_desc_sql, ex)
            self.last_error_info = msg
            logger.error("[0] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            return {"result": False, "explaindata": [], "explaindesc": "", "errorinfo": msg}

        return {"result": True, "instance_name": instance_name, "explaindata": plan_table_data,
                "explaindesc": explain_desc, "errorinfo": ""}

    def get_view_dba_tables(self, schema_name, table_name) -> dict:
        """
        get TABLE meta from dba_tables & dba_tab_comments
        :return: dict -> {
                "OWNER": str(owner),
                "TABLE_NAME": str(table_name),
                "TABLESPACE_NAME": str(tablespace_name),
                "STATUS": str(status),
                "NUM_ROWS": int(num_rows),
                "BLOCKS": int(blocks),
                "AVG_ROW_LEN": int(avg_row_len),
                "SAMPLE_SIZE": int(sample_size),
                "LAST_ANALYZED": str(last_analyzed),
                "PARTITIONED": str(partitioned),
                "READ_ONLY": str(read_only),
                "COMMENTS": str(comments)
            }
        """
        sql_text = "SELECT tab.*,comm.comments comments FROM (" \
                   "select OWNER,TABLE_NAME,TABLESPACE_NAME,STATUS,NUM_ROWS,BLOCKS,AVG_ROW_LEN,SAMPLE_SIZE, " \
                   "to_char(LAST_ANALYZED, 'yyyyhhmmhh24miss') LAST_ANALYZED,PARTITIONED,READ_ONLY from dba_tables " \
                   "where owner = '{0}' and table_name = '{1}') tab " \
                   "left join dba_tab_comments comm " \
                   "on tab.owner=comm.owner and tab.table_name=comm.table_name".\
            format(schema_name.upper(), table_name.upper())

        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
            if not ora_data.result:
                msg = "[{0}.{1}] get table info error [{2}\n{3}]".\
                    format(schema_name, table_name, ora_data.message, traceback.format_exc())
                self.last_error_info = msg
                return None

            # now_table_md5_dict = {}
            if len(ora_data.data) <= 0:
                msg = "[{0}.{1}] can not fine table".format(schema_name, table_name)
                self.last_error_info = msg
                return {}

            # 理论上只有一条数据
            if len(ora_data.data) != 1:
                msg = "[{0}.{1}] get table info error [table view data number: {2}]".\
                    format(schema_name, table_name, len(ora_data.data))
                self.last_error_info = msg
                return None

            view_table_data = ora_data.data[0]
            owner = view_table_data[0]
            table_name = view_table_data[1]
            tablespace_name = view_table_data[2]
            status = view_table_data[3]
            num_rows = view_table_data[4]
            blocks = view_table_data[5]
            avg_row_len = view_table_data[6]
            sample_size = view_table_data[7]
            last_analyzed = view_table_data[8]
            partitioned = view_table_data[9]
            read_only = view_table_data[10]
            comments = view_table_data[11]

            tab_dict = {
                "OWNER": "" if owner is None else str(owner),
                "TABLE_NAME": "" if table_name is None else str(table_name),
                "TABLESPACE_NAME": "" if tablespace_name is None else str(tablespace_name),
                "STATUS": "" if status is None else str(status),
                "NUM_ROWS": -1 if num_rows is None else int(num_rows),
                "BLOCKS": -1 if blocks is None else int(blocks),
                "AVG_ROW_LEN": -1 if avg_row_len is None else int(avg_row_len),
                "SAMPLE_SIZE": -1 if sample_size is None else int(sample_size),
                "LAST_ANALYZED": "" if last_analyzed is None else str(last_analyzed),
                "PARTITIONED": "" if partitioned is None else str(partitioned),
                "READ_ONLY": "" if read_only is None else str(read_only),
                "COMMENTS": "" if comments is None else str(comments)
            }
            return tab_dict

        except Exception as ex:
            msg = "[{0}.{1}] table meta exception [{2}\n{3}]".\
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None

    def get_view_tab_columns(self, schema_name, table_name) -> list:
        """
        column meta 信息
        :return: list -> [dict]
            [
                {
                    "OWNER": str(owner),
                    "TABLE_NAME": str(table_name),
                    "COLUMN_NAME": str(column_name),
                    "DATA_TYPE": "" if data_type is None else str(data_type),
                    "DATA_LENGTH": -1 if data_length is None else int(data_length),
                    "DATA_PRECISION": -1 if data_precision is None else int(data_precision),
                    "DATA_SCALE": -1 if data_scale is None else int(data_scale),
                    "NULLABLE": "" if nullable is None else str(nullable),
                    "COLUMN_ID": -1 if column_id is None else int(column_id),
                    "DEFAULT_LENGTH": -1 if default_length is None else int(default_length),
                    "DATA_DEFAULT": "" if data_default is None else str(data_default),
                    "NUM_DISTINCT": -1 if num_distinct is None else int(num_distinct),
                    "DENSITY": -1 if density is None else float(density),
                    "NUM_NULLS": -1 if num_nulls is None else int(num_nulls),
                    "NUM_BUCKETS": -1 if num_buckets is None else int(num_buckets),
                    "LAST_ANALYZED": "" if last_analyzed is None else str(last_analyzed),
                    "SAMPLE_SIZE": -1 if sample_size is None else int(sample_size),
                    "HISTOGRAM": "" if histogram is None else str(histogram),
                    "COMMENTS": "" if comments is None else str(comments)
                }
            ]
        """
        sql_text = "select col.*, " \
                   "comments.comments from (" \
                   "select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION, " \
                   "DATA_SCALE,NULLABLE,COLUMN_ID,DEFAULT_LENGTH,DATA_DEFAULT,NUM_DISTINCT,DENSITY,NUM_NULLS, " \
                   "NUM_BUCKETS,to_char(LAST_ANALYZED,'yyyymmddhh24miss') LAST_ANALYZED,SAMPLE_SIZE,HISTOGRAM " \
                   "from dba_tab_columns where owner='{0}' and table_name = '{1}' order by column_id) col " \
                   "left join dba_col_comments comments " \
                   "on col.owner = comments.owner and col.table_name = comments.table_name " \
                   "and col.column_name = comments.column_name".\
            format(schema_name.upper().strip(), table_name.upper().strip())
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
            if not ora_data.result:
                msg = "[{0}.{1} SQL: {3}] run sql error [{2}]".\
                    format(schema_name, table_name, ora_data.message, sql_text)
                self.last_error_info = msg
                return None

            col_meta = []
            for row in ora_data.data:
                owner = row[0]
                table_name = row[1]
                column_name = row[2]
                data_type = row[3]
                data_length = row[4]
                data_precision = row[5]
                data_scale = row[6]
                nullable = row[7]       # Y | N
                column_id = row[8]
                default_length = row[9]
                data_default = row[10]
                num_distinct = row[11]
                density = row[12]
                num_nulls = row[13]
                num_buckets = row[14]
                last_analyzed = row[15]
                sample_size = row[16]
                histogram = row[17]
                comments = row[18]

                col_dict = {
                    "OWNER": "" if owner is None else str(owner),
                    "TABLE_NAME": "" if table_name is None else str(table_name),
                    "COLUMN_NAME": "" if column_name is None else str(column_name),
                    "DATA_TYPE": "" if data_type is None else str(data_type),
                    "DATA_LENGTH": -1 if data_length is None else int(data_length),
                    "DATA_PRECISION": -1 if data_precision is None else int(data_precision),
                    "DATA_SCALE": -1 if data_scale is None else int(data_scale),
                    "NULLABLE": "" if nullable is None else str(nullable),
                    "COLUMN_ID": -1 if column_id is None else int(column_id),
                    "DEFAULT_LENGTH": -1 if default_length is None else int(default_length),
                    "DATA_DEFAULT": "" if data_default is None else str(data_default),
                    "NUM_DISTINCT": -1 if num_distinct is None else int(num_distinct),
                    "DENSITY": -1 if density is None else float(density),
                    "NUM_NULLS": -1 if num_nulls is None else int(num_nulls),
                    "NUM_BUCKETS": -1 if num_buckets is None else int(num_buckets),
                    "LAST_ANALYZED": "" if last_analyzed is None else str(last_analyzed),
                    "SAMPLE_SIZE": -1 if sample_size is None else int(sample_size),
                    "HISTOGRAM": "" if histogram is None else str(histogram),
                    "COMMENTS": "" if comments is None else str(comments)
                    }
                col_meta.append(col_dict)
            # end for
            return col_meta
        except Exception as ex:
            msg = "[{0}.{1}] get columns exception [{2}]".format(schema_name, table_name, ex)
            self.last_error_info = msg
            return None

    def get_view_dba_index(self, schema_name, table_name) -> list:
        """
        get dba_indexes & dba_constraints
        :return: list -> [dict]
            {
                "OWNER": str(idx_owner),
                "INDEX_NAME": str(idx_index_name),
                "INDEX_TYPE": str(idx_index_type),
                "TABLE_OWNER": str(idx_table_owner),
                "TABLE_NAME": str(idx_table_name),
                "UNIQUENESS": str(idx_uniqueness),
                "TABLESPACE_NAME": str(idx_tablespace_name),
                "BLEVEL": int(idx_blevel),
                "LEAF_BLOCKS": int(idx_leaf_blocks),
                "DISTINCT_KEYS": int(idx_distinct_keys),
                "STATUS": str(idx_status),
                "NUM_ROWS": int(idx_num_rows),
                "SAMPLE_SIZE": int(idx_sample_size),
                "LAST_ANALYZED": str(idx_last_analyzed),
                "PARTITIONED": str(idx_partitioned),
                "CONSTRAINT_NAME": str(idx_constraint_name),
                "CONSTRAINT_TYPE": str(idx_constraint_type),
                "INDEX_COLUMNS": idx_cols
            }

        """
        sql_text = "SELECT idx.*,con.constraint_name,con.constraint_type FROM ( " \
                   "select OWNER,INDEX_NAME,INDEX_TYPE,TABLE_OWNER,TABLE_NAME,UNIQUENESS, " \
                   "TABLESPACE_NAME,BLEVEL,LEAF_BLOCKS,DISTINCT_KEYS,STATUS,NUM_ROWS,SAMPLE_SIZE, " \
                   "to_char(LAST_ANALYZED,'YYYYMMDDHH24MISS') LAST_ANALYZED,PARTITIONED " \
                   "from dba_indexes where owner = '{0}' AND TABLE_NAME='{1}') idx " \
                   "left join dba_constraints con " \
                   "on idx.table_owner = con.owner " \
                   "and idx.table_name = con.table_name " \
                   "and idx.owner = con.index_owner " \
                   "and idx.index_name = con.index_name".format(schema_name.upper().strip(),
                                                                table_name.upper().strip())
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
            if not ora_data.result:
                msg = "[{0}.{1} SQL: {3}] run sql error [{2}]".\
                    format(schema_name, table_name, ora_data.message, sql_text)
                self.last_error_info = msg
                return None

            tab_index_meta = []
            for row in ora_data.data:
                idx_owner = row[0]
                idx_index_name = row[1]
                idx_index_type = row[2]
                idx_table_owner = row[3]
                idx_table_name = row[4]
                idx_uniqueness = row[5]
                idx_tablespace_name = row[6]
                idx_blevel = row[7]
                idx_leaf_blocks = row[8]
                idx_distinct_keys = row[9]
                idx_status = row[10]
                idx_num_rows = row[11]
                idx_sample_size = row[12]
                idx_last_analyzed = row[13]
                idx_partitioned = row[14]
                idx_constraint_name = row[15]
                idx_constraint_type = row[16]

                # INDEX_TYPE LIST
                # NORMAL
                # NORMAL/REV
                # BITMAP
                # FUNCTION-BASED NORMAL
                # FUNCTION-BASED NORMAL/REV
                # FUNCTION-BASED BITMAP
                # CLUSTER
                # IOT - TOP (索引组织表对应的索引，注意IOT - TOP 之间存有空格，个人观察：表的类型为IOT;
                #            对应索引类型为IOT - TOP;对应overflow表类型为IOT-OVERFLOW)
                # DOMAIN（自定义索引类型）

                # 索引只记录NORMAL类型索引
                if idx_index_type.upper() not in ("NORMAL", "FUNCTION-BASED NORMAL"):
                    msg = "{0}.{1} -> {2} [{3}] IS NOT NORMAL INDEX, META CAN NOT RECORD".format(
                        schema_name, table_name, idx_index_name, idx_index_type
                    )
                    logger.warning(msg)
                    continue

                idx_col_sql_text = "select column_name, descend " \
                                   "from dba_ind_columns " \
                                   "where index_name = '{0}' and table_owner = '{1}' and table_name = '{2}' " \
                                   "order by column_position".\
                    format(idx_index_name, idx_table_owner, idx_table_name)

                idx_col_data = self._oracle_handle_obj.ora_execute_query_get_all_data(idx_col_sql_text)
                if not idx_col_data.result:
                    msg = "[{0}.{1} SQL: {3}] run sql error [{2}]".\
                        format(schema_name, table_name, idx_col_data.message, sql_text)
                    logger.error(msg)
                    continue

                idx_cols = []
                for idx_col in idx_col_data.data:
                    idx_col_name = idx_col[0]
                    idx_col_descend = idx_col[1]
                    if idx_col_descend == 'ASC':
                        idx_cols.append("{0}".format(idx_col_name))
                    else:
                        idx_cols.append("{0} {1}".format(idx_col_name, idx_col_descend))

                idx_dict = {
                    "OWNER": "" if idx_owner is None else str(idx_owner),
                    "INDEX_NAME": "" if idx_index_name is None else str(idx_index_name),
                    "INDEX_TYPE": "" if idx_index_type is None else str(idx_index_type),
                    "TABLE_OWNER": "" if idx_table_owner is None else str(idx_table_owner),
                    "TABLE_NAME": "" if idx_table_name is None else str(idx_table_name),
                    "UNIQUENESS": "" if idx_uniqueness is None else str(idx_uniqueness),
                    "TABLESPACE_NAME": "" if idx_tablespace_name is None else str(idx_tablespace_name),
                    "BLEVEL": -1 if idx_blevel is None else int(idx_blevel),
                    "LEAF_BLOCKS": -1 if idx_leaf_blocks is None else int(idx_leaf_blocks),
                    "DISTINCT_KEYS": -1 if idx_distinct_keys is None else int(idx_distinct_keys),
                    "STATUS": "" if idx_status is None else str(idx_status),
                    "NUM_ROWS": -1 if idx_num_rows is None else int(idx_num_rows),
                    "SAMPLE_SIZE": -1 if idx_sample_size is None else int(idx_sample_size),
                    "LAST_ANALYZED": "" if idx_last_analyzed is None else str(idx_last_analyzed),
                    "PARTITIONED": "" if idx_partitioned is None else str(idx_partitioned),
                    "CONSTRAINT_NAME": "" if idx_constraint_name is None else str(idx_constraint_name),
                    "CONSTRAINT_TYPE": "" if idx_constraint_type is None else str(idx_constraint_type),
                    "INDEX_COLUMNS": idx_cols
                }
                tab_index_meta.append(idx_dict)
            # end for
            return tab_index_meta
        except Exception as ex:
            msg = "[{0}.{1}] handle index meta exception [{2}\n{3}]".\
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None

    def get_view_dba_part_tables(self, schema_name, table_name) -> dict:
        """
        get dba_part_tables
        :return: list -> [dict]
            {
                "PARTITIONING_TYPE": str(part_partitioning_type),
                "SUBPARTITIONING_TYPE": str(part_subpartitioning_type),
                "PARTITIONING_KEY_COUNT": int(part_partitioning_key_count),
                "STATUS": str(part_status),
                "INTERVAL": str(part_interval),
                "PART_COLUMNS": part_cols
            }

        """
        sql_text = "select partitioning_type,subpartitioning_type,partitioning_key_count," \
                   "status,interval from dba_part_tables " \
                   "where owner='{0}' and table_name = '{1}'".\
            format(schema_name.upper().strip(), table_name.upper().strip())
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
            if not ora_data.result:
                msg = "[{0}.{1} SQL: {3}] run sql error [{2}]".\
                    format(schema_name, table_name, ora_data.message, sql_text)
                self.last_error_info = msg
                return None

            part_tab_dict = {}
            for row in ora_data.data:
                part_partitioning_type = row[0]
                part_subpartitioning_type = row[1]
                part_partitioning_key_count = row[2]
                part_status = row[3]
                part_interval = row[4]

                part_col_sql_text = "select column_name " \
                                    "from dba_part_key_columns " \
                                    "where owner='{0}' and name = '{1}' and object_type = 'TABLE' " \
                                    "order by column_position".\
                    format(schema_name.upper().strip(), table_name.upper().strip())

                part_col_data = self._oracle_handle_obj.ora_execute_query_get_all_data(part_col_sql_text)
                if not part_col_data.result:
                    msg = "[{0}.{1} SQL: {3}] run sql error [{2}]".\
                        format(schema_name, table_name, part_col_data.message, sql_text)
                    self.last_error_info = msg
                    return None

                part_cols = []
                for col_row in part_col_data.data:
                    part_col = col_row[0]
                    if part_col is not None:
                        part_cols.append(str(part_col))
                # end for

                part_tab_dict = {
                    "PARTITIONING_TYPE": "" if part_partitioning_type is None else str(part_partitioning_type),
                    "SUBPARTITIONING_TYPE": "" if part_subpartitioning_type is None else str(part_subpartitioning_type),
                    "PARTITIONING_KEY_COUNT": -1 if part_partitioning_key_count is None
                    else int(part_partitioning_key_count),
                    "STATUS": "" if part_status is None else str(part_status),
                    "INTERVAL": "" if part_interval is None else str(part_interval),
                    "PART_COLUMNS": part_cols
                }
            # end for
            return part_tab_dict
        except Exception as ex:
            msg = "[{0}.{1}] handle part table exception [{2}\n{3}]".\
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None

    def get_view_dba_tab_privs(self, schema_name, table_name) -> list:
        """
        get dba_tab_privs
        :return: list -> [dict]
            {
                'priv_grantee': priv_grantee,
                'priv_privilege': priv_privilege
            }
        """
        sql_text = "select OWNER,TABLE_NAME,GRANTOR,GRANTEE,PRIVILEGE,GRANTABLE " \
                   "from dba_tab_privs " \
                   "where owner = '{0}' and table_name='{1}'".\
            format(schema_name.upper().strip(), table_name.upper().strip())
        table_privileges = []
        try:
            ora_data = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)
            if not ora_data.result:
                msg = "[{0}.{1} SQL: {3}] run sql error [{2}]".\
                    format(schema_name, table_name, ora_data.message, sql_text)
                logger.error(msg)
                return None

            for row in ora_data.data:
                priv_owner = row[0]
                priv_table_name = row[1]
                priv_grantor = row[2]
                priv_grantee = row[3]
                priv_privilege = row[4]
                priv_grantable = row[5]

                # 所有的权限都取出来, 交给客户端进行数据处理; 剔除非法用户等操作
                if priv_privilege not in ["SELECT", "INSERT", "UPDATE",
                                          "DELETE", "ALTER", "DEBUG", "INDEX", "FLASHBACK"]:
                    continue

                priv_dict = {
                    'GRANTEE': "" if priv_grantee is None else str(priv_grantee),
                    'PRIVILEGE': "" if priv_privilege is None else str(priv_privilege)
                }
                table_privileges.append(priv_dict)
            # end for
            return table_privileges
        except Exception as ex:
            msg = "[{0}.{1}] special privilege exception [{2}\n{3}]".\
                format(schema_name, table_name, ex, traceback.format_exc())
            logger.error(msg)
            return None

    def get_view_dba_histogram(self, schema_name, table_name) -> list:
        """
        get dba_histograms
        :param schema_name:
        :param table_name:
        :return: list -> tuple(COLUMN_NAME,ENDPOINT_NUMBER,ENDPOINT_NUMBER,ENDPOINT_ACTUAL_VALUE)
        """
        sql_text = "select COLUMN_NAME,ENDPOINT_NUMBER,ENDPOINT_NUMBER,ENDPOINT_ACTUAL_VALUE " \
                   "from dba_histograms where owner='{0}' and table_name='{1}'".\
            format(schema_name.upper(), table_name.upper())
        ora_result = self._oracle_handle_obj.ora_execute_query_get_all_data(sql_text)

        if not ora_result.result:
            self.last_error_info = ora_result.message
            return None
        return ora_result.data


