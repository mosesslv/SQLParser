# -*- coding: UTF-8 -*-

from common.MysqlHandle import MysqlHandle
import common.utils as utils
import prettytable
import traceback
import pandas
import logging
logger = logging.getLogger("")


class MysqlCommon:
    """
    MYSQL 管理, 完成 mysql 基本的操作方法
    """

    def __init__(self, mysql_handle_obj):
        """
        此类不关心创建连接, 连接对象需要有操作权限, 没有权限当失败处理
        :param mysql_handle_obj: mysql 连接, 此类不关心DB建连
        :return:
        """
        assert isinstance(mysql_handle_obj, MysqlHandle)
        if mysql_handle_obj is None:
            raise Exception("mysql connection invalid, object is none")

        self._mysql_handle_obj = mysql_handle_obj
        self.last_error_info = ""

    def create_database(self, database_name):
        """
        mysql 创建 database
        :param database_name:
        :return: boolean
        """
        sql_text = "create database {0}".format(database_name)
        create_db_result = self._mysql_handle_obj.mysql_execute_dml_sql(sql_text)
        if not create_db_result.result:
            msg = "create database [{0}] failed [{1}]".format(database_name, create_db_result.message)
            self.last_error_info = msg
            return False

        return True

    def create_mysql_user(self, username, passwd):
        """
        mysql 创建用户

        create user 'dbcm'@'%' identified by 'bettle@lufax.com';
        flush privileges;
        :param username:
        :param passwd:
        :return: boolean
        """
        sql_text = "create user '{0}'@'%' identified by '{1}'".format(username, passwd)
        create_user_result = self._mysql_handle_obj.mysql_execute_dml_sql(sql_text)
        if not create_user_result.result:
            msg = "create user [{0}] failed [{1}]".format(username, create_user_result.message)
            self.last_error_info = msg
            return False

        flush_sql = "flush privileges"
        flush_result = self._mysql_handle_obj.mysql_execute_dml_sql(flush_sql)
        if not flush_result.result:
            msg = "flush privileges failed [{0}]".format(username, flush_result.message)
            self.last_error_info = msg
            return False

        return True

    def rename_table_name(self, database_name, table_name, table_name_new):
        """
        mysql 重命名表名

        create user 'dbcm'@'%' identified by 'bettle@lufax.com';
        flush privileges;
        :param database_name:
        :param table_name:
        :param table_name_new:
        :return: boolean
        """
        sql_text_use_db = "use {0}".format(database_name)
        use_result = self._mysql_handle_obj.mysql_execute_dml_sql(sql_text_use_db)
        if not use_result.result:
            msg = "use db [{0}] failed [{1}]".format(database_name, use_result.message)
            self.last_error_info = msg
            return False

        sql_text_alter = "alter table {0} rename to {1}".format(table_name, table_name_new)
        alter_result = self._mysql_handle_obj.mysql_execute_dml_sql(sql_text_alter)
        if not alter_result.result:
            msg = "rename table [{0}] failed [{1}]".format(table_name, alter_result.message)
            self.last_error_info = msg
            return False

        flush_sql = "flush privileges"
        flush_result = self._mysql_handle_obj.mysql_execute_dml_sql(flush_sql)
        if not flush_result.result:
            msg = "flush privileges failed [{0}]".format(flush_result.message)
            self.last_error_info = msg
            return False

        return True

    def excute_drop_partition_table_sql(self, database, table_name, partition_name):
        """
        执行删除分区表的sql语句
        alter table ARKDATA.ACK_BALFUND drop partition SYS_P1161;
        :param database: schema
        :param table_name: 表名
        :param partition_name: 分区表名
        :return: boolean
        """
        sql_text = "alter table {0}.{1} drop partition {2}".format(database, table_name, partition_name)
        sqltext_result = self._mysql_handle_obj.mysql_execute_dml_sql(sql_text)
        if not sqltext_result.result:
            msg = "[{0}.{1}] excute drop partition table [{2}] sql failed :[{3}]".\
                format(database, table_name, partition_name, sqltext_result.message)
            self.last_error_info = msg
            return False
        return True

    def excute_system_variables_modify(self, parameter_key, parameter_value):
        """
        执行mysql系统变量修改的sql语句
        set global parameter_key=parameter_value;
        :param parameter_key:  系统变量名
        :param parameter_value: 修改后的value
        :return: boolean
        """
        sql_text = "set global {0}={1}".format(parameter_key, parameter_value)
        sqltext_result = self._mysql_handle_obj.mysql_execute_dml_sql(sql_text)
        if not sqltext_result.result:
            msg = "excute mysql system variables modify sql[{0}] failed :[{1}]".format(sql_text, sqltext_result.message)
            self.last_error_info = msg
            return False
        return True

    def get_explain(self, schema_name, sql_text):
        """
        获取TABLE META 数据
        :param schema_name:
        :param sql_text:
        :return: dict {"result": True|False, "db_type": xxx, "explaindata": [], "explaindesc":xxx }
        """
        if self._mysql_handle_obj is None:
            msg = "get mysql connection invalid [{0}]".format(schema_name)
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        use_sql = "use {0}".format(schema_name)
        try:
            use_result = self._mysql_handle_obj.mysql_execute_dml_sql(use_sql)
            if not use_result.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, use_sql, use_result.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}
        except Exception as ex:
            msg = "[{0}\n{1}] switch database exception [{2}\n{3}]". \
                format(schema_name, use_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        explain_sql = "explain {0}".format(sql_text)
        try:
            my_data = self._mysql_handle_obj.mysql_execute_query_get_all_data(explain_sql)
            if not my_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, explain_sql, my_data.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}
            else:
                if len(my_data.data) <= 0:
                    msg = "nothing explain data"
                    self.last_error_info = msg
                    return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

                display_columns = self._mysql_handle_obj.get_sql_display_column_names()
                if display_columns is None:
                    msg = "get display column name failed"
                    self.last_error_info = msg
                    return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

                headers = []
                for col_name in display_columns:
                    headers.append(col_name)

                t = prettytable.PrettyTable(headers)
                for header in headers:
                    t.align[header] = "l"

                for row in my_data.data:
                    t.add_row(row)

                explaindesc = t.get_string()

        except Exception as ex:
            msg = "[{0}\n{1}] exec explain exception [{2}\n{3}]".\
                format(schema_name, explain_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        return {"result": True, "db_type": "MYSQL", "explaindata": my_data.data,
                "explaindesc": explaindesc, "errorinfo": ""}

    def get_explain_from_pandas(self, schema_name, sql_text):
        """
        获取TABLE META 数据
        :param schema_name:
        :param sql_text:
        :return: dict {"result": True|False, "db_type": xxx, "explaindata": [], "explaindesc":xxx }
        """
        if self._mysql_handle_obj is None:
            msg = "get mysql connection invalid [{0}]".format(schema_name)
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        use_sql = "use {0}".format(schema_name)
        try:
            use_result = self._mysql_handle_obj.mysql_execute_dml_sql(use_sql)
            if not use_result.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, use_sql, use_result.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}
        except Exception as ex:
            msg = "[{0}\n{1}] switch database exception [{2}\n{3}]". \
                format(schema_name, use_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        explain_sql = "explain {0}".format(sql_text)
        try:
            explain_pandas_data = pandas.read_sql(explain_sql, self._mysql_handle_obj.connection)
            headers = [c for c in explain_pandas_data.columns.values]

            t = prettytable.PrettyTable(headers)
            for header in headers:
                t.align[header] = "l"

            for i in explain_pandas_data.index:
                row = explain_pandas_data.loc[i]
                t.add_row(row)
            explaindesc = t.get_string()
        except Exception as ex:
            # msg = "[{0}\n{1}] exec explain exception [{2}\n{3}]". \
            #     format(schema_name, explain_sql, ex, traceback.format_exc())
            msg = str(ex)
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        return {"result": True, "db_type": "MYSQL", "explaindata": explain_pandas_data,
                "explaindesc": explaindesc, "errorinfo": ""}

    def get_meta_tables(self, schema_name, table_name) -> dict:
        """
        get TABLE meta from information_schema.tables
        :return: dict -> {
                "TABLE_CATALOG": str,
                "TABLE_SCHEMA": str,
                "TABLE_NAME": str,
                "TABLE_TYPE": str,
                "ENGINE": str,
                "VERSION": int,
                "ROW_FORMAT": str,
                "TABLE_ROWS": int,
                "AVG_ROW_LENGTH": int,
                "DATA_LENGTH": int,
                "MAX_DATA_LENGTH": int,
                "INDEX_LENGTH": int,
                "DATA_FREE": int,
                "AUTO_INCREMENT": int,
                "CREATE_TIME": str -> "%Y%m%d%H%M%S%f",
                "UPDATE_TIME": str -> "%Y%m%d%H%M%S%f",
                "CHECK_TIME": str -> "%Y%m%d%H%M%S%f",
                "TABLE_COLLATION": str,
                "CHECKSUM": int,
                "CREATE_OPTIONS": str,
                "TABLE_COMMENT": str,
            }

+-----------------+---------------------+------+-----+---------+-------+
| Field           | Type                | Null | Key | Default | Extra |
+-----------------+---------------------+------+-----+---------+-------+
| TABLE_CATALOG   | varchar(512)        | NO   |     |         |       |
| TABLE_SCHEMA    | varchar(64)         | NO   |     |         |       |
| TABLE_NAME      | varchar(64)         | NO   |     |         |       |
| TABLE_TYPE      | varchar(64)         | NO   |     |         |       |
| ENGINE          | varchar(64)         | YES  |     | NULL    |       |
| VERSION         | bigint(21) unsigned | YES  |     | NULL    |       |
| ROW_FORMAT      | varchar(10)         | YES  |     | NULL    |       |
| TABLE_ROWS      | bigint(21) unsigned | YES  |     | NULL    |       |
| AVG_ROW_LENGTH  | bigint(21) unsigned | YES  |     | NULL    |       |
| DATA_LENGTH     | bigint(21) unsigned | YES  |     | NULL    |       |
| MAX_DATA_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
| INDEX_LENGTH    | bigint(21) unsigned | YES  |     | NULL    |       |
| DATA_FREE       | bigint(21) unsigned | YES  |     | NULL    |       |
| AUTO_INCREMENT  | bigint(21) unsigned | YES  |     | NULL    |       |
| CREATE_TIME     | datetime            | YES  |     | NULL    |       |
| UPDATE_TIME     | datetime            | YES  |     | NULL    |       |
| CHECK_TIME      | datetime            | YES  |     | NULL    |       |
| TABLE_COLLATION | varchar(32)         | YES  |     | NULL    |       |
| CHECKSUM        | bigint(21) unsigned | YES  |     | NULL    |       |
| CREATE_OPTIONS  | varchar(255)        | YES  |     | NULL    |       |
| TABLE_COMMENT   | varchar(2048)       | NO   |     |         |       |
+-----------------+---------------------+------+-----+---------+-------+

        """
        sql_text = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, ROW_FORMAT, "\
                   "TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, "\
                   "AUTO_INCREMENT, CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, CHECKSUM, "\
                   "CREATE_OPTIONS, TABLE_COMMENT FROM information_schema.TABLES "\
                   "WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'".format(schema_name, table_name)
        try:
            my_data = self._mysql_handle_obj.mysql_execute_query_get_all_data(sql_text)
            if not my_data.result:
                msg = "[{0}.{1}] get table info error [{2}\n{3}]".\
                    format(schema_name, table_name, my_data.message, traceback.format_exc())
                self.last_error_info = msg
                return None

            # now_table_md5_dict = {}
            if len(my_data.data) <= 0:
                msg = "[{0}.{1}] can not fine table".format(schema_name, table_name)
                self.last_error_info = msg
                return {}

            # 理论上只有一条数据
            if len(my_data.data) != 1:
                msg = "[{0}.{1}] get table info error [table view data number: {2}]".\
                    format(schema_name, table_name, len(my_data.data))
                self.last_error_info = msg
                return None

            meta_table_data = my_data.data[0]
            table_catalog = meta_table_data[0]
            table_schema = meta_table_data[1]
            table_name = meta_table_data[2]
            table_type = meta_table_data[3]
            engine = meta_table_data[4]
            version = meta_table_data[5]
            row_format = meta_table_data[6]
            table_rows = meta_table_data[7]
            avg_row_length = meta_table_data[8]
            data_length = meta_table_data[9]
            max_data_length = meta_table_data[10]
            index_length = meta_table_data[11]
            data_free = meta_table_data[12]
            auto_increment = meta_table_data[13]
            create_time = meta_table_data[14]
            update_time = meta_table_data[15]
            check_time = meta_table_data[16]
            table_collation = meta_table_data[17]
            checksum = meta_table_data[18]
            create_options = meta_table_data[19]
            table_comment = meta_table_data[20]

            tab_dict = {
                "TABLE_CATALOG": "" if utils.str_is_none_or_empty(table_catalog) else str(table_catalog),
                "TABLE_SCHEMA": "" if utils.str_is_none_or_empty(table_schema) else str(table_schema),
                "TABLE_NAME": "" if utils.str_is_none_or_empty(table_name) else str(table_name),
                "TABLE_TYPE": "" if utils.str_is_none_or_empty(table_type) else str(table_type),
                "ENGINE": "" if utils.str_is_none_or_empty(engine) else str(engine),
                "VERSION": -1 if version is None else int(version),
                "ROW_FORMAT": "" if utils.str_is_none_or_empty(row_format) else str(row_format),
                "TABLE_ROWS": -1 if table_rows is None else int(table_rows),
                "AVG_ROW_LENGTH": -1 if avg_row_length is None else int(avg_row_length),
                "DATA_LENGTH": -1 if data_length is None else int(data_length),
                "MAX_DATA_LENGTH": -1 if max_data_length is None else int(max_data_length),
                "INDEX_LENGTH": -1 if index_length is None else int(index_length),
                "DATA_FREE": -1 if data_free is None else int(data_free),
                "AUTO_INCREMENT": -1 if auto_increment is None else int(auto_increment),
                "CREATE_TIME": "" if create_time is None else create_time.strftime("%Y%m%d%H%M%S%f"),
                "UPDATE_TIME": "" if update_time is None else update_time.strftime("%Y%m%d%H%M%S%f"),
                "CHECK_TIME": "" if check_time is None else check_time.strftime("%Y%m%d%H%M%S%f"),
                "TABLE_COLLATION": "" if utils.str_is_none_or_empty(table_collation) else str(table_collation),
                "CHECKSUM": -1 if checksum is None else int(checksum),
                "CREATE_OPTIONS": "" if utils.str_is_none_or_empty(create_options) else str(create_options),
                "TABLE_COMMENT": "" if utils.str_is_none_or_empty(table_comment) else str(table_comment),
            }
            return tab_dict

        except Exception as ex:
            msg = "[{0}.{1}] table meta exception [{2}\n{3}]".\
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None

    def get_meta_partition(self, schema_name, table_name) -> [tuple, list]:
        """
        get partition meta from information_schema.partitions
        :return: tuple : 表头
                 list: item -> tuple
+-------------------------------+---------------------+------+-----+---------+-------+
| Field                         | Type                | Null | Key | Default | Extra |
+-------------------------------+---------------------+------+-----+---------+-------+
| TABLE_CATALOG                 | varchar(512)        | NO   |     |         |       |
| TABLE_SCHEMA                  | varchar(64)         | NO   |     |         |       |
| TABLE_NAME                    | varchar(64)         | NO   |     |         |       |
| PARTITION_NAME                | varchar(64)         | YES  |     | NULL    |       |
| SUBPARTITION_NAME             | varchar(64)         | YES  |     | NULL    |       |
| PARTITION_ORDINAL_POSITION    | bigint(21) unsigned | YES  |     | NULL    |       |
| SUBPARTITION_ORDINAL_POSITION | bigint(21) unsigned | YES  |     | NULL    |       |
| PARTITION_METHOD              | varchar(18)         | YES  |     | NULL    |       |
| SUBPARTITION_METHOD           | varchar(12)         | YES  |     | NULL    |       |
| PARTITION_EXPRESSION          | longtext            | YES  |     | NULL    |       |
| SUBPARTITION_EXPRESSION       | longtext            | YES  |     | NULL    |       |
| PARTITION_DESCRIPTION         | longtext            | YES  |     | NULL    |       |
| TABLE_ROWS                    | bigint(21) unsigned | NO   |     | 0       |       |
| AVG_ROW_LENGTH                | bigint(21) unsigned | NO   |     | 0       |       |
| DATA_LENGTH                   | bigint(21) unsigned | NO   |     | 0       |       |
| MAX_DATA_LENGTH               | bigint(21) unsigned | YES  |     | NULL    |       |
| INDEX_LENGTH                  | bigint(21) unsigned | NO   |     | 0       |       |
| DATA_FREE                     | bigint(21) unsigned | NO   |     | 0       |       |
| CREATE_TIME                   | datetime            | YES  |     | NULL    |       |
| UPDATE_TIME                   | datetime            | YES  |     | NULL    |       |
| CHECK_TIME                    | datetime            | YES  |     | NULL    |       |
| CHECKSUM                      | bigint(21) unsigned | YES  |     | NULL    |       |
| PARTITION_COMMENT             | varchar(80)         | NO   |     |         |       |
| NODEGROUP                     | varchar(12)         | NO   |     |         |       |
| TABLESPACE_NAME               | varchar(64)         | YES  |     | NULL    |       |
+-------------------------------+---------------------+------+-----+---------+-------+
        """
        sql_text = "select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,SUBPARTITION_NAME, "\
                   "PARTITION_ORDINAL_POSITION,SUBPARTITION_ORDINAL_POSITION,PARTITION_METHOD, "\
                   "SUBPARTITION_METHOD,PARTITION_EXPRESSION,SUBPARTITION_EXPRESSION,PARTITION_DESCRIPTION, "\
                   "TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,CREATE_TIME, "\
                   "UPDATE_TIME,CHECK_TIME,CHECKSUM,PARTITION_COMMENT,NODEGROUP,TABLESPACE_NAME "\
                   "from information_schema.PARTITIONS WHERE TABLE_SCHEMA='{0}' AND TABLE_NAME = '{1}' "\
                   "order by PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION".format(schema_name, table_name)
        try:
            my_data = self._mysql_handle_obj.mysql_execute_query_get_all_data(sql_text)
            if not my_data.result:
                msg = "[{0}.{1}] get partition info error [{2}]". \
                    format(schema_name, table_name, my_data.message)
                self.last_error_info = msg
                return None

            column_names = self._mysql_handle_obj.get_sql_display_column_names()
            return column_names, my_data.data
        except Exception as ex:
            msg = "[{0}.{1}] patitions meta exception [{2}\n{3}]". \
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None

    def get_meta_columns(self, schema_name, table_name) -> [tuple, list]:
        """
        get column meta from information_schema.COLUMNS
        :return: tuple : 表头
                 list: item -> tuple
+--------------------------+---------------------+------+-----+---------+-------+
| Field                    | Type                | Null | Key | Default | Extra |
+--------------------------+---------------------+------+-----+---------+-------+
| TABLE_CATALOG            | varchar(512)        | NO   |     |         |       |
| TABLE_SCHEMA             | varchar(64)         | NO   |     |         |       |
| TABLE_NAME               | varchar(64)         | NO   |     |         |       |
| COLUMN_NAME              | varchar(64)         | NO   |     |         |       |
| ORDINAL_POSITION         | bigint(21) unsigned | NO   |     | 0       |       |
| COLUMN_DEFAULT           | longtext            | YES  |     | NULL    |       |
| IS_NULLABLE              | varchar(3)          | NO   |     |         |       |
| DATA_TYPE                | varchar(64)         | NO   |     |         |       |
| CHARACTER_MAXIMUM_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
| CHARACTER_OCTET_LENGTH   | bigint(21) unsigned | YES  |     | NULL    |       |
| NUMERIC_PRECISION        | bigint(21) unsigned | YES  |     | NULL    |       |
| NUMERIC_SCALE            | bigint(21) unsigned | YES  |     | NULL    |       |
| DATETIME_PRECISION       | bigint(21) unsigned | YES  |     | NULL    |       |
| CHARACTER_SET_NAME       | varchar(32)         | YES  |     | NULL    |       |
| COLLATION_NAME           | varchar(32)         | YES  |     | NULL    |       |
| COLUMN_TYPE              | longtext            | NO   |     | NULL    |       |
| COLUMN_KEY               | varchar(3)          | NO   |     |         |       |
| EXTRA                    | varchar(30)         | NO   |     |         |       |
| PRIVILEGES               | varchar(80)         | NO   |     |         |       |
| COLUMN_COMMENT           | varchar(1024)       | NO   |     |         |       |
| GENERATION_EXPRESSION    | longtext            | NO   |     | NULL    |       | -> 5.7 +
+--------------------------+---------------------+------+-----+---------+-------+
        """
        sql_text = "select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT, "\
                   "IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION, "\
                   "NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY, "\
                   "EXTRA,PRIVILEGES,COLUMN_COMMENT "\
                   "from information_schema.COLUMNS where TABLE_SCHEMA='{0}' AND TABLE_NAME = '{1}' "\
                   "order by ORDINAL_POSITION".format(schema_name, table_name)
        try:
            my_data = self._mysql_handle_obj.mysql_execute_query_get_all_data(sql_text)
            if not my_data.result:
                msg = "[{0}.{1}] get column meta error [{2}]".\
                    format(schema_name, table_name, my_data.message)
                self.last_error_info = msg
                return None

            column_names = self._mysql_handle_obj.get_sql_display_column_names()
            return column_names, my_data.data
        except Exception as ex:
            msg = "[{0}.{1}] column meta exception [{2}\n{3}]". \
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None

    def get_meta_index(self, schema_name, table_name) -> [tuple, list]:
        """
        get index meta from information_schema.STATISTICS
        :return: tuple : 表头
                 list: item -> tuple
+---------------+---------------+------+-----+---------+-------+
| Field         | Type          | Null | Key | Default | Extra |
+---------------+---------------+------+-----+---------+-------+
| TABLE_CATALOG | varchar(512)  | NO   |     |         |       |
| TABLE_SCHEMA  | varchar(64)   | NO   |     |         |       |
| TABLE_NAME    | varchar(64)   | NO   |     |         |       |
| NON_UNIQUE    | bigint(1)     | NO   |     | 0       |       |
| INDEX_SCHEMA  | varchar(64)   | NO   |     |         |       |
| INDEX_NAME    | varchar(64)   | NO   |     |         |       |
| SEQ_IN_INDEX  | bigint(2)     | NO   |     | 0       |       |
| COLUMN_NAME   | varchar(64)   | NO   |     |         |       |
| COLLATION     | varchar(1)    | YES  |     | NULL    |       |
| CARDINALITY   | bigint(21)    | YES  |     | NULL    |       |
| SUB_PART      | bigint(3)     | YES  |     | NULL    |       |
| PACKED        | varchar(10)   | YES  |     | NULL    |       |
| NULLABLE      | varchar(3)    | NO   |     |         |       |
| INDEX_TYPE    | varchar(16)   | NO   |     |         |       |
| COMMENT       | varchar(16)   | YES  |     | NULL    |       |
| INDEX_COMMENT | varchar(1024) | NO   |     |         |       |
+---------------+---------------+------+-----+---------+-------+

        """
        sql_text = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, NON_UNIQUE, INDEX_SCHEMA, INDEX_NAME, "\
                   "SEQ_IN_INDEX, COLUMN_NAME, COLLATION, CARDINALITY, SUB_PART, PACKED, NULLABLE, INDEX_TYPE, "\
                   "COMMENT, INDEX_COMMENT "\
                   "FROM information_schema.STATISTICS where TABLE_SCHEMA='{0}' AND TABLE_NAME = '{1}' "\
                   "order by INDEX_SCHEMA, INDEX_NAME, SEQ_IN_INDEX".format(schema_name, table_name)
        try:
            my_data = self._mysql_handle_obj.mysql_execute_query_get_all_data(sql_text)
            if not my_data.result:
                msg = "[{0}.{1}] get index meta error [{2}]".\
                    format(schema_name, table_name, my_data.message)
                self.last_error_info = msg
                return None

            column_names = self._mysql_handle_obj.get_sql_display_column_names()
            return column_names, my_data.data
        except Exception as ex:
            msg = "[{0}.{1}] index meta exception [{2}\n{3}]". \
                format(schema_name, table_name, ex, traceback.format_exc())
            self.last_error_info = msg
            return None
