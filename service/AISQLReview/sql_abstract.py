# -*- coding: UTF-8 -*-

"""
oracle data struct:
    sequence                    : unique data (uuid)
    data_handle_result          : 数据处理结果 boolean
    message                     : 数据处理消息, 一般反映错误内容
    oracle_conn                 : OracleHandle -> Oracle 连接, 本类不考虑连接方法及连接权限等问题
    schema_name                 : 必填信息
    sql_text                    : 必填信息
    sql_ast                     : 抽象语法树
    table_names                 : SQL中表名的列表
    plan_text                   : 执行计划展示部分

    plan_raw                    : 执行计划裸数据 plan_raw: -> list
        [tuple -> (STATEMENT_ID, PLAN_ID, to_char(TIMESTAMP,'yyyy-mm-dd hh24:mi:ss'),
         REMARKS, OPERATION, OPTIONS, OBJECT_NODE,
         OBJECT_OWNER, OBJECT_NAME, OBJECT_ALIAS, OBJECT_INSTANCE, OBJECT_TYPE, OPTIMIZER,
         SEARCH_COLUMNS, ID, PARENT_ID, DEPTH, POSITION, COST, CARDINALITY, BYTES, OTHER_TAG,
         PARTITION_START, PARTITION_STOP, PARTITION_ID, OTHER, DISTRIBUTION, CPU_COST, IO_COST,
         TEMP_SPACE, ACCESS_PREDICATES, FILTER_PREDICATES, PROJECTION, TIME, QBLOCK_NAME)]

         select STATEMENT_ID, PLAN_ID, to_char(TIMESTAMP,'yyyy-mm-dd hh24:mi:ss'),
         REMARKS, OPERATION, OPTIONS, OBJECT_NODE,
         OBJECT_OWNER, OBJECT_NAME, OBJECT_ALIAS, OBJECT_INSTANCE, OBJECT_TYPE, OPTIMIZER,
         SEARCH_COLUMNS, ID, PARENT_ID, DEPTH, POSITION, COST, CARDINALITY, BYTES, OTHER_TAG,
         PARTITION_START, PARTITION_STOP, PARTITION_ID, OTHER, DISTRIBUTION, CPU_COST, IO_COST,
         TEMP_SPACE, ACCESS_PREDICATES, FILTER_PREDICATES, PROJECTION, TIME, QBLOCK_NAME
         from plan_table where plan_id={0} order by id

    tab_info                    : 表的元数据 -> list[object -> OracleTableMeta]

    view_histogram              : 直方图数据 -> list
        [tuple -> (COLUMN_NAME,ENDPOINT_NUMBER,ENDPOINT_NUMBER,ENDPOINT_ACTUAL_VALUE)]
        "select COLUMN_NAME,ENDPOINT_NUMBER,ENDPOINT_NUMBER,ENDPOINT_ACTUAL_VALUE " \
                   "from dba_histograms where owner='{0}' and table_name='{1}'"

    addition : -> list [dict]
        "Discrimination"区分度 : [
            {
                "schema_name": "xxxx",          # 兼容老版本的协议, 新版本此KEY没有使用
                "name": "xxx,xxx,xxx",          # 字段名称, ","分割表示多字段
                "type": "COLUMN | VIEW",        # 类型标识, 区分name的属性
                "sql_text": "xxxxxx",           # 获取区分度信息的SQL
                "value": list                   # 区分度值
            }
        ]

    statement                   : SQL的句式
    has_dynamic_mosaicking      : 动态拼接标识

    ai_result                   : AI处理结果
    ai_recommend                : AI建议

"""

class OracleSQLStruct:
    """
    定义 ORACLE SQL 数据结构
    所有的数据对应到对象属性
    所有数据串联由对象完成
    """
    def  __init__(self):
        self.sequence = ""              # 唯一序列, 必填
        self.tenant_code = ""           # 租户代码
        self.data_handle_result = False  # 数据处理结果
        self.message = ""               # 处理消息
        self.oracle_conn = None         # oracle connection handle
        self.schema_name = ""           # schema name
        self.sql_text = ""              # SQL 文本
        self.sql_ast = None             # AST object -> lusqlparser
        self.table_names = []           # list -> string
        self.plan_text = ""             # plan text -> string
        self.plan_raw = []              # plan raw data -> list
        self.tab_info = []              # table info object -> list : items -> oracle_meta_abstracet.OracleTableMeta
        self.view_histogram = []        # dba_histogram view -> list
        self.addition = {}              # 附加需求数据 -> dict
                                        # {
                                        #     "COLUMNS_DISCRIMINATION": [  字段区分度
                                        #       {
                                        #           'columns': '[user_id]',
                                        #           'sql_text': 'select count(*) from (
                                        #       select distinct user_id from MEMBER_BANK_ACCOUNT where rownum<100000)a',
                                        #           "value": list
                                        #       },
                                        #   ]
                                        # }
        self.statement = ""             # SELECT UPDATE INSERT DELETE
        self.has_dynamic_mosaicking = ""    # 是否使用了动态拼接 YES | NO | UNKNONW

        self.ai_result = ""             # AI 预测结果 (1 - PASS, 0 - NOPASS, -1 - INVALID)
        self.ai_recommend = ""          # AI 建议内容 string
        self.ai_error_code = ""         # AI 错误编码
        self.ai_error_type = ""         # AI 错误类型定义 -> AIError.py

        self.ai_program_type = []       # AI 不通过的问题分类列表
        self.plan_text_opt = ""         # plan text after Ai optimization -> string
        self.sql_text_opt = ""          # 优化后的SQL 文本

    def copy(self) -> 'OracleSQLStruct':
        '''
        clone this OracleSQLStruct
        :return: new OracleSQLStruct
        '''
        import copy
        cp_struct = OracleSQLStruct()
        self_struct_dict = self.__dict__
        for key in self_struct_dict:
            try:
                cp_struct.__dict__[key] = copy.deepcopy(self_struct_dict[key])
            except:
                # -- if copy failed then equal
                cp_struct.__dict__[key] = self_struct_dict[key]
        return cp_struct

"""
mysql data struct:
    sequence                    : unique data (uuid)
    data_handle_result          : 数据处理结果 boolean
    message                     : 数据处理消息, 一般反映错误内容
    mysql_conn                  : MysqlHandle -> mysql 连接, 本类不考虑连接方法及连接权限等问题
    schema_name                 : 必填信息
    sql_text                    : 必填信息
    sql_ast                     : 抽象语法树
    table_names                 : SQL中表名的列表
    plan_text                   : 执行计划展示部分

    plan_raw                    : 执行计划裸数据 plan_raw: -> list
        [tuple -> (id,select_type,table,partitions,type,possible_keys,key,key_len,ref,rows,filtered,Extra)]
        
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra          |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
|  1 | SIMPLE      | NULL  | NULL       | NULL | NULL          | NULL | NULL    | NULL | NULL |     NULL | No tables used |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
        
    注意:mysql5.6以前,执行计划字段为10个； 5.7以后为12个
         
    tab_info                    : 表的元数据 -> list[object -> MysqlTableMeta]

    addition : -> list [dict]
        "Discrimination"区分度 : [
            {
                "schema_name": "xxxx",          # 兼容老版本的协议, 新版本此KEY没有使用
                "name": "xxx,xxx,xxx",          # 字段名称, ","分割表示多字段
                "type": "COLUMN | VIEW",        # 类型标识, 区分name的属性
                "sql_text": "xxxxxx",           # 获取区分度信息的SQL
                "value": list                   # 区分度值
            }
        ]

    statement                   : SQL的句式
    has_dynamic_mosaicking      : 动态拼接标识

    ai_result                   : AI处理结果
    ai_recommend                : AI建议

"""


class MysqlSQLStruct:
    """
    定义 MYSQL SQL 数据结构
    所有的数据对应到对象属性
    所有数据串联由对象完成
    """
    def __init__(self):
        self.sequence = ""              # 唯一序列, 必填
        self.tenant_code = ""           # 租户代码
        self.data_handle_result = False  # 数据处理结果
        self.message = ""               # 处理消息
        self.mysql_conn = None          # mysql connection handle
        self.schema_name = ""           # schema name
        self.sql_text = ""              # SQL 文本
        self.sql_ast = None             # AST object -> lusqlparser
        self.table_names = []           # list -> string
        self.plan_text = ""             # plan text -> string
        self.plan_raw = None            # plan raw data -> pandas data frame
        self.tab_info = []              # table info object -> list : items -> mysql_meta_abstracet.MysqlTableMeta
        self.addition = {}              # 附加需求数据 -> dict
                                        # {
                                        #     "COLUMNS_DISCRIMINATION": [  字段区分度
                                        #       {
                                        #           'columns': '[user_id]',
                                        #           'sql_text': 'select count(*) from (
                                        #       select distinct user_id from MEMBER_BANK_ACCOUNT where rownum<100000)a',
                                        #           "value": list
                                        #       },
                                        #   ]
                                        # }
        self.statement = ""             # SELECT UPDATE INSERT DELETE
        self.has_dynamic_mosaicking = ""    # 是否使用了动态拼接 YES | NO | UNKNONW

        self.ai_result = ""             # AI 预测结果 (1 - PASS, 0 - NOPASS, -1 - INVALID)
        self.ai_recommend = ""          # AI 建议内容 string
        self.ai_error_code = ""         # AI 错误编码
        self.ai_error_type = ""         # AI 错误类型定义 -> AIError.py

        self.ai_program_type = []       # AI 不通过的问题分类列表
        self.plan_text_opt = ""         # plan text after Ai optimization -> string
        self.sql_text_opt = ""          # 优化后的SQL 文本
