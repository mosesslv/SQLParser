# -*- coding:utf-8 -*-

import os
import common.utils as utils
import service.lu_parser_graph.LUSQLParser as LUSQLParser
import service.AISQLReview.service_enter as ServiceEnter
from service.ai_sr_models import AISrSQLDetail
from service.GitSQLReview.lufax_table_meta import TableMeta
from service.ai_sr_models import DBATable, DBAColumn, Instance, SchemaGroup, EraseOTableConf, DevTable, AISrReviewDetail
import prettytable
import common.OracleHandle as OracleHandle
import common.MysqlHandle as MysqlHandle
import traceback
import json
from api_service.pallas_api.models import AiSrTaskSql, AiSrSqlDetail
from datetime import datetime
import service.AISQLReview.AIError as AIError
from service.AISQLReview.AIError import get_error_type_description


class SQLReviewStruct:
    """
    SQL Review 单条SQL的数据结构信息
    """

    def __init__(self):
        self.handle_result = False  # SQL数据处理结果
        self.last_error_info = ""  # SQL数据处理错误信息
        self.sql_text = ""  # SQL文本
        self.sqlmap_filename = ""  # SQL对应的SQLMAP文件全路径(去O场景需要用文件名判断DB TYPE)
        self.is_need_base_plan = False  # 是否需要从测试环境拿计划; LUFAX环境需要拿BASE环境计划
        self.db_type = ""  # ORACLE or MYSQL ......
        self.schema_name = ""  # SCHEMA 名称
        self.table_count = 0  # SQL中包含的表的数量
        self.table_names = []  # SQL中包含的表的列表
        self.statement = ""  # SQL句式：INSERT SELECT UPDATE DELETE
        self.has_hint = False  # SQL是否使用HINT
        self.has_dynamic_mosaicking = 0  # SQL是否使用动态拼接： YES | NO | UNKNOWN
        self.has_order_by = False  # SQL是否使用ORDER BY
        self.is_o2m = False  # SQL是否处于去O场景(一般通过表名在BETTLE中查出的DBTYPE为ORACLE,
        # 但SQLMAP文件是'_mysql'结尾的场景)
        self.db_conn_url = ""  # SQL获取执行计划的物理连接串
        self.plan_text = ""  # 执行计划的文本
        self.plan_src = ""  # 执行计划来源：BASE、PRD
        self.is_white_list = False  # SQLMAP ID是否在白名单
        self.sql_sequence = ""  # 同步AI平台, 标识唯一SQL(UUID)
        self.ai_result = ""  # AI判定结果
        self.ai_recommend = ""  # AI建议
        self.ai_message = ""  # AI消息
        self.sql_text_mysql_replace_variable = ""  # MYSQL更换绑定变量后的SQL
        self.sql_syntax_correct = False  # SQL语法是否正确

        self.review_request_id = 0
        self.namespace = ""
        self.sqlmap_id = ""
        self.sql_modified_type = ""
        self.delete_mark = "1"
        self.noai_error_code = ""
        self.noai_error_type = ""


class SQLReviewHandle:
    """
    sqlmap 解析出的单条SQL处理;
    ORACLE AI HANDLE
    MYSQL PLAN HANDLE
    and so on

        self.is_white_list = False                  # SQLMAP ID是否在白名单
    """

    def __init__(self, sql_review_data, tenant="", userid="", profile_name=""):
        assert isinstance(sql_review_data, SQLReviewStruct)
        self._sql_review_data = sql_review_data
        if utils.str_is_none_or_empty(self._sql_review_data.sql_text):
            raise Exception("sql text is empty")

        self._tenant = tenant
        self._userid = userid
        self._profile_name = profile_name

        self._sql_review_data.is_o2m = False

        try:
            self._lu_sql_parser = LUSQLParser.LuSQLParser(self._sql_review_data.sql_text)
        except Exception as ex:
            msg = "[{0}] sqlparser failed: {1}".format(self._sql_review_data.sql_text, ex)
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = msg
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "SQL语义分析异常"
            self._sql_review_data.noai_error_code = "AIErr_00006"
            try:
                type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                self._sql_review_data.ai_message = desc_str
                self._sql_review_data.noai_error_type = type_str
            except:
                self._sql_review_data.ai_message = "SQL语义分析异常"
                self._sql_review_data.noai_error_type = ""
            raise Exception(msg)

        try:
            _table_names = self._lu_sql_parser.get_table_name() # 未lu_parser未获取到table name，则抛出报错
            if len(_table_names) <= 0: # 如果获取到table name，但是表的数量<=0，则依然抛出报错
                msg = "[{0}] can not find table names".format(self._sql_review_data.sql_text)
                self._sql_review_data.handle_result = False
                self._sql_review_data.last_error_info = msg
                self._sql_review_data.ai_result = "INVALID"
                self._sql_review_data.ai_recommend = ""
                # self._sql_review_data.ai_message = "SQL分析表名异常, 无法获取schema信息"

                self._sql_review_data.noai_error_code = "`AIErr_00007`"
                try:
                    type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                    self._sql_review_data.ai_message = desc_str
                    self._sql_review_data.noai_error_type = type_str
                except:
                    self._sql_review_data.ai_message = "SQL分析表名异常, 无法获取schema信息"
                    self._sql_review_data.noai_error_type = ""

                raise Exception(msg)
        except Exception as ex:
            msg = "[{0}] sqlparse get table names failed: {1}".format(self._sql_review_data.sql_text, ex)
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = msg
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "SQL分析表名异常, 无法获取schema信息"

            self._sql_review_data.noai_error_code = "AIErr_00007"
            try:
                type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                self._sql_review_data.ai_message = desc_str
                self._sql_review_data.noai_error_type = type_str
            except:
                self._sql_review_data.ai_message = "SQL分析表名异常, 无法获取schema信息"
                self._sql_review_data.noai_error_type = ""

            raise Exception(msg)
        self._sql_review_data.table_names = _table_names
        self._sql_review_data.table_count = len(_table_names)

        try:
            _statement = self._lu_sql_parser.sql_statement()
        except Exception as ex:
            msg = "[{0}] sqlparse statement failed: {1}".format(self._sql_review_data.sql_text, ex)
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = msg
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "SQL语义分析异常"

            self._sql_review_data.noai_error_code = "AIErr_00006"
            try:
                type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                self._sql_review_data.ai_message = desc_str
                self._sql_review_data.noai_error_type = type_str
            except:
                self._sql_review_data.ai_message = "SQL语义分析异常SQL语义分析异常"
                self._sql_review_data.noai_error_type = ""

            raise Exception(msg)
        self._sql_review_data.statement = _statement

        try:
            _has_hint = self._lu_sql_parser.has_hint
        except Exception as ex:
            msg = "[{0}] sqlparse hint failed: {1}".format(self._sql_review_data.sql_text, ex)
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = msg
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "SQL语义分析异常"

            self._sql_review_data.noai_error_code = "AIErr_00006"
            try:
                type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                self._sql_review_data.ai_message = desc_str
                self._sql_review_data.noai_error_type = type_str
            except:
                self._sql_review_data.ai_message = "SQL语义分析异常"
                self._sql_review_data.noai_error_type = ""
            raise Exception(msg)
        self._sql_review_data.has_hint = _has_hint

        try:
            _dynamic_mosaicking = self._lu_sql_parser.sql_has_dynamic_mosaicking()
        except Exception as ex:
            msg = "[{0}] sqlparse dynamic mosaicking failed: {1}".format(self._sql_review_data.sql_text, ex)
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = msg
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "SQL语义分析异常"

            self._sql_review_data.noai_error_code = "AIErr_00006"
            try:
                type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                self._sql_review_data.ai_message = desc_str
                self._sql_review_data.noai_error_type = type_str
            except:
                self._sql_review_data.ai_message = "SQL语义分析异常"
                self._sql_review_data.noai_error_type = ""
            raise Exception(msg)
        self._sql_review_data.has_dynamic_mosaicking = _dynamic_mosaicking

        try:
            _has_order = self._lu_sql_parser.has_order
        except Exception as ex:
            msg = "[{0}] sqlparse order handle failed: {1}".format(self._sql_review_data.sql_text, ex)
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = msg
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "SQL语义分析异常"

            self._sql_review_data.noai_error_code = "AIErr_00006"
            try:
                type_str, desc_str = AIError.get_error_type_description(self._sql_review_data.noai_error_code)
                self._sql_review_data.ai_message = desc_str
                self._sql_review_data.noai_error_type = type_str
            except:
                self._sql_review_data.ai_message = "SQL语义分析异常"
                self._sql_review_data.noai_error_type = ""
            raise Exception(msg)
        self._sql_review_data.has_order_by = _has_order

        # get db_type and schema
        data_dict = self._get_engine_schema()
        if not data_dict["result"]:
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = data_dict["message"]
            self._sql_review_data.ai_result = "INVALID"
            self._sql_review_data.ai_recommend = ""
            # self._sql_review_data.ai_message = "元数据信息缺失 无法获取schema信息"
            self._sql_review_data.ai_message = data_dict["message"]
            self._sql_review_data.is_o2m = data_dict["o2m_flag"]
            self._sql_review_data.noai_error_code = "AIErr_00008"
            raise Exception(data_dict["message"])
        else:
            self._sql_review_data.db_type = data_dict["db_type"]
            self._sql_review_data.schema_name = data_dict["schema_name"]
            self._sql_review_data.is_o2m = data_dict["o2m_flag"]

        if self._sql_review_data.db_type.upper() == "MYSQL":
            self._mysql_replace_variable()

        # get base plan
        # BASE plan 只是尝试获取, 理论上所有的SQL都应该有BASE PLAN
        # 结果不做处理
        if self._sql_review_data.is_need_base_plan:
            try:
                base_plan_result = self._get_base_plan_text()
            except:
                pass

        if utils.str_is_none_or_empty(self._sql_review_data.sql_sequence):
            self._sql_review_data.sql_sequence = utils.get_uuid()

        # # get ai
        # if utils.str_is_none_or_empty(self._sql_review_data.sql_sequence):
        #     self._sql_review_data.sql_sequence = utils.get_uuid()
        #
        # ai_process_result = self._get_ai_predict()
        # if not ai_process_result:
        #     self._sql_review_data.handle_result = False
        # else:
        #     self._sql_review_data.handle_result = True

    def _get_engine_schema(self):
        """
        LUFAX环境下获取DB引擎和SCHEMA NAME
        考虑去O场景
        :return: dict : {"result": False, "db_type": "MYSQL", "schema_name": "xxx", "o2m_flag": True, "message": "xxx"}
        """
        db_type = ""
        o2m_flag = False
        if not utils.str_is_none_or_empty(self._sql_review_data.sqlmap_filename):
            filename = os.path.split(self._sql_review_data.sqlmap_filename)[-1].lower()
            if filename.endswith(".xml"):
                # MySqlCivilCommerDocMapper.xml
                file_pre = filename.split(".")[0]
                mysql_flag_str = file_pre.split("_")[-1]
                mysql_flag = True if mysql_flag_str.lower().strip() == "mysql" else False

                if mysql_flag:  # 存在MYSQL标识, 确认DB TYPE为MYSQL
                    db_type = "MYSQL"

        # 取一个表, 进行数据获取; 这里只考虑SQL只有一个SCHEMA
        table_name = self._sql_review_data.table_names[0]
        dev_table_data = DevTable.objects.filter(name=table_name.upper())
        if len(dev_table_data) <= 0:
            msg = "[{0}] 没有在BETTLE元数据中管理".format(table_name)
            return {"result": False, "db_type": "", "schema_name": "", "o2m_flag": o2m_flag, "message": msg}
        dev_table = dev_table_data[0]

        try:
            # schema_group_src = SchemaGroup.objects.get(id=dev_table.schema_group_id)
            schema_group = SchemaGroup.objects.get(id=dev_table.schema_group_id)
        except Exception as ex:
            msg = "table [{0}] 不能找到对应的SCHEMA信息".format(table_name)
            return {"result": False, "db_type": "", "schema_name": "", "o2m_flag": o2m_flag, "message": msg}

        # ------------------- bettle 由表名找SCHEMA，支持租户场景 --------------------------
        # schema_route_data = common.models.SchemaRoute.objects.filter(source_schema=schema_group_src.data_user)
        # if len(schema_route_data) <= 0:
        #     # 没有路由规则，表对应的SCHEMA就是原生SCHEMA
        #     schema_group = schema_group_src
        # else:
        #     # 在路由数据中查找与SRC相同的SCHEMA
        #     schema_group_route_src = schema_route_data.filter(target_schema=schema_group_src.data_user)
        #     if len(schema_group_route_src) > 0:
        #         # 有自己路由到自己的配置
        #         schema_group = schema_group_src
        #     else:
        #         schema_group = None
        #         for schema_route in schema_route_data:
        #             try:
        #                 schema_group_r = SchemaGroup.objects.get(name=schema_route.target_schema)
        #                 if schema_group_r.area.upper().strip() in ["PATRUST-CLOUD", "P2PI-INDO"]:
        #                     # 平安依托云，印尼云的网络问题，不作为选择对象
        #                     continue
        #                 else:
        #                     schema_group = schema_group_r
        #             except Exception as ex:
        #                 continue
        #
        # if schema_group is None:
        #     msg = "table [{0}] 不能找到对应的SCHEMA信息".format(table_name)
        #     return {"result": False, "db_type": "", "schema_name": "", "o2m_flag": o2m_flag, "message": msg}

        # ------------------- bettle 由表名找SCHEMA，支持租户场景 --------------------------

        table_db_type = schema_group.instance_type.upper()
        table_schema = schema_group.data_user

        if db_type.upper() == "MYSQL":
            if table_db_type.upper() == "MYSQL":
                db_type = "MYSQL"
                schema_name = table_schema

            elif table_db_type.upper() == "ORACLE":
                # 去O场景
                o2m_flag = True
                o2m_conf_data = EraseOTableConf.objects.filter(table_name=table_name)
                if len(o2m_conf_data) <= 0:
                    # msg = "[{0}] o2m conf data is empty, not comfirm real dbtype and schema".format(table_name)
                    msg = "[{0}] SQLMAP文件标识为MYSQL，表对应的SCHEMA为ORACLE，去O配置管理中没有找到配置信息".format(table_name)
                    return {"result": False, "db_type": "", "schema_name": "", "o2m_flag": o2m_flag, "message": msg}
                o2m_conf = o2m_conf_data[0]

                db_type = "MYSQL"
                schema_name = o2m_conf.erase_o_schema_name
            else:
                # msg = "[{0}] unknown db type".format(table_db_type)
                msg = "[{0}] SQLMAP文件标识为MYSQL，BETTLE管理表对应SCHEMA的DB引擎未知".format(table_db_type)
                return {"result": False, "db_type": "", "schema_name": "", "o2m_flag": o2m_flag, "message": msg}
        else:
            if table_db_type.upper() in ["ORACLE", "MYSQL"]:
                db_type = table_db_type.upper()
                schema_name = table_schema
            else:
                # msg = "[{0}] unknown db type".format(table_db_type)
                msg = "[{0}] BETTLE管理表对应SCHEMA的DB引擎未知".format(table_db_type)
                return {"result": False, "db_type": "", "schema_name": "", "o2m_flag": o2m_flag, "message": msg}

        return {"result": True, "db_type": db_type, "schema_name": schema_name, "o2m_flag": o2m_flag, "message": ""}

    def _get_oracle_ai_predict(self):
        """
        获取AI判定结论
        :return: bool
        """
        sql_predict_dict = {
            "userid": self._userid,
            "tenant": self._tenant,
            "profile_name": self._profile_name,
            "schema": self._sql_review_data.schema_name,
            "sql_text": self._sql_review_data.sql_text,
            "sequence": self._sql_review_data.sql_sequence
        }
        ser_enter = ServiceEnter.ServiceEnter(sql_predict_dict)
        ai_result_dict = ser_enter.predict()

        self._sql_review_data.ai_result = ai_result_dict['AI_RESULT']
        self._sql_review_data.ai_recommend = ai_result_dict['AI_RECOMMEND']
        self._sql_review_data.ai_message = ai_result_dict['MESSAGE']
        self._sql_review_data.message = ai_result_dict['MESSAGE']

        try:
            ai_sql_detail = AISrSQLDetail.objects.get(sequence=self._sql_review_data.sql_sequence)
        except Exception as ex:
            self._sql_review_data.last_error_info = "get ai detail data exception [{0}]".format(ex)
            return False

        self._sql_review_data.db_conn_url = ai_sql_detail.db_conn_url
        if not utils.str_is_none_or_empty(ai_sql_detail.plan_text):
            self._sql_review_data.plan_text = ai_sql_detail.plan_text
            self._sql_review_data.plan_src = "PRD"
        return True

    def _get_mysql_ai_predict(self):
        """
        获取AI判定结论
        :return: bool
        """
        sql_predict_dict = {
            "userid": self._userid,
            "tenant": self._tenant,
            "profile_name": self._profile_name,
            "schema": self._sql_review_data.schema_name,
            "sql_text": self._sql_review_data.sql_text_mysql_replace_variable,
            "sequence": self._sql_review_data.sql_sequence
        }
        ser_enter = ServiceEnter.ServiceEnter(sql_predict_dict)
        ai_result_dict = ser_enter.predict()

        self._sql_review_data.ai_result = ai_result_dict['AI_RESULT']
        self._sql_review_data.ai_recommend = ai_result_dict['AI_RECOMMEND']
        self._sql_review_data.ai_message = ai_result_dict['MESSAGE']
        self._sql_review_data.message = ai_result_dict['MESSAGE']

        try:
            ai_sql_detail = AISrSQLDetail.objects.get(sequence=self._sql_review_data.sql_sequence)
        except Exception as ex:
            self._sql_review_data.last_error_info = "get ai detail data exception [{0}]".format(ex)
            return False

        self._sql_review_data.db_conn_url = ai_sql_detail.db_conn_url
        if not utils.str_is_none_or_empty(ai_sql_detail.plan_text):
            self._sql_review_data.plan_text = ai_sql_detail.plan_text
            self._sql_review_data.plan_src = "PRD"
        return True

    def _get_ai_predict(self):
        """
        获取生产执行计划, AI结论
        ORACLE调用AI预测逻辑
        MYSQL更换绑定变量, 获取生产执行计划
        :return: bool
        """
        if self._sql_review_data.db_type.upper() == "ORACLE":
            ai_process_result = self._get_oracle_ai_predict()

        elif self._sql_review_data.db_type.upper() == "MYSQL":
            # return self._get_mysql_prd_plan()
            ai_process_result = self._get_mysql_ai_predict()
        else:
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = "unknown db type"
            return False

        if not ai_process_result:
            self._sql_review_data.handle_result = False
        else:
            self._sql_review_data.handle_result = True

    def _get_base_plan_text(self):
        """
        默认到BASE QA环境先拿执行计划
        :return: bool
        """
        base_plan = SQLBasePlan(self._sql_review_data)
        base_plan_result = base_plan.get_base_plan()
        if not base_plan_result:
            self._sql_review_data.handle_result = False
            self._sql_review_data.last_error_info = base_plan.last_error_info
            return False
        return True

    def _mysql_replace_variable(self):
        base_plan = SQLBasePlan(self._sql_review_data)
        result = base_plan.mysql_sql_replace_bind_variable()
        if not result:
            self._sql_review_data.sql_text_mysql_replace_variable = self._sql_review_data.sql_text

    def get_ai_predict(self):
        return self._get_ai_predict()

    def write_database(self):
        # if self._sql_review_data.review_request_id is None:
        #     return False, 'review_request_id is none'
        # if not self._sql_review_data.handle_result:
        #     return False, 'ai result error:{0}'.format(self._sql_review_data.last_error_info)

        review_status = "0"
        if self._sql_review_data.ai_result == "PASS" and self._sql_review_data.db_type == "ORACLE":
            review_status = "1"

        AISrReviewDetail.objects.filter(sequence=self._sql_review_data.sql_sequence).update(
            updated_at=datetime.now(),
            review_request_id=self._sql_review_data.review_request_id,
            namespace=self._sql_review_data.namespace,
            sqlmap_id=self._sql_review_data.sqlmap_id,
            sql_modified_type=self._sql_review_data.sql_modified_type,
            sql_new_text=self._sql_review_data.sql_text,
            plan_new_text=self._sql_review_data.plan_text,
            sqlmap_files=self._sql_review_data.sqlmap_filename,
            hint_status=self._sql_review_data.has_hint,
            white_list=self._sql_review_data.is_white_list,
            dynamic_sql=self._sql_review_data.has_dynamic_mosaicking,
            table_cnt=self._sql_review_data.table_count,
            table_names=self._sql_review_data.table_names,
            order_by=self._sql_review_data.has_order_by,
            db_type=self._sql_review_data.db_type,
            ai_result=self._sql_review_data.ai_result,
            ai_recommend=self._sql_review_data.ai_recommend,
            message=self._sql_review_data.ai_message,
            sql_correct=self._sql_review_data.sql_syntax_correct,
            schema_name=self._sql_review_data.schema_name,
            delete_mark=self._sql_review_data.delete_mark,
            review_status=review_status
        )

        ai_error_code = self._sql_review_data.noai_error_code
        ai_error_type = self._sql_review_data.noai_error_type
        plan_text = self._sql_review_data.plan_text
        ai_program_type = ''
        try:
            sql_detail = AiSrSqlDetail.objects.get(sequence=self._sql_review_data.sql_sequence)
            ai_error_code = sql_detail.ai_error_code
            ai_error_type = sql_detail.ai_error_type
            ai_program_type = sql_detail.ai_program_type
        except Exception as e:
            print(e)

        ai_recommend = ''
        ai_message = ''
        if self._sql_review_data.ai_result == 'NOPASS':
            ai_recommend = self._sql_review_data.ai_recommend
        elif self._sql_review_data.ai_result == 'INVALID':
            ai_recommend = self._sql_review_data.ai_message

        AiSrTaskSql.objects.filter(sql_sequence=self._sql_review_data.sql_sequence) \
            .update(updated_at=datetime.now(), db_type=self._sql_review_data.db_type,
                    schema_name=self._sql_review_data.schema_name,
                    plan_text=plan_text,
                    ai_result=self._sql_review_data.ai_result,
                    ai_recommend=ai_recommend,
                    ai_message=ai_message,
                    ai_error_code=ai_error_code,
                    ai_error_type=ai_error_type,
                    ai_program_type=ai_program_type,
                    review_status=review_status)
        return True, "SUCCESS"


class SQLHandleAndWrite:
    def __init__(self, sql_info, review_request_id, sql_sequence):
        self.sql_info = sql_info
        self.review_request_id = review_request_id
        self.sql_sequence = sql_sequence

    def handle_ai_exception(self, data_struct, ex_info):
        if not data_struct.handle_result:
            ai_result = ''
            ai_recommend = ''
            ai_message = ''
            if data_struct.ai_result == 'NOPASS':
                ai_result = data_struct.ai_result
                ai_recommend = data_struct.ai_recommend
            elif data_struct.ai_result == 'INVALID':
                ai_result = data_struct.ai_result
                ai_recommend = data_struct.ai_message
            elif data_struct.ai_result in (None, "") or len(data_struct.ai_result) <= 0:
                ai_result = 'INVALID'
                try:
                    ai_ex = json.loads(str(ex_info))
                    ex_code = ai_ex['AI_ERROR_CODE']
                    type, desc = get_error_type_description(ex_code)
                    ai_recommend = desc
                except Exception as e:
                    print(e)
                    ai_recommend = "AI处理异常"
                    ai_message = ex_info

            AISrReviewDetail.objects.filter(sequence=data_struct.sql_sequence) \
                .update(updated_at=datetime.now(), review_request_id=data_struct.review_request_id,
                        namespace=data_struct.namespace,
                        sqlmap_id=data_struct.sqlmap_id,
                        sql_modified_type=data_struct.sql_modified_type,
                        sql_new_text=data_struct.sql_text,
                        plan_new_text=data_struct.plan_text,
                        sqlmap_files=data_struct.sqlmap_filename,
                        hint_status=data_struct.has_hint,
                        white_list=data_struct.is_white_list,
                        dynamic_sql=data_struct.has_dynamic_mosaicking,
                        table_cnt=data_struct.table_count,
                        table_names=data_struct.table_names,
                        order_by=data_struct.has_order_by,
                        db_type=data_struct.db_type,
                        ai_result=ai_result,
                        ai_recommend=ai_recommend,
                        message=data_struct.ai_message,
                        sql_correct=data_struct.sql_syntax_correct,
                        schema_name=data_struct.schema_name)

            ai_error_code = data_struct.noai_error_code
            ai_error_type = data_struct.noai_error_type
            plan_text = data_struct.plan_text
            ai_program_type = ''
            try:
                sql_detail = AiSrSqlDetail.objects.get(sequence=data_struct.sql_sequence)
                ai_error_code = sql_detail.ai_error_code
                ai_error_type = sql_detail.ai_error_type
                ai_program_type = sql_detail.ai_program_type
            except Exception as e:
                print(e)

            review_status = "0"
            AiSrTaskSql.objects.filter(sql_sequence=data_struct.sql_sequence) \
                .update(updated_at=datetime.now(), db_type=data_struct.db_type,
                        schema_name=data_struct.schema_name,
                        plan_text=plan_text,
                        ai_result=ai_result,
                        ai_recommend=ai_recommend,
                        ai_message=ai_message,
                        ai_error_code=ai_error_code,
                        ai_error_type=ai_error_type,
                        ai_program_type=ai_program_type,
                        review_status=review_status)

    def sql_handle_and_write(self):
        sql_struct = SQLReviewStruct()
        sql_struct.sql_text = self.sql_info.sql_text
        sql_struct.sqlmap_filename = list(self.sql_info.sqlmap_files)[0]
        sql_struct.namespace = self.sql_info.namespace
        sql_struct.sqlmap_id = self.sql_info.sqlmap_piece_id
        sql_struct.review_request_id = self.review_request_id
        sql_struct.sql_sequence = self.sql_sequence
        sql_struct.is_need_base_plan = True
        try:
            sql_handle = SQLReviewHandle(sql_struct, tenant="LUFAX")
            sql_handle.get_ai_predict()
            result = sql_handle.write_database()
            print(result)
            if sql_struct.ai_result == "PASS" and sql_struct.db_type == "ORACLE":
                return False
            else:
                return True
        except Exception as e:
            print(e)
            self.handle_ai_exception(sql_struct, e)
            if sql_struct.ai_result == "PASS" and sql_struct.db_type == "ORACLE":
                return False
            else:
                return True


class SingleSQLHandleAndWrite:
    def __init__(self, tenant, userid, profile_name, sql_text, sql_sequence, schema_name, db_type):
        self.tenant = tenant
        self.userid = userid
        self.profile_name = profile_name
        self.sql_text = sql_text
        self.sql_sequence = sql_sequence
        self.schema_name = schema_name
        self.db_type = db_type

    def handle_ai_exception(self, data_struct):

        ai_error_code = data_struct.noai_error_code
        ai_error_type = data_struct.noai_error_type
        plan_text = data_struct.plan_text
        ai_program_type = ''
        try:
            sql_detail = AiSrSqlDetail.objects.get(sequence=data_struct.sql_sequence)
            ai_error_code = sql_detail.ai_error_code
            ai_error_type = sql_detail.ai_error_type
            ai_program_type = sql_detail.ai_program_type
        except Exception as e:
            print(e)

        ai_recommend = ''
        ai_message = ''
        if data_struct.ai_result == 'NOPASS':
            ai_recommend = data_struct.ai_recommend
        elif data_struct.ai_result == 'INVALID':
            ai_recommend = data_struct.ai_message

        AiSrTaskSql.objects.filter(sql_sequence=data_struct.sql_sequence) \
            .update(updated_at=datetime.now(), db_type=data_struct.db_type,
                    schema_name=data_struct.schema_name,
                    plan_text=plan_text,
                    ai_result=data_struct.ai_result,
                    ai_recommend=ai_recommend,
                    ai_message=ai_message,
                    ai_error_code=ai_error_code,
                    ai_error_type=ai_error_type,
                    ai_program_type=ai_program_type,
                    review_status='0')

    def sql_handle_and_write(self):
        service_enter_dict = {
            "userid": self.userid,
            "tenant": self.tenant,
            "profile_name": self.profile_name,
            "schema": self.schema_name,
            "sql_text": self.sql_text,
            "sequence": self.sql_sequence
        }
        service_enter = ServiceEnter.ServiceEnter(service_enter_dict)
        result_dict = service_enter.predict()

        try:
            dict_ai_result = result_dict["AI_RESULT"]
            ai_recommend = result_dict["AI_RECOMMEND"]
            ai_message = result_dict["MESSAGE"]

            sql_detail = AiSrSqlDetail.objects.get(sequence=self.sql_sequence)
            ai_error_code = sql_detail.ai_error_code
            ai_error_type = sql_detail.ai_error_type
            ai_program_type = sql_detail.ai_program_type

            if dict_ai_result == 'NOPASS':
                ai_recommend = ai_recommend
            elif dict_ai_result == 'INVALID':
                ai_recommend = ai_message

            review_status = '0'
            if dict_ai_result == "PASS" and service_enter._db_type == "ORACLE":
                review_status = "1"

            AiSrTaskSql.objects.filter(sql_sequence=self.sql_sequence) \
                .update(updated_at=datetime.now(), db_type=sql_detail.db_type,
                        schema_name=sql_detail.schema_name,
                        plan_text=sql_detail.plan_text,
                        ai_result=dict_ai_result,
                        ai_recommend=ai_recommend,
                        ai_message=ai_message,
                        ai_error_code=ai_error_code,
                        ai_error_type=ai_error_type,
                        ai_program_type=ai_program_type,
                        review_status=review_status)

        except Exception as e:
            print(e)

        #
        # sql_struct = SQLReviewStruct()
        # sql_struct.sql_text = self.sql_text
        # sql_struct.sql_sequence = self.sql_sequence
        # sql_struct.schema_name = self.schema_name
        # sql_struct.db_type = self.db_type
        # try:
        #     sql_handle = SQLReviewHandle(sql_struct, tenant=self.tenant, userid=self.userid, profile_name=self.profile_name)
        #     sql_handle.get_ai_predict()
        #     result = sql_handle.write_database()
        #     print(result)
        #     return sql_struct.ai_result
        # except Exception as e:
        #     print(e)
        #     self.handle_ai_exception(sql_struct)
        #     return sql_struct.ai_result


class SQLBasePlan:
    """
    SQL从BASE QA获取PLAN
    调用此类前提: SQLReviewStruct -> [db_type, schema_name, table_names] 信息已经存在
    """

    def __init__(self, sql_review_data):
        assert isinstance(sql_review_data, SQLReviewStruct)
        self._sql_review_data = sql_review_data
        self.last_error_info = ""

    def _get_table_meta(self, schema_name, table_name):
        """
        从BETTLE获取元数据信息
        :return: TableMeta
        """
        try:
            dba_table = DBATable.objects.get(name=table_name)
        except Exception as ex:
            msg = "[{0}] can not find meta info".format(table_name)
            self.last_error_info = msg
            return None

        try:
            schema_group = SchemaGroup.objects.get(id=dba_table.schema_group_id)
        except Exception as ex:
            msg = "[{0}] can not find schema info".format(table_name)
            self.last_error_info = msg
            return None

        if schema_group.data_user.upper().strip() != schema_name.upper().strip():
            msg = "schema parameter error"
            self.last_error_info = msg
            return None

        table_meta = TableMeta()
        table_meta.db_type = self._sql_review_data.db_type
        table_meta.schema_name = schema_name
        table_meta.table_name = table_name
        table_meta.table_comments = dba_table.comments

        try:
            dba_columns_data = DBAColumn.objects.filter(table_id=dba_table.source_table_id)
        except Exception as ex:
            msg = "[{0}] column meta invalid".format(table_name)
            self.last_error_info = msg
            return None

        columns = []
        for dba_column in dba_columns_data:
            col_dict = {
                'col_name': dba_column.name,
                'col_type': dba_column.col_type,
                'col_null': dba_column.col_null,
                'default_value': dba_column.default_value,
                'col_comments': dba_column.comments
            }
            columns.append(col_dict)

        table_meta.table_columns = columns
        return table_meta

    def _get_table_prd_single_data(self, schema_name, table_name):
        """
        获取指定表的单条生产数据
        :param schema_name:
        :param table_name:
        :return: tuple
        """
        return None

    def mysql_sql_replace_bind_variable(self):
        return self._mysql_sql_replace_bind_variable()

    def _mysql_sql_replace_bind_variable(self):
        """
        MYSQL绑定变量不能正常生成PLAN, 需要替换绑定变量
        替换完的SQL记录在 SQLReviewStruct.sql_text_mysql_replace_variable
        :return: bool
        """
        # 先用老的LUSQLPARSE方法进行变量替换, 待博怀的新类
        # from baseservice.lufax_sqlparser.LuSQLParser import LuSQLParser
        # lu_sql_parser = LuSQLParser(self._sql_review_data.sql_text)

        # bind_var_tokens = lu_sql_parser._get_bind_variable_tokens()
        # if len(bind_var_tokens) <= 0:
        #     # 没有绑定变量
        #     self._sql_review_data.sql_text_mysql_replace_variable = self._sql_review_data.sql_text
        #     return True

        tab_metas = []
        for table_name in self._sql_review_data.table_names:
            table_meta = self._get_table_meta(self._sql_review_data.schema_name, table_name)
            if table_meta is None:
                return False
            tab_metas.append(table_meta)
        # end for

        exception_list = LUSQLParser.get_expection_list(tab_metas)
        lu_sql_parser = LUSQLParser.LuSQLParser(self._sql_review_data.sql_text, exception=exception_list)

        data_map = {}
        for table_name in self._sql_review_data.table_names:
            table_data = self._get_table_prd_single_data(self._sql_review_data.schema_name, table_name)
            if table_data is None:
                table_data = ()  # 空tuple

            data_map[table_name.upper()] = table_data
        # end for

        new_sql = lu_sql_parser.sql_replace_bind_variable(tab_metas, data_map)
        if not utils.str_is_none_or_empty(new_sql):
            self._sql_review_data.sql_text_mysql_replace_variable = new_sql
        else:
            self._sql_review_data.sql_text_mysql_replace_variable = self._sql_review_data.sql_text

        return True

    def _get_oracle_common_explain(self, oracle_handle, schema_name, sql_text):
        """
        通用获取ORACLE指定SQL PLAN数据
        :param schema_name:
        :param sql_text:
        :return: dict {"result": True|False, "db_type": xxx, "explaindata": [], "explaindesc":xxx }
        """
        ora_conn = oracle_handle
        if ora_conn is None:
            msg = "get oracle connection invalid [{0}]".format(schema_name)
            self.last_error_info = msg
            return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        instance_name = ora_conn.get_instance_name()
        explain_sql = "explain plan for {0}".format(sql_text)
        try:
            ora_data = ora_conn.ora_execute_dml_sql(explain_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, explain_sql, ora_data.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        except Exception as ex:
            msg = "[{0}\n{1}] exec explain exception [{2}\n{3}]". \
                format(schema_name, explain_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        # select * from table(dbms_xplan.display_plan) 在plan_table只取最新的数据;
        # 获取最新的EXPLAIN数据
        plan_id_sql = "select max(plan_id) from plan_table"
        try:
            ora_data = ora_conn.ora_execute_query_get_all_data(plan_id_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, plan_id_sql, ora_data.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

            plan_id = ora_data.data[0][0]

        except Exception as ex:
            msg = "[{0}\n{1}] get plan id exception [{2}\n{3}]". \
                format(schema_name, plan_id_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        # get plan_table data
        plan_table_sql = """select STATEMENT_ID, PLAN_ID, to_char(TIMESTAMP,'yyyy-mm-dd hh24:mi:ss'),
                         REMARKS, OPERATION, OPTIONS, OBJECT_NODE,
                         OBJECT_OWNER, OBJECT_NAME, OBJECT_ALIAS, OBJECT_INSTANCE, OBJECT_TYPE, OPTIMIZER,
                         SEARCH_COLUMNS, ID, PARENT_ID, DEPTH, POSITION, COST, CARDINALITY, BYTES, OTHER_TAG,
                         PARTITION_START, PARTITION_STOP, PARTITION_ID, OTHER, DISTRIBUTION, CPU_COST, IO_COST,
                         TEMP_SPACE, ACCESS_PREDICATES, FILTER_PREDICATES, PROJECTION, TIME, QBLOCK_NAME
                         from plan_table where plan_id={0} order by id""".format(plan_id)
        try:
            ora_data = ora_conn.ora_execute_query_get_all_data(plan_table_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, plan_table_sql, ora_data.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

            plan_table_data = ora_data.data

        except Exception as ex:
            msg = "[{0}\n{1}] get plan_table exception [{2}\n{3}]". \
                format(schema_name, plan_table_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        # select * from table(dbms_xplan.display)
        explain_desc_sql = "select plan_table_output from table(dbms_xplan.display)"
        try:
            ora_data = ora_conn.ora_execute_query_get_all_data(explain_desc_sql)
            if not ora_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, explain_desc_sql, ora_data.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

            explain_desc = ""
            for row in ora_data.data:
                explain_desc += row[0]
                explain_desc += "\n"

        except Exception as ex:
            msg = "[{0}\n{1}] get plan_table exception [{2}\n{3}]". \
                format(schema_name, explain_desc_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "ORACLE", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        return {"result": True, "db_type": "ORACLE", "instance_name": instance_name, "explaindata": plan_table_data,
                "explaindesc": explain_desc, "errorinfo": ""}

    def _get_mysql_common_explain(self, mysql_handle, schema_name, sql_text):
        """
        通用获取 MYSQL 指定SQL PLAN数据
        :param schema_name:
        :param sql_text:
        :return: dict {"result": True|False, "db_type": xxx, "explaindata": [], "explaindesc":xxx }
        """
        my_conn = mysql_handle
        instance_name = ""
        if my_conn is None:
            msg = "get mysql connection invalid [{0}]".format(schema_name)
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        explain_sql = "explain {0}".format(sql_text)
        try:
            my_data = my_conn.mysql_execute_query_get_all_data(explain_sql)
            if not my_data.result:
                msg = "[{0} SQL: {1}] run sql error [{2}]".format(schema_name, explain_sql, my_data.message)
                self.last_error_info = msg
                return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}
            else:
                plan_table_data = my_data.data

                # mysql5.6 和 mysql5.7 的执行计划格式不同

                # 5.7
                # +----+-------------+--------------------+------------+-------+---------------+---------+---------+-------+------+----------+-------------+
                # | id | select_type | table              | partitions | type  | possible_keys | key     | key_len | ref   | rows | filtered | Extra       |
                # +----+-------------+--------------------+------------+-------+---------------+---------+---------+-------+------+----------+-------------+
                # |  1 | UPDATE      | m_lcb_event_engine | NULL       | range | PRIMARY       | PRIMARY | 8       | const |    1 |   100.00 | Using where |
                # +----+-------------+--------------------+------------+-------+---------------+---------+---------+-------+------+----------+-------------+
                # 5.6
                # +----+-------------+--------------------------+------+---------------+------+---------+------+------+-------+
                # | id | select_type | table                    | type | possible_keys | key  | key_len | ref  | rows | Extra |
                # +----+-------------+--------------------------+------+---------------+------+---------+------+------+-------+
                # |  1 | SIMPLE      | bak_app_k8s_param_190819 | ALL  | NULL          | NULL | NULL    | NULL |    1 | NULL  |
                # +----+-------------+--------------------------+------+---------------+------+---------+------+------+-------+

                # 根据数据长度来区分
                data_len = len(my_data.data[0])

                if data_len == 12:
                    headers = ["id", "select_type", "table", "partitions", "type", "possible_keys",
                               "key", "key_len", "ref", "rows", "filtered", "Extra"]
                else:
                    #  data_len == 10
                    headers = ["id", "select_type", "table", "type", "possible_keys",
                               "key", "key_len", "ref", "rows", "Extra"]

                t = prettytable.PrettyTable(headers)
                for header in headers:
                    t.align[header] = "l"

                for row in my_data.data:
                    t.add_row(row)

                explaindesc = t.get_string()

        except Exception as ex:
            msg = "[{0}\n{1}] exec explain exception [{2}\n{3}]". \
                format(schema_name, explain_sql, ex, traceback.format_exc())
            self.last_error_info = msg
            return {"result": False, "db_type": "MYSQL", "explaindata": [], "explaindesc": "", "errorinfo": msg}

        return {"result": True, "db_type": "MYSQL", "instance_name": instance_name, "explaindata": plan_table_data,
                "explaindesc": explaindesc, "errorinfo": ""}

    def get_base_plan(self):
        """
        从 BASE 获取计划, 理论上BASE都可以正常拿到计划; 如果报错, 认为语法有问题
        :return:
        """
        self._sql_review_data.sql_syntax_correct = False

        if self._sql_review_data.db_type.upper() == "ORACLE":
            ora_base_instance_data = Instance.objects.filter(name='BASE-DB')
            if len(ora_base_instance_data) <= 0:
                self.last_error_info = "can not find oracle base instance"
                return False
            ora_base_instance = ora_base_instance_data[0]
            username = ora_base_instance.dba_username
            password = ora_base_instance.dba_password
            # 172.19.47.100:1521/ies
            host = ora_base_instance.url.split(":")[0]
            port = ora_base_instance.url.split(":")[1]
            instance_name = ora_base_instance.url.split("/")[-1]
            try:
                oracle_handle = OracleHandle.OracleHandle(username, password, instance_name, host, port=port)
            except Exception as ex:
                msg = "oracle base connect invalid [{0}]".format(ex)
                self.last_error_info = msg
                return False

            oracle_explain_dict = self._get_oracle_common_explain(
                oracle_handle, self._sql_review_data.schema_name, self._sql_review_data.sql_text)
            if not oracle_explain_dict["result"]:
                self.last_error_info = oracle_explain_dict["errorinfo"]
                return False

            self._sql_review_data.plan_text = oracle_explain_dict["explaindesc"]
            self._sql_review_data.plan_src = "BASE"
            self._sql_review_data.sql_syntax_correct = True
            return True

        elif self._sql_review_data.db_type.upper() == "MYSQL":
            # 先进行变量替换
            replace_sql_result = self._mysql_sql_replace_bind_variable()
            if not replace_sql_result:
                self.last_error_info = "mysql bind variable replace failed"
                return False

            my_base_instance_data = Instance.objects.filter(name='BASE-DB.MY')
            if len(my_base_instance_data) <= 0:
                self.last_error_info = "can not find mysql base instance"
                return False
            my_base_instance = my_base_instance_data[0]
            username = my_base_instance.dba_username
            password = my_base_instance.dba_password
            # 172.23.30.48:3306
            host = my_base_instance.url.split(":")[0]
            port = my_base_instance.url.split(":")[1]
            try:
                mysql_handle = MysqlHandle.MysqlHandle(username, password,
                                                       host, port=port, database=self._sql_review_data.schema_name)
            except Exception as ex:
                msg = "mysql base connect invalid [{0}]".format(ex)
                self.last_error_info = msg
                return False

            mysql_explain_dict = self._get_mysql_common_explain(
                mysql_handle, self._sql_review_data.schema_name, self._sql_review_data.sql_text_mysql_replace_variable)
            if not mysql_explain_dict["result"]:
                self.last_error_info = mysql_explain_dict["errorinfo"]
                return False

            self._sql_review_data.plan_text = mysql_explain_dict["explaindesc"]
            self._sql_review_data.plan_src = "BASE"
            self._sql_review_data.sql_syntax_correct = True
            return True
        else:
            self.last_error_info = "unknown db type"
            return False


if __name__ == "__main__":
    sql_review_struct = SQLReviewStruct()
    sql_base = SQLBasePlan(sql_review_struct)
    resutl = sql_base.get_base_plan()
    print("hello")
