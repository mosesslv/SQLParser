# -*- coding: UTF-8 -*-
# ORACLE AI PROCESS
from service.AISQLReview.sql_abstract import OracleSQLStruct
from service.oracle_opt.oracle_common import OracleCommon
from service.oracle_meta.oracle_meta_abstract import OracleTableMeta
from service.oracle_meta.oracle_meta_handle import OracleMetaHandle
from service.lu_parser_graph.LUSQLParser import LuSQLParser
from service.ai_sr_models import AISrSQLDetail
from service.predict_sql_review_oracle.P_DISCRIMINATOR import AI
from service.predict_sql_review_oracle.P_RECOMMENDATION import RECOMM
from service.predict_sql_review_oracle.InfoHandler import InfoHandler
import service.predict_sql_review_oracle.AIException as AIException
import service.AISQLReview.AIError as AIError
import json
import common.utils as utils
import datetime
import logging
logger = logging.getLogger("")


class OracleAISQLReview:
    """
    AI 处理入口
    """
    def __init__(self, oracle_sql_struct):
        self.last_error_info = ""

        assert isinstance(oracle_sql_struct, OracleSQLStruct)
        self._oracle_sql_struct = oracle_sql_struct
        self._oracle_sql_struct.data_handle_result = False  # 初始化数据结果为False

        # check struct
        if utils.str_is_none_or_empty(self._oracle_sql_struct.schema_name):
            self._oracle_sql_struct.ai_error_code = "AIErr_00003"
            type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
            self._oracle_sql_struct.ai_error_type = type_str
            self._oracle_sql_struct.message = desc_str
            raise Exception("schema name invalid")

        if utils.str_is_none_or_empty(self._oracle_sql_struct.sql_text.strip()):
            self._oracle_sql_struct.ai_error_code = "AIErr_00004"
            type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
            self._oracle_sql_struct.ai_error_type = type_str
            self._oracle_sql_struct.message = desc_str
            raise Exception("sql text invalid")

        if utils.str_is_none_or_empty(self._oracle_sql_struct.tenant_code.strip()):
            self._oracle_sql_struct.ai_error_code = "AIErr_00005"
            type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
            self._oracle_sql_struct.ai_error_type = type_str
            self._oracle_sql_struct.message = desc_str
            raise Exception("tenant code invalid")

        # 构建SQL AST
        try:
            sql_parser = LuSQLParser(self._oracle_sql_struct.sql_text)
        except Exception as ex:
            msg = "struct sqlparser graph exception [{0}]".format(ex)
            self.last_error_info = msg
            ai_err_code = "AIErr_Oracle_00005"
            message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": msg}
            raise Exception(json.dumps(message_dict))
        self._oracle_sql_struct.sql_ast = sql_parser
        self._oracle_sql_struct.statement = self._oracle_sql_struct.sql_ast.sql_statement()
        self._oracle_sql_struct.has_dynamic_mosaicking = self._oracle_sql_struct.sql_ast.sql_has_dynamic_mosaicking()

        # 分析出表名的LIST
        try:
            table_names = self._oracle_sql_struct.sql_ast.get_table_name()
            table_names = utils.list_str_item_clear_empty_line_and_duplication_data(table_names)
            for table_name in table_names:
                if table_name.upper() in ["DUAL"]:
                    continue
                else:
                    self._oracle_sql_struct.table_names.append(table_name)
        except Exception as ex:
            msg = "[{0}] parser table name exception [{1}]".format(self._oracle_sql_struct.sql_text, ex)
            self.last_error_info = msg
            ai_err_code = "AIErr_Oracle_00006"
            message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": msg}
            raise Exception(json.dumps(message_dict))

        # 补充sequence
        if utils.str_is_none_or_empty(self._oracle_sql_struct.sequence):
            self._oracle_sql_struct.sequence = utils.get_uuid()

        # 确认ORACLE HANDLE
        if self._oracle_sql_struct.oracle_conn is None:
            ai_err_code = "AIErr_Oracle_00004"
            message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
            raise Exception(json.dumps(message_dict))
        else:
            self._oracle_common = OracleCommon(self._oracle_sql_struct.oracle_conn)

    def handle_struct_data(self, need_explain_handle=True, need_histogram=True) -> bool:
        """
        处理获取数据结构相关数据
        :return: bool
        """
        if need_explain_handle:
            plan_dict = self._oracle_common.get_explain(self._oracle_sql_struct.schema_name,
                                                        self._oracle_sql_struct.sql_text)
            # handle plan
            try:
                plan_result = plan_dict["result"]
                plan_raw = plan_dict["explaindata"]
                plan_text = plan_dict["explaindesc"]
                plan_error = plan_dict["errorinfo"]
            except Exception as ex:
                msg = "[{0}] plan data exception [{1}]".format(plan_dict, ex)
                self.last_error_info = msg
                logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
                self._oracle_sql_struct.data_handle_result = False

                self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10000"
                type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
                self._oracle_sql_struct.ai_error_type = type_str
                self._oracle_sql_struct.message = desc_str

                return False

            if not plan_result:
                # msg = "[{0}] sql sync error, get plan data error [{1}]".format(plan_dict, plan_error)
                msg = plan_error
                self.last_error_info = msg
                logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
                self._oracle_sql_struct.data_handle_result = False

                if plan_error.find("ORA-00905") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10001"
                elif plan_error.find("ORA-00904") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10002"
                elif plan_error.find("ORA-00902") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10003"
                elif plan_error.find("ORA-01747") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10004"
                elif plan_error.find("ORA-00909") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10005"
                elif plan_error.find("ORA-00942") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10006"
                elif plan_error.find("ORA-00936") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10007"
                elif plan_error.find("ORA-01756") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10008"
                elif plan_error.find("ORA-00933") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10009"
                elif plan_error.find("ORA-00907") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10010"
                elif plan_error.find("ORA-00972") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10011"
                elif plan_error.find("ORA-00903") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10012"
                elif plan_error.find("ORA-22806") >= 0:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10013"

                else:
                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_10000"

                type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
                self._oracle_sql_struct.ai_error_type = type_str
                self._oracle_sql_struct.message = msg

                return False

            self._oracle_sql_struct.plan_raw = plan_raw
            self._oracle_sql_struct.plan_text = plan_text

        # handle table info
        for table_name in self._oracle_sql_struct.table_names:
            tab_meta_obj = self._get_table_info(self._oracle_sql_struct.schema_name, table_name)
            if tab_meta_obj is None:

                self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_00007"
                type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
                self._oracle_sql_struct.ai_error_type = type_str
                self._oracle_sql_struct.message = self.last_error_info

                return False
            self._oracle_sql_struct.tab_info.append(tab_meta_obj)
        # end for

        if need_histogram:
            # handle histogram
            for table_name in self._oracle_sql_struct.table_names:
                histogram_data = self._oracle_common.get_view_dba_histogram(self._oracle_sql_struct.schema_name, table_name)
                if histogram_data is None:
                    self.last_error_info = self._oracle_common.last_error_info

                    self._oracle_sql_struct.ai_error_code = "AIErr_Oracle_00008"
                    type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)
                    self._oracle_sql_struct.ai_error_type = type_str
                    self._oracle_sql_struct.message = self.last_error_info

                    return False
                self._oracle_sql_struct.view_histogram.append(histogram_data)
            # end for

        return True

    def _get_table_info(self, schema_name, table_name) -> object:
        """
        获取单表的元数据对象 OracleTableMeta
        :param table_name:
        :return: OracleTableMeta
        """
        try:
            oracle_meta_handle = OracleMetaHandle(self._oracle_sql_struct.oracle_conn)
        except Exception as ex:
            msg = "[{0}.{1}] struct OracleMetaHandle exception [{2}]".format(schema_name, table_name, ex)
            self.last_error_info = msg
            return None

        oracle_tab_meta = OracleTableMeta()
        oracle_tab_meta.info_result = False
        oracle_tab_meta.last_error_info = ""
        oracle_tab_meta.schema_name = schema_name
        oracle_tab_meta.table_name = table_name

        result = oracle_meta_handle.get_oracle_tab_meta(oracle_tab_meta)
        if not result or not oracle_tab_meta.info_result:
            msg = "[{0}.{1}] handle table meta failed [{2}]".\
                format(schema_name, table_name, oracle_tab_meta.last_error_info)
            self.last_error_info = msg
            return None

        return oracle_tab_meta

    def _get_addition_data(self) -> bool:
        """
        获取附加数据
        :return: bool
        """
        def _get_value_from_sql(input_sql_text):
            result_data = self._oracle_sql_struct.oracle_conn.ora_execute_query_get_all_data(input_sql_text)
            if not result_data.result:
                self.last_error_info = "[{0}] SQL exec failed [{1}]".format(sql_text, result_data.message)
                return None
            else:
                return result_data.data

        if self._oracle_sql_struct.addition is None:
            return False

        try:
            cols_distinct_data = self._oracle_sql_struct.addition["COLUMNS_DISCRIMINATION"]
        except Exception as ex:
            msg = "[{0}] addition key COLUMNS_DISCRIMINATION exception [{1}]".\
                format(self._oracle_sql_struct.sequence, ex)
            self.last_error_info = msg
            return False

        if len(cols_distinct_data) <= 0:
            return True

        for dict_data in cols_distinct_data:
            sql_text = dict_data["sql_text"]
            data = _get_value_from_sql(sql_text)
            if data is None:
                return False
            else:
                dict_data["value"] = data
        # end for
        return True

    def sql_predict(self) -> bool:
        """
        AI SQLReview实现
        :return: bool 结果代表处理过程的正确与否, 不是AI判定结果; 结果见 oracle_sql_struct 对象
        """
        result = self.handle_struct_data()

        # record sql
        try:
            ai_sr_sql_detail = AISrSQLDetail.objects.get(sequence=self._oracle_sql_struct.sequence)
        except AISrSQLDetail.DoesNotExist:
            ai_sr_sql_detail = AISrSQLDetail()
            ai_sr_sql_detail.created_at = datetime.datetime.now()
            ai_sr_sql_detail.created_by = "SYS"
            ai_sr_sql_detail.updated_at = datetime.datetime.now()
            ai_sr_sql_detail.updated_by = "SYS"
            ai_sr_sql_detail.sequence = self._oracle_sql_struct.sequence
            ai_sr_sql_detail.tenant_code = self._oracle_sql_struct.tenant_code
            ai_sr_sql_detail.db_type = "ORACLE"
            ai_sr_sql_detail.schema_name = self._oracle_sql_struct.schema_name
            ai_sr_sql_detail.sql_text = self._oracle_sql_struct.sql_text

        except Exception as ex:
            msg = "[{0} -> {1}] AISrSQLDetail orm exception [{2}]".\
                format(self._oracle_sql_struct.schema_name, self._oracle_sql_struct.sql_text, ex)
            self.last_error_info = msg
            return False

        ai_sr_sql_detail.tenant_code = self._oracle_sql_struct.tenant_code
        ai_sr_sql_detail.db_type = "ORACLE"
        ai_sr_sql_detail.schema_name = self._oracle_sql_struct.schema_name
        ai_sr_sql_detail.db_conn_url = "{0}:{1}/{2}".format(self._oracle_sql_struct.oracle_conn.get_address(),
                                                            self._oracle_sql_struct.oracle_conn.get_port(),
                                                            self._oracle_sql_struct.oracle_conn.get_instance_name())
        ai_sr_sql_detail.statement = self._oracle_sql_struct.statement
        ai_sr_sql_detail.dynamic_mosaicking = self._oracle_sql_struct.has_dynamic_mosaicking
        ai_sr_sql_detail.table_names = "" if self._oracle_sql_struct.table_names is None \
            else json.dumps(self._oracle_sql_struct.table_names)
        ai_sr_sql_detail.plan_text = self._oracle_sql_struct.plan_text
        ai_sr_sql_detail.plan_raw = "" if self._oracle_sql_struct.plan_raw is None \
            else json.dumps(self._oracle_sql_struct.plan_raw)

        tab_meta_list = []
        if self._oracle_sql_struct.tab_info is not None:
            for tab_meta_obj in self._oracle_sql_struct.tab_info:
                tab_meta_dict = tab_meta_obj.__dict__
                tab_meta_list.append(tab_meta_dict)

        sql_data = {
            "TABLE_INFO": tab_meta_list,
            "HISTOGRAM": [] if self._oracle_sql_struct.view_histogram is None
            else self._oracle_sql_struct.view_histogram,
            "ADDITION": {} if self._oracle_sql_struct.addition is None else self._oracle_sql_struct.addition
        }
        ai_sr_sql_detail.sql_data = json.dumps(sql_data)
        ai_sr_sql_detail.message = self._oracle_sql_struct.message
        ai_sr_sql_detail.save()

        if not result:
            self._oracle_sql_struct.ai_result = "INVALID"
            self._oracle_sql_struct.ai_recommend = ""
            type_str, desc_str = AIError.get_error_type_description(self._oracle_sql_struct.ai_error_code)

            # self._oracle_sql_struct.message = "SQL语法错误"
            # ai_result_dict = {
            #     "AI_RESULT": self._oracle_sql_struct.ai_result,
            #     "AI_RECOMMEND": self._oracle_sql_struct.ai_recommend,
            #     "MESSAGE": self._oracle_sql_struct.message
            # }

            ai_result_dict = {
                "AI_RESULT": self._oracle_sql_struct.ai_result,
                "AI_RECOMMEND": self._oracle_sql_struct.ai_recommend,
                "MESSAGE": desc_str
            }
            ai_sr_sql_detail.ai_result = json.dumps(ai_result_dict)
            ai_sr_sql_detail.message = self._oracle_sql_struct.message
            ai_sr_sql_detail.ai_error_code = self._oracle_sql_struct.ai_error_code
            ai_sr_sql_detail.ai_error_type = self._oracle_sql_struct.ai_error_type
            ai_sr_sql_detail.save()
            return False

        # call recommend arithmetic

        predict_ai = AI()
        predict_recomm = RECOMM()

        ai_err_code = ""
        ai_err_type = ""
        ai_err_desc = ""
        ext_msg = ""
        recomm_str = ""
        try:
            handler = InfoHandler(self._oracle_sql_struct)
            predict_json_data = handler.getData()
            logger.info(handler.error)

            try:
                predict_result = predict_ai.predict(predict_json_data)
            except Exception as ex:
                ai_err_code = "AIErr_Oracle_99997"
                ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
                ext_msg = "predict service result exception [{0}]".format(ex)
                predict_result = -1

            if predict_result == 0:
                try:
                    recomm_str = predict_recomm.predict(predict_json_data)
                except Exception as ex:
                    ai_err_code = "AIErr_Oracle_99998"
                    ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
                    ext_msg = "predict service result exception [{0}]".format(ex)
            else:
                recomm_str = ""

        # except AIException.SQLSyntaxError as ex:
        #     predict_result = -1
        #     error_msg = ex.message      # {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
        #     try:
        #         err_dict = json.loads(error_msg)
        #         ai_err_code = err_dict.get("AI_ERROR_CODE")
        #         ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
        #         ext_msg = err_dict.get("MESSAGE")
        #     except:
        #         ai_err_code = "AIErr_Oracle_99999"
        #         ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
        #         ext_msg = ex.message
        #
        # except AIException.SQLInfoMiss as ex:
        #     # predict_recomm = ""
        #     # predict_result = -1
        #     # self._oracle_sql_struct.message = str(ex.message)
        #
        #     predict_result = -1
        #     error_msg = ex.message  # {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
        #     try:
        #         err_dict = json.loads(error_msg)
        #         ai_err_code = err_dict.get("AI_ERROR_CODE")
        #         ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
        #         ext_msg = err_dict.get("MESSAGE")
        #     except:
        #         ai_err_code = "AIErr_Oracle_99999"
        #         ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
        #         ext_msg = ex.message
        #
        # except AIException.SQLConflict as ex:
        #     # predict_recomm = ""
        #     # predict_result = -1
        #     # self._oracle_sql_struct.message = str(ex.message)
        #
        #     predict_result = -1
        #     error_msg = ex.message  # {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
        #     try:
        #         err_dict = json.loads(error_msg)
        #         ai_err_code = err_dict.get("AI_ERROR_CODE")
        #         ext_msg = err_dict.get("MESSAGE")
        #     except:
        #         ai_err_code = "AIErr_Oracle_99999"
        #         ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
        #         ext_msg = ex.message
        #
        # except AIException.SQLHandleError as ex:
        #     # predict_recomm = ""
        #     # predict_result = -1
        #     # self._oracle_sql_struct.message = str(ex.message)
        #
        #     predict_result = -1
        #     error_msg = ex.message  # {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
        #     try:
        #         err_dict = json.loads(error_msg)
        #         ai_err_code = err_dict.get("AI_ERROR_CODE")
        #         ext_msg = err_dict.get("MESSAGE")
        #     except:
        #         ai_err_code = "AIErr_Oracle_99999"
        #         ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
        #         ext_msg = ex.message

        except Exception as ex:
            # predict_result = -1
            # ai_err_code = "AIErr_Oracle_99999"
            # ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
            # ext_msg = "Input handler result exception [{0}]".format(ex)

            predict_result = -1
            error_msg = str(ex)

            try:
                ai_err_code = ex.code
                ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
                ext_msg = ai_err_desc
            except:
                ai_err_code = "AIErr_Oracle_99999"
                ai_err_type, ai_err_desc = AIError.get_error_type_description(ai_err_code)
                ext_msg = error_msg

        if predict_result == 1:
            self._oracle_sql_struct.ai_result = "PASS"
        elif predict_result == 0:
            self._oracle_sql_struct.ai_result = "NOPASS"

            if recomm_str is not None and len(recomm_str) > 0:
                tag_list = predict_recomm.get_problem_tag(recomm_str)
                self._oracle_sql_struct.ai_program_type = [] if tag_list is None else tag_list
        else:
            self._oracle_sql_struct.ai_result = "INVALID"
            self._oracle_sql_struct.ai_error_code = ai_err_code
            self._oracle_sql_struct.ai_error_type = ai_err_type

        self._oracle_sql_struct.ai_recommend = recomm_str

        ai_result_dict = {
            "AI_RESULT": self._oracle_sql_struct.ai_result,
            "AI_RECOMMEND": self._oracle_sql_struct.ai_recommend,
            "MESSAGE": ai_err_desc
        }
        ai_sr_sql_detail.ai_result = json.dumps(ai_result_dict)
        ai_sr_sql_detail.ai_error_code = None if len(ai_err_code) <= 0 else ai_err_code
        ai_sr_sql_detail.message = ext_msg
        ai_sr_sql_detail.ai_program_type = ",".join(self._oracle_sql_struct.ai_program_type)
        ai_sr_sql_detail.save()
        return True
