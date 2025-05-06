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
import copy
from typing import Optional, Tuple
from service.predict_sql_review_oracle.Utility.OracleExplainAnalysis import to_Dataframe
import numpy as np

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
            self.ai_err_code = "AIErr_Oracle_00005"
            message_dict = {"AI_ERROR_CODE": self.ai_err_code, "MESSAGE": msg}
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
            self.ai_err_code = "AIErr_Oracle_00006"
            message_dict = {"AI_ERROR_CODE": self.ai_err_code, "MESSAGE": msg}
            raise Exception(json.dumps(message_dict))

        # 补充sequence
        if utils.str_is_none_or_empty(self._oracle_sql_struct.sequence):
            self._oracle_sql_struct.sequence = utils.get_uuid()

        # 确认ORACLE HANDLE
        if self._oracle_sql_struct.oracle_conn is None:
            self.ai_err_code = "AIErr_Oracle_00004"
            message_dict = {"AI_ERROR_CODE": self.ai_err_code, "MESSAGE": ""}
            raise Exception(json.dumps(message_dict))
        else:
            self._oracle_common = OracleCommon(self._oracle_sql_struct.oracle_conn)

        # 初始全局变量
        self.ext_msg = ''
        self.ai_err_code = ""
        self.ai_err_type = ""
        self.ai_err_desc = ""

    ####################################
    #          core functions          #
    ####################################

    def handle_struct_data(self, oracle_sql_struct: OracleSQLStruct) -> bool:
        """
        处理获取数据结构相关数据
        :return: bool
        """
        plan_dict = self._oracle_common.get_explain(oracle_sql_struct.schema_name,
                                                    oracle_sql_struct.sql_text)
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
            oracle_sql_struct.data_handle_result = False

            oracle_sql_struct.ai_error_code = "AIErr_Oracle_10000"
            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            oracle_sql_struct.ai_error_type = type_str
            oracle_sql_struct.message = desc_str

            return False

        if not plan_result:
            # msg = "[{0}] sql sync error, get plan data error [{1}]".format(plan_dict, plan_error)
            msg = plan_error
            self.last_error_info = msg
            logger.error("[0] {1}".format(utils.get_current_class_methord_name(self), msg))
            oracle_sql_struct.data_handle_result = False

            if plan_error.find("ORA-00933") >= 0 or plan_error.find("ORA-00907") >= 0 or plan_error.find(
                    "ORA-00905") >= 0:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10001"
            elif plan_error.find("ORA-00904") >= 0:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10002"
            elif plan_error.find("ORA-00902") >= 0:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10003"
            elif plan_error.find("ORA-01747") >= 0:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10004"
            elif plan_error.find("ORA-00909") >= 0:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10005"
            elif plan_error.find("ORA-00942") >= 0:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10006"
            else:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_10000"

            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            oracle_sql_struct.ai_error_type = type_str
            oracle_sql_struct.message = msg

            return False

        oracle_sql_struct.plan_raw = plan_raw
        oracle_sql_struct.plan_text = plan_text

        # handle table info
        for table_name in oracle_sql_struct.table_names:
            tab_meta_obj = self._get_table_info(oracle_sql_struct.schema_name, table_name)
            if tab_meta_obj is None:
                oracle_sql_struct.ai_error_code = "AIErr_Oracle_00007"
                type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
                oracle_sql_struct.ai_error_type = type_str
                oracle_sql_struct.message = self.last_error_info

                return False
            oracle_sql_struct.tab_info.append(tab_meta_obj)
        # end for

        # handle histogram
        for table_name in oracle_sql_struct.table_names:
            histogram_data = self._oracle_common.get_view_dba_histogram(oracle_sql_struct.schema_name, table_name)
            if histogram_data is None:
                self.last_error_info = self._oracle_common.last_error_info

                oracle_sql_struct.ai_error_code = "AIErr_Oracle_00008"
                type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
                oracle_sql_struct.ai_error_type = type_str
                oracle_sql_struct.message = self.last_error_info

                return False
            oracle_sql_struct.view_histogram.append(histogram_data)
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
            msg = "[{0}.{1}] handle table meta failed [{2}]". \
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
            msg = "[{0}] addition key COLUMNS_DISCRIMINATION exception [{1}]". \
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
        # -- debug purpose -- #
        from termcolor import colored
        # -- input origin sql --#
        result = self.handle_struct_data(self._oracle_sql_struct)

        prdict_status, self.oracle_sql_struct, opt_sql = self.predict(self._oracle_sql_struct, handle_statu=result)

        if opt_sql != '' and 'hint' in  self.oracle_sql_struct.ai_recommend:
            explain = self._oracle_sql_struct.plan_raw
            # --- fetch new explain --#
            oracle_sql_struct_opt = self._oracle_sql_struct.copy()
            oracle_sql_struct_opt.sql_text = opt_sql
            self.handle_struct_data(oracle_sql_struct_opt)
            explain_opt = oracle_sql_struct_opt.plan_raw
            # --- it is better explain --- #
            if self.is_better_explain_Cost(explain, explain_opt):
                # -- update opt result
                self._oracle_sql_struct.plan_text_opt = oracle_sql_struct_opt.plan_text
                self._oracle_sql_struct.sql_text_opt = opt_sql
                print(colored(self.oracle_sql_struct.ai_recommend, 'green'))

                print(colored(self.oracle_sql_struct.sql_text, 'yellow'))
                print(colored(self.oracle_sql_struct.plan_text, 'yellow'))

                print(colored(self.oracle_sql_struct.sql_text_opt, 'yellow'))
                print(colored(self.oracle_sql_struct.plan_text_opt, 'yellow'))
                import pdb
                pdb.set_trace()
            else:
                # --- overwrite recommendation -- #
                self.oracle_sql_struct.ai_recommend = self.no_better_recom(self.oracle_sql_struct.ai_recommend)
                print(colored(self.oracle_sql_struct.ai_recommend,'red'))
        self.save_to_ai_sr_sql_detail(self.oracle_sql_struct)


        return prdict_status

    ####################################
    #     assist utility functions     #
    ####################################
    def is_better_explain_Cardinality(self, explain_left: list, explain_right: list) -> bool:
        '''
        比较两个执行计划的好坏
        :param explain_left-左边执行计划
        :param explain_right-右边执行计划
        :return:　true ->   right 好于left
                  false -> otherwise
        '''
        explain_left = to_Dataframe(explain_left)
        explain_right = to_Dataframe(explain_right)
        left_v = np.sum(explain_left["CARDINALITY"]) + len(explain_left)
        right_v = np.sum(explain_right["CARDINALITY"]) + len(explain_right)
        if right_v < left_v:
            return True
        else:
            return False

    def is_better_explain_Cost(self, explain_left: list, explain_right: list) -> bool:
        '''
        比较两个执行计划的好坏
        :param explain_left-左边执行计划
        :param explain_right-右边执行计划
        :return:　true ->   right 好于left
                  false -> otherwise
        '''
        explain_left = to_Dataframe(explain_left)
        explain_right = to_Dataframe(explain_right)
        left_v = np.sum(explain_left["COST"])
        right_v = np.sum(explain_right["COST"])
        if right_v <= left_v:
            return True
        else:
            return False

    def no_better_recom(self, recom: str) -> str:
        recom_list = recom.split('\n')
        out_recom = ''
        for v in recom_list:
            if v in ['建议:', '加入:hint']:
                break
            else:
                out_recom += v + '\n'

        out_recom += '建议:\n\t请评估是否还有效率更高的扫描方式\n'
        return out_recom

    def predict(self, oracle_sql_struct: OracleSQLStruct, handle_statu: bool) -> Tuple[bool, OracleSQLStruct, str]:
        """
        AI SQLReview实现
        :return: bool 结果代表处理过程的正确与否, 不是AI判定结果; 结果见 oracle_sql_struct 对象
                OracleSQLStruct
                str: 优化过的sql
        """
        # record sql
        # --- check sequence --- #
        try:
            AISrSQLDetail.objects.get(sequence=oracle_sql_struct.sequence)
        except AISrSQLDetail.DoesNotExist:
            pass
        except Exception as ex:
            msg = "[{0} -> {1}] AISrSQLDetail orm exception [{2}]". \
                format(oracle_sql_struct.schema_name, oracle_sql_struct.sql_text, ex)
            self.last_error_info = msg
            return False, oracle_sql_struct, ''

        if not handle_statu:
            oracle_sql_struct.ai_result = "INVALID"
            oracle_sql_struct.ai_recommend = ""
            self.ai_err_type, self.ai_err_desc = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)

            return False, oracle_sql_struct, ''

        # call recommend arithmetic

        predict_ai = AI()
        predict_recomm = RECOMM()
        opt_sql = ''
        recomm_str = ""
        try:
            handler = InfoHandler(oracle_sql_struct)
            predict_json_data = handler.getData()
            logger.info(handler.error)

            try:
                predict_result = predict_ai.predict(predict_json_data)
            except Exception as ex:
                self.ai_err_code = "AIErr_Oracle_99997"
                self.ai_err_type, self.ai_err_desc = AIError.get_error_type_description(self.ai_err_code)
                self.ext_msg = "predict service result exception [{0}]".format(ex)
                predict_result = -1

            if predict_result == 0:
                try:
                    recomm_str, opt_sql = predict_recomm.predict(predict_json_data)
                    # collect _
                except Exception as ex:
                    self.ai_err_code = "AIErr_Oracle_99998"
                    self.ai_err_type, self.ai_err_desc = AIError.get_error_type_description(self.ai_err_code)
                    self.ext_msg = "predict service result exception [{0}]".format(ex)
            else:
                recomm_str = ""

        except Exception as ex:

            predict_result = -1
            error_msg = str(ex)  # {"AI_ERROR_CODE": self.ai_err_code, "MESSAGE": ""}

            try:
                self.ai_err_code = ex.code
                self.ai_err_type, self.ai_err_desc = AIError.get_error_type_description(self.ai_err_code)
                self.ext_msg = self.ai_err_desc
            except:
                self.ai_err_code = "AIErr_Oracle_99999"
                self.ai_err_type, self.ai_err_desc = AIError.get_error_type_description(self.ai_err_code)
                self.ext_msg = error_msg

        if predict_result == 1:
            oracle_sql_struct.ai_result = "PASS"
        elif predict_result == 0:
            oracle_sql_struct.ai_result = "NOPASS"

            if recomm_str is not None and len(recomm_str) > 0:
                tag_list = predict_recomm.get_problem_tag(recomm_str)
                oracle_sql_struct.ai_program_type = [] if tag_list is None else tag_list
        else:
            oracle_sql_struct.ai_result = "INVALID"
            oracle_sql_struct.ai_error_code = self.ai_err_code
            oracle_sql_struct.ai_error_type = self.ai_err_type

        oracle_sql_struct.ai_recommend = recomm_str

        return True, oracle_sql_struct, opt_sql

    def save_to_ai_sr_sql_detail(self, oracle_sql_struct: OracleSQLStruct) -> None:
        '''
        将oracle_sql_struct 落数据库
        :param oracle_sql_struct, handle_status
        :return: None
        '''

        try:
            ai_sr_sql_detail = AISrSQLDetail.objects.get(sequence=oracle_sql_struct.sequence)
        except AISrSQLDetail.DoesNotExist:
            ai_sr_sql_detail = AISrSQLDetail()
            ai_sr_sql_detail.created_at = datetime.datetime.now()
            ai_sr_sql_detail.created_by = "SYS"
            ai_sr_sql_detail.updated_at = datetime.datetime.now()
            ai_sr_sql_detail.updated_by = "SYS"
            ai_sr_sql_detail.sequence = oracle_sql_struct.sequence
            ai_sr_sql_detail.tenant_code = oracle_sql_struct.tenant_code
            ai_sr_sql_detail.db_type = "ORACLE"
            ai_sr_sql_detail.schema_name = oracle_sql_struct.schema_name
            ai_sr_sql_detail.sql_text = oracle_sql_struct.sql_text
        except Exception as ex:
            msg = "[{0} -> {1}] AISrSQLDetail orm exception [{2}]". \
                format(oracle_sql_struct.schema_name, oracle_sql_struct.sql_text, ex)
            self.last_error_info = msg
            return

        ai_sr_sql_detail.tenant_code = self._oracle_sql_struct.tenant_code
        ai_sr_sql_detail.db_type = "ORACLE"
        ai_sr_sql_detail.schema_name = self._oracle_sql_struct.schema_name
        ai_sr_sql_detail.db_conn_url = "{0}:{1}/{2}".format(oracle_sql_struct.oracle_conn.get_address(),
                                                            oracle_sql_struct.oracle_conn.get_port(),
                                                            oracle_sql_struct.oracle_conn.get_instance_name())
        ai_sr_sql_detail.statement = oracle_sql_struct.statement
        ai_sr_sql_detail.dynamic_mosaicking = oracle_sql_struct.has_dynamic_mosaicking
        ai_sr_sql_detail.table_names = "" if oracle_sql_struct.table_names is None \
            else json.dumps(oracle_sql_struct.table_names)
        ai_sr_sql_detail.plan_text = oracle_sql_struct.plan_text
        ai_sr_sql_detail.plan_raw = "" if oracle_sql_struct.plan_raw is None \
            else json.dumps(oracle_sql_struct.plan_raw)

        tab_meta_list = []
        if oracle_sql_struct.tab_info is not None:
            for tab_meta_obj in oracle_sql_struct.tab_info:
                tab_meta_dict = tab_meta_obj.__dict__
                tab_meta_list.append(tab_meta_dict)

        sql_data = {
            "TABLE_INFO": tab_meta_list,
            "HISTOGRAM": [] if oracle_sql_struct.view_histogram is None
            else oracle_sql_struct.view_histogram,
            "ADDITION": {} if oracle_sql_struct.addition is None else oracle_sql_struct.addition
        }

        ai_sr_sql_detail.sql_data = json.dumps(sql_data)
        ai_sr_sql_detail.message = oracle_sql_struct.message
        #ai_sr_sql_detail.save()
        ai_result_dict = {
            "AI_RESULT": oracle_sql_struct.ai_result,
            "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
            "MESSAGE": self.ai_err_desc
        }
        ai_sr_sql_detail.ai_result = json.dumps(ai_result_dict)
        ai_sr_sql_detail.ai_error_code = None if len(self.ai_err_code) <= 0 else self.ai_err_code
        ai_sr_sql_detail.ai_error_type = self.ai_err_type
        ai_sr_sql_detail.message = self.ext_msg
        ai_sr_sql_detail.ai_program_type = ",".join(oracle_sql_struct.ai_program_type)
        ai_sr_sql_detail.save()
