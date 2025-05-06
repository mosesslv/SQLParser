# -*- coding: UTF-8 -*-

from common.OracleHandle import OracleHandle
from common.MysqlHandle import MysqlHandle
#from service.AISQLReview.ai_process import OracleAISQLReview
from service.AISQLReview.ai_process_v2 import OracleAISQLReview
from service.AISQLReview.sql_abstract import OracleSQLStruct, MysqlSQLStruct
import common.utils as utils
from common.DjangoMysqlHandle import DjMysqlHandle
from service.ai_sr_models import AISrProfileManage, AISrTenantSchema
from service.AISQLReview.handle_exception import DBConnectException
from service.AISQLReview.mysql_ai_process import MysqlAISQLReview
from common.pyos import Pyos
import json
import service.AISQLReview.AIError as AIError


SERVICE_PARAMETER_KEY = [   # 入参KEY定义
    "userid", "tenant", "profile_name", "schema", "sql_text", "sequence"
]


class ServiceEnter:
    """
    业务入口
    """
    def __init__(self, parameter_dict):
        # ch
        # eck parameter
        if not self._check_parameter(parameter_dict):
            msg = "parameter invalid"
            raise Exception(msg)

        # host = parameter_dict["host"]
        # port = parameter_dict["port"]
        # username = parameter_dict["username"]
        # passwd = parameter_dict["passwd"]
        # instance_name = parameter_dict["instance_name"]

        self._userid = parameter_dict["userid"]
        self._profile_name = parameter_dict["profile_name"]
        self._sql_struct_sequence = parameter_dict["sequence"]
        self._sql_struct_tenant = parameter_dict["tenant"]
        self._sql_struct_schema = parameter_dict["schema"]
        self._sql_struct_sql_text = parameter_dict["sql_text"]

        db_type, conn_result = self._confirm_db_type_by_schema_profile(
            self._userid, self._sql_struct_tenant, self._profile_name, self._sql_struct_schema)

        if utils.str_is_none_or_empty(db_type.upper()):
            ai_err_code = "AIErr_00001"
            message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
            raise DBConnectException(json.dumps(message_dict))

        if conn_result is None:
            if db_type.upper() == "ORACLE":
                ai_err_code = "AIErr_Oracle_00004"
                message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
                raise DBConnectException(json.dumps(message_dict))

                # raise DBConnectException()
            elif db_type.upper() == "MYSQL":
                ai_err_code = "AIErr_Mysql_00004"
                message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
                raise DBConnectException(json.dumps(message_dict))

            else:
                ai_err_code = "AIErr_00001"
                message_dict = {"AI_ERROR_CODE": ai_err_code, "MESSAGE": ""}
                raise DBConnectException(json.dumps(message_dict))

        self._db_type = db_type
        self._db_handle = conn_result

        # self._oracle_handle = self._create_oracle_handle_by_profile_name(
        #     self._userid, self._sql_struct_tenant, self._profile_name, self._sql_struct_schema)
        #
        # if self._oracle_handle is None:
        #     raise DBConnectException()
        self.last_error_info = ""

    def _confirm_db_type_by_schema_profile(self, userid, tenant, profile_name, schema_name) -> [str, object]:
        """
        根据入参确定 DB_TYPE 并且创建链接
        :param userid:
        :param tenant:
        :param profile_name:
        :param schema_name:
        :return:
        """
        django_conn = DjMysqlHandle()
        if tenant.upper().strip() == "LUFAX":
            # LUFAX 使用DPAA数据策略
            sql_text_schema_phy_info = "select a.schema_name, a.instance_name, b.instance_type, b.master_ip, " \
                                       "b.master_port, b.master_username, b.master_passwd,b.slave_ip, " \
                                       "b.slave_port, b.slave_username, b.slave_passwd from " \
                                       "(select instance_name, schema_name from dpaa_instance_schema_rel " \
                                       "where schema_name = '{0}') a " \
                                       "left join dpaa_instance_conn_info b " \
                                       "on a.instance_name = b.instance_name".format(schema_name)
            schema_phy_info_result = django_conn.mysql_execute_query_get_all_data(sql_text_schema_phy_info)
            if not schema_phy_info_result.result:
                self.last_error_info = schema_phy_info_result.message
                return "", None

            if len(schema_phy_info_result.data) <= 0:
                self.last_error_info = "schema can not find physical info"
                return "", None

            data = schema_phy_info_result.data[0]
            try:
                db_type = data[2]
                _host = data[3]
                port = data[4]
                instance_name = data[1]
                username = data[5]
                passwd = data[6]

                if self._confirm_host_is_dns(_host):
                    dns_to_ip = self._get_ip_from_dns(_host)
                    if utils.str_is_none_or_empty(dns_to_ip):
                        data_ip = _host
                    else:
                        data_ip = self._get_data_segment_ip(dns_to_ip)
                else:
                    # IP
                    data_ip = self._get_data_segment_ip(_host)

                host = data_ip
            except Exception as ex:
                msg = "schema [{0}] physical info invalid [{1}]".format(schema_name, ex)
                self.last_error_info = msg
                return "", None

            if db_type.upper().strip() == "ORACLE":
                try:
                    db_handle = OracleHandle(username, passwd, instance_name, host, port)
                except Exception as ex:
                    msg = "schema [{0}] create oracle connect exception [{1}]".format(schema_name, ex)
                    self.last_error_info = msg
                    return "ORACLE", None
            elif db_type.upper().strip() == "MYSQL":
                try:
                    db_handle = MysqlHandle(username, passwd, host, port, schema_name)
                except Exception as ex:
                    msg = "schema [{0}] create oracle connect exception [{1}]".format(schema_name, ex)
                    self.last_error_info = msg
                    return "MYSQL", None
            else:
                msg = "unknown db type [{0}]".format(db_type)
                self.last_error_info = msg
                return "", None
            return db_type.upper(), db_handle
        else:
            # 客户的ORACLE连接从PROFILE表中获取
            try:
                # profile_manage = AISrProfileManage.objects.get(userid=userid, tenant=tenant,
                #                                                profile_name=profile_name)
                # profile_manage_data = AISrProfileManage.objects.filter(tenant=tenant)
                # profile_manage = profile_manage_data[0]
                tenant_schema_data = AISrTenantSchema.objects.filter(tenant=tenant, schema_name=schema_name)[0]
                profile_manage = AISrProfileManage.objects.filter(tenant=tenant, profile_name=tenant_schema_data.profile_name)[0]

            except Exception as ex:
                msg = "userid[{0}] tenant[{1}] profile[{2}] can not find valid data [{3}]". \
                    format(userid, tenant, profile_name, ex)
                self.last_error_info = msg
                return "", None

            try:
                db_type = profile_manage.db_type
                host = profile_manage.host
                port = profile_manage.port
                instance_name = profile_manage.instance_name
                username = profile_manage.username
                passwd = profile_manage.passwd
                if db_type.upper().strip() == "ORACLE":
                    db_handle = OracleHandle(username, passwd, instance_name, host, port)
                elif db_type.upper().strip() == "MYSQL":
                    db_handle = MysqlHandle(username, passwd, host, port, schema_name)
                else:
                    msg = "unknown db type [{0}]".format(db_type)
                    self.last_error_info = msg
                    return "", None
            except Exception as ex:
                msg = "schema [{0}] create connect exception [{1}]".format(schema_name, ex)
                self.last_error_info = msg
                return "", None
            return db_type.upper(), db_handle

    # noinspection PyMethodMayBeStatic
    def _confirm_host_is_dns(self, host):
        """
        确认 HOST 是DNS 或 IP
        :param host:
        :return: bool
        """
        idx_lufax_sotrage = host.lower().find(".lufax.storage")
        if idx_lufax_sotrage < 0:
            return False
        else:
            return True

    # noinspection PyMethodMayBeStatic
    def _get_ip_from_dns(self, dns_name):
        """
        反向解析出IP
        :param dns_name:
        :return: STR
        """
        pyos = Pyos()
        dig_command = "dig {0} +short".format(dns_name)
        pyos.call_os_shell(dig_command)
        if pyos.return_code != 0 or pyos.process_info.return_code != 0:
            return ""

        out = str(pyos.process_info.stdout_info, encoding="utf-8")
        content_list = utils.list_str_item_clear_empty_line_and_duplication_data(out.replace("\r", "").split("\n"))
        ip = content_list[-1]
        return ip

    # noinspection PyMethodMayBeStatic
    def _get_data_segment_ip(self, ip):
        """
        获取数据段IP, 解决 LUFAX 网络问题
        :param ip:
        :return: str
        """
        ip_split = ip.split(".")
        if ip_split[0] == "30":
            ip_split[0] = "31"

        return ".".join(ip_split)

    def _create_oracle_handle_by_profile_name(self, userid, tenant, profile_name, schema_name=""):
        """
        通过profile name创建oracle handle; 租户和策略确认唯一的物理策略
        profile_name管理物理连接策略数据
        :param tenant:
        :param profile_name:
        :param schema_name: 当tenant为LUFAX时, 以DPAA的逻辑创建ORACLE HANDLE
        :return: oracle_handle
        """
        oracle_handle_django_conn = DjMysqlHandle()
        if tenant.upper().strip() == "LUFAX":
            # LUFAX 使用DPAA数据策略
            sql_oracle_phy_info = "select a.schema_name, a.instance_name, b.instance_type, b.master_ip, " \
                                  "b.master_port, b.master_username, b.master_passwd,b.slave_ip, " \
                                  "b.slave_port, b.slave_username, b.slave_passwd from " \
                                  "(select instance_name, schema_name from dpaa_instance_schema_rel " \
                                  "where instance_type = 'ORACLE' and schema_name = '{0}') a " \
                                  "left join dpaa_instance_conn_info b " \
                                  "on a.instance_name = b.instance_name".format(schema_name)
            oracle_handle_phy_info = oracle_handle_django_conn.mysql_execute_query_get_all_data(sql_oracle_phy_info)
            if not oracle_handle_phy_info.result:
                self.last_error_info = oracle_handle_phy_info.message
                return None

            if len(oracle_handle_phy_info.data) <= 0:
                self.last_error_info = "schema can not find oracle physical info"
                return None

            data = oracle_handle_phy_info.data[0]
            try:
                host = data[3]
                port = data[4]
                instance_name = data[1]
                username = data[5]
                passwd = data[6]
            except Exception as ex:
                msg = "schema [{0}] oracle physical info invalid [{1}]".format(schema_name, ex)
                self.last_error_info = msg
                return None

            try:
                oracle_handle = OracleHandle(username, passwd, instance_name, host, port)
            except Exception as ex:
                msg = "schema [{0}] create oracle connect exception [{1}]".format(schema_name, ex)
                self.last_error_info = msg
                return None

            return oracle_handle
        else:
            # 客户的ORACLE连接从PROFILE表中获取
            try:
                profile_manage = AISrProfileManage.objects.get(userid=userid, tenant=tenant, profile_name=profile_name)
            except Exception as ex:
                msg = "userid[{0}] tenant[{1}] profile[{2}] can not find valid data [{3}]".\
                    format(userid, tenant, profile_name, ex)
                self.last_error_info = msg
                return None

            try:
                host = profile_manage.host
                port = profile_manage.port
                instance_name = profile_manage.instance_name
                username = profile_manage.username
                passwd = profile_manage.passwd
                oracle_handle = OracleHandle(username, passwd, instance_name, host, port)
            except Exception as ex:
                msg = "schema [{0}] create oracle connect exception [{1}]".format(schema_name, ex)
                self.last_error_info = msg
                return None

            return oracle_handle

    def _check_parameter(self, parameter) -> bool:
        """
        检查参数
        :param parameter:
        :return: bool
        """
        try:
            keys = parameter.keys()
        except Exception as ex:
            msg = "parameter invalid, exception [{0}]".format(ex)
            self.last_error_info = msg
            return False

        diff = list(set(SERVICE_PARAMETER_KEY).difference((set(keys))))
        if len(diff) <= 0:
            return True
        else:
            return False

    def predict(self) -> dict:
        """
        AI 服务调用
        :return: dict ->
            {
                "AI_RESULT": "PASS | NO PASS | EXCEPTION",
                "AI_RECOMMEND": "XXXXXXXXX",
                "MESSAGE": "xxxxxxx"
            }
        """
        if self._db_type.upper().strip() == "ORACLE":
            oracle_sql_struct = OracleSQLStruct()
            oracle_sql_struct.sequence = utils.get_uuid() if utils.str_is_none_or_empty(self._sql_struct_sequence) \
                else str(self._sql_struct_sequence)
            oracle_sql_struct.tenant_code = "" if utils.str_is_none_or_empty(self._sql_struct_tenant) \
                else str(self._sql_struct_tenant)
            oracle_sql_struct.data_handle_result = False
            oracle_sql_struct.message = ""
            oracle_sql_struct.oracle_conn = self._db_handle
            oracle_sql_struct.schema_name = self._sql_struct_schema
            oracle_sql_struct.sql_text = self._sql_struct_sql_text

            try:
                oracle_ai_sqlreview = OracleAISQLReview(oracle_sql_struct)
            except Exception as ex:
                ai_err_code = "AIErr_Oracle_99999"
                type_str, desc_str = AIError.get_error_type_description(ai_err_code)
                # msg = "ai sqlreview struct exception [{0}]".format(ex)

                return {
                    "AI_RESULT": "INVALID",
                    "AI_RECOMMEND": "",
                    "MESSAGE": desc_str
                }

            ai_result = oracle_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            if not ai_result:
                return {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }

            return {
                "AI_RESULT": oracle_sql_struct.ai_result,
                "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                "MESSAGE": desc_str
            }

        elif self._db_type.upper().strip() == "MYSQL":
            mysql_sql_struct = MysqlSQLStruct()
            mysql_sql_struct.sequence = utils.get_uuid() if utils.str_is_none_or_empty(self._sql_struct_sequence) \
                else str(self._sql_struct_sequence)
            mysql_sql_struct.tenant_code = "" if utils.str_is_none_or_empty(self._sql_struct_tenant) \
                else str(self._sql_struct_tenant)
            mysql_sql_struct.data_handle_result = False
            mysql_sql_struct.message = ""
            mysql_sql_struct.mysql_conn = self._db_handle
            mysql_sql_struct.schema_name = self._sql_struct_schema
            mysql_sql_struct.sql_text = self._sql_struct_sql_text

            try:
                mysql_ai_sqlreview = MysqlAISQLReview(mysql_sql_struct)
            except Exception as ex:
                ai_err_code = "AIErr_Mysql_99999"
                type_str, desc_str = AIError.get_error_type_description(ai_err_code)

                # msg = "ai sqlreview struct exception [{0}]".format(ex)
                return {
                    "AI_RESULT": "INVALID",
                    "AI_RECOMMEND": "",
                    "MESSAGE": desc_str
                }

            ai_result = mysql_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(mysql_sql_struct.ai_error_code)
            if not ai_result:
                return {
                    "AI_RESULT": mysql_sql_struct.ai_result,
                    "AI_RECOMMEND": mysql_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }

            return {
                "AI_RESULT": mysql_sql_struct.ai_result,
                "AI_RECOMMEND": mysql_sql_struct.ai_recommend,
                "MESSAGE": desc_str
            }
        else:
            return {
                "AI_RESULT": "INVALID",
                "AI_RECOMMEND": "",
                "MESSAGE": "未知数据库类型"
            }


class ServiceEnterV2:
    """
    AI 服务入口
    提供单一SQL的AI服务
    """
    def __init__(self, db_type, host, port, instance_name, username, passpord):
        self._db_type = db_type
        self._host = host
        self._port = port
        self._instance_name = instance_name
        self._username = username
        self._passpord = passpord

        if self._db_type.upper() == "ORACLE":
            try:
                self._oracle_handle = OracleHandle(
                    self._username, self._passpord, self._instance_name, self._host, port=self._port)
            except Exception as ex:
                raise Exception("DB初始化连接失败")

        elif self._db_type.upper() == "MYSQL":
            try:
                self._mysql_handle = MysqlHandle(self._username, self._passpord, self._host, port=self._port)
            except Exception as ex:
                raise Exception("DB初始化连接失败")

        else:
            msg = "未知数据库类型"
            raise Exception(msg)

        # 增加账号的权限验证

    def predict_sqltext(self, schema, sql_text):
        """
        SQL文本预测
        :param schema:
        :param sql_text:
        :return:
        """
        if self._db_type.upper().strip() == "ORACLE":
            oracle_sql_struct = OracleSQLStruct()
            oracle_sql_struct.sequence = utils.get_uuid().replace("-", "")
            oracle_sql_struct.tenant_code = "PALLAS"
            oracle_sql_struct.data_handle_result = False
            oracle_sql_struct.message = ""
            oracle_sql_struct.oracle_conn = self._oracle_handle
            oracle_sql_struct.schema_name = schema
            oracle_sql_struct.sql_text = sql_text

            try:
                oracle_ai_sqlreview = OracleAISQLReview(oracle_sql_struct)
            except Exception as ex:
                ai_err_code = "AIErr_Oracle_99999"
                type_str, desc_str = AIError.get_error_type_description(ai_err_code)
                return {
                    "AI_RESULT": "INVALID",
                    "AI_RECOMMEND": "",
                    "MESSAGE": desc_str
                }

            ai_result = oracle_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(oracle_sql_struct.ai_error_code)
            if not ai_result:
                return {
                    "AI_RESULT": oracle_sql_struct.ai_result,
                    "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }

            return {
                "AI_RESULT": oracle_sql_struct.ai_result,
                "AI_RECOMMEND": oracle_sql_struct.ai_recommend,
                "MESSAGE": desc_str
            }

        elif self._db_type.upper().strip() == "MYSQL":
            mysql_sql_struct = MysqlSQLStruct()
            mysql_sql_struct.sequence = utils.get_uuid().replace("-", "")
            mysql_sql_struct.tenant_code = "PALLAS"
            mysql_sql_struct.data_handle_result = False
            mysql_sql_struct.message = ""
            mysql_sql_struct.mysql_conn = self._mysql_handle
            mysql_sql_struct.schema_name = schema
            mysql_sql_struct.sql_text = sql_text

            # switch mysql schema
            use_sql_text = "use {0}".format(schema)
            self._mysql_handle.mysql_execute_dml_sql(use_sql_text)

            try:
                mysql_ai_sqlreview = MysqlAISQLReview(mysql_sql_struct)
            except Exception as ex:
                ai_err_code = "AIErr_Mysql_99999"
                type_str, desc_str = AIError.get_error_type_description(ai_err_code)

                # msg = "ai sqlreview struct exception [{0}]".format(ex)
                return {
                    "AI_RESULT": "INVALID",
                    "AI_RECOMMEND": "",
                    "MESSAGE": desc_str
                }

            ai_result = mysql_ai_sqlreview.sql_predict()
            type_str, desc_str = AIError.get_error_type_description(mysql_sql_struct.ai_error_code)
            if not ai_result:
                return {
                    "AI_RESULT": mysql_sql_struct.ai_result,
                    "AI_RECOMMEND": mysql_sql_struct.ai_recommend,
                    "MESSAGE": desc_str
                }

            return {
                "AI_RESULT": mysql_sql_struct.ai_result,
                "AI_RECOMMEND": mysql_sql_struct.ai_recommend,
                "MESSAGE": desc_str
            }
        else:
            return {
                "AI_RESULT": "INVALID",
                "AI_RECOMMEND": "",
                "MESSAGE": "未知数据库类型"
            }
