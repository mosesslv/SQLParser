# -*- coding: UTF-8 -*-

"""
METHOD: CLOUD_GET_SCHEMAS_BY_USERID_TENANT

POST DATA: {
    "method": "CLOUD_GET_SCHEMAS_BY_USERID_TENANT",
    "call_type": "SYNC",
    "result": "",
    "message": "",
    "userid": "XXX",
    "tenant": "XXX",
}

DEMO:
{
    "method": "CLOUD_GET_SCHEMAS_BY_USERID_TENANT",
    "call_type": "SYNC",
    "result": "",
    "message": "",
    "userid": "shm",
    "tenant": "LUFAX"
}

RETURN:
{
    "method": "CLOUD_GET_SCHEMAS_BY_USERID_TENANT",
    "call_type": "SYNC",
    "result": "",
    "message": "",
    "schemas": {"ORACLE": [XXX, XXX, XXX], "MYSQL": [xxx, xxx, xx]}
}

curl -H "Content-Type: application/json" -X POST "http://127.0.0.1:8000/service/http_api" -d '{"method": "CLOUD_GET_SCHEMAS_BY_USERID_TENANT", "call_type": "SYNC", "result": "", "message": "", "userid": "shm", "tenant": "LUFAX"}'

"""

from api_service.httprequest.http_factory_common import HttpAbstractBaseObject
import common.utils
from common.DjangoMysqlHandle import DjMysqlHandle
from service.ai_sr_models import AISrTenantSchema
import json
import logging
logger = logging.getLogger("")


class CloudGetSchemasByUseridTenant(HttpAbstractBaseObject):
    def handle(self) -> dict:
        logger.info("[{0}] get CLOUD_GET_SCHEMAS_BY_USERID_TENANT request [{1}]".
                    format(common.utils.get_current_class_methord_name(self), self._http_data))
        try:
            method = self._http_data["http_request_value"]["method"]
            call_type = self._http_data["http_request_value"]['call_type']
        except Exception as ex:
            msg = "http request parser exception [{0}]".format(ex)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            json_dict = {"method": "UNKKNOWN", "call_type": "UNKKNOWN", "result": "FAILED", "message": msg}
            return json_dict

        if method.upper().strip() != "CLOUD_GET_SCHEMAS_BY_USERID_TENANT":
            msg = "[{0}] request method invalid".format(method)
            logger.error(msg)
            json_dict = {"method": method, "call_type": call_type, "result": "FAILED", "message": msg}
            return json_dict

        try:
            http_request_value = self._http_data["http_request_value"]
            userid = http_request_value["userid"]
            tenant = http_request_value["tenant"]

        except Exception as ex:
            msg = "parameter parser failed exception [{0}]".format(ex)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            json_dict = {"method": method, "call_type": call_type, "result": "FAILED", "message": msg}
            return json_dict

        if tenant.upper().strip() == "LUFAX":
            schema_dict = self._get_lufax_schemas()
        else:
            schema_dict = self._get_tenant_schemas(tenant)

        json_dict = {
            "method": method, "call_type": call_type, "result": "SUCCESS",
            "message": "",
            "schemas": schema_dict
        }
        return json_dict

    # noinspection PyMethodMayBeStatic
    def _get_lufax_schemas(self) -> dict:
        """
        LUFAX 租户SCHEMA获取
        :return: dict
            {"ORACLE": [XXX, XXX, XXX], "MYSQL": [xxx, xxx, xx]}
        """
        dpaa_mysql_conn = DjMysqlHandle()
        sql_text_schema = "select instance_type, schema_name from dpaa_instance_schema_rel"
        schema_result = dpaa_mysql_conn.mysql_execute_query_get_all_data(sql_text_schema)
        if not schema_result.result:
            msg = "[{0}] run sql failed [{1}]".format(sql_text_schema, schema_result.message)
            logger.error(msg)
            return None

        oracle_schemas = []
        mysql_schemas = []

        for row in schema_result.data:
            instance_type = row[0]
            schema_name = row[1]
            if instance_type.upper().strip() == "ORACLE":
                oracle_schemas.append(schema_name)
            elif instance_type.upper().strip() == "MYSQL":
                mysql_schemas.append(schema_name)
            else:
                pass
        # end for

        schema_dict = {
            "ORACLE": oracle_schemas,
            "MYSQL": mysql_schemas
        }
        return schema_dict

    # noinspection PyMethodMayBeStatic
    def _get_tenant_schemas(self, tenant) -> dict:
        """
        租户SCHEMA获取
        :return: dict
            {"ORACLE": [XXX, XXX, XXX], "MYSQL": [xxx, xxx, xx]}
        """
        tenant_schemas = AISrTenantSchema.objects.filter(tenant=tenant)
        oracle_schemas = []
        mysql_schemas = []

        for tenant_schema in tenant_schemas:
            if tenant_schema.db_type.upper() == "ORACLE":
                oracle_schemas.append(tenant_schema.schema_name)
            elif tenant_schema.db_type.upper() == "MYSQL":
                mysql_schemas.append(tenant_schema.schema_name)
            else:
                pass
        # end for

        schema_dict = {
            "ORACLE": oracle_schemas,
            "MYSQL": mysql_schemas
        }
        return schema_dict
