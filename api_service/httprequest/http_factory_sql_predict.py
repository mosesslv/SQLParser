# -*- coding: UTF-8 -*-

"""
METHOD:SQLPREDICT

POST DATA: {
    "method": "SQLPREDICT",
    "call_type": "SYNC",
    "result": "",
    "message": "",
    "userid": "XXX",
    "tenant": "XXX",
    "profile_name": "XXX",
    "schema": "XXX",
    "sql_text": "XXX",
    "sequence": "UUID"          # 默认为空, 新数据为空
}

DEMO:
{
    "method": "SQLPREDICT",
    "call_type": "SYNC",
    "result": "",
    "message": "",
    "userid": "SHM",
    "tenant": "LUFAX",
    "profile_name": "LUFAX",
    "schema": "kefdata",
    "sql_text": "select * from BECS_MQ_CLIENT_MSG",
    "sequence": ""
}

curl -H "Content-Type: application/json" -X POST "http://127.0.0.1:8000/service/http_api" -d '{"method": "SQLPREDICT", "call_type": "SYNC", "result": "", "message": "", "userid": "SHM", "tenant": "LUFAX", "profile_name": "LUFAX", "schema": "kefdata", "sql_text": "select * from BECS_MQ_CLIENT_MSG", "sequence": ""}'

"""

from api_service.httprequest.http_factory_common import HttpAbstractBaseObject
from service.AISQLReview.service_enter import ServiceEnter
from service.AISQLReview.handle_exception import DBConnectException
import common.utils
import json
import logging
logger = logging.getLogger("")


class SQLPredict(HttpAbstractBaseObject):
    def handle(self) -> dict:
        logger.info("[{0}] ai sqlreview request [{1}]".
                    format(common.utils.get_current_class_methord_name(self), self._http_data))
        try:
            method = self._http_data["http_request_value"]["method"]
            call_type = self._http_data["http_request_value"]['call_type']
        except Exception as ex:
            msg = "http request parser exception [{0}]".format(ex)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            json_dict = {"method": "UNKKNOWN", "call_type": "UNKKNOWN", "result": "FAILED", "message": msg}
            return json_dict

        if method.upper().strip() != "SQLPREDICT":
            msg = "[{0}] request method invalid".format(method)
            logger.error(msg)
            json_dict = {"method": method, "call_type": call_type, "result": "FAILED", "message": msg}
            return json_dict

        try:
            http_request_value = self._http_data["http_request_value"]
            data_dict = {
                "userid": http_request_value["userid"],
                "tenant": http_request_value["tenant"],
                "profile_name": http_request_value["profile_name"],
                "schema": http_request_value["schema"],
                "sql_text": http_request_value["sql_text"],
                "sequence": http_request_value["sequence"]
            }
        except Exception as ex:
            msg = "parameter parser failed exception [{0}]".format(ex)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            json_dict = {"method": method, "call_type": call_type, "result": "FAILED", "message": msg}
            return json_dict

        try:
            sql_predict_service = ServiceEnter(data_dict)
            sql_predict_result_dict = sql_predict_service.predict()
        except DBConnectException as ex:
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), ex.message))
            json_dict = {"method": method, "call_type": call_type, "result": "FAILED", "message": ex.message}
            return json_dict
        except Exception as ex:
            msg = "predict exception [{0}]".format(ex)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            json_dict = {"method": method, "call_type": call_type, "result": "FAILED", "message": msg}
            return json_dict

        # {
        #     "AI_RESULT": "PASS | NO PASS | EXCEPTION",
        #     "AI_RECOMMEND": "XXXXXXXXX",
        #     "MESSAGE": "xxxxxxx"
        # }
        json_dict = {
            "method": method, "call_type": call_type, "result": "SUCCESS",
            "message": sql_predict_result_dict["MESSAGE"],
            "ai_result": sql_predict_result_dict["AI_RESULT"],
            "ai_recommend": sql_predict_result_dict["AI_RECOMMEND"],
        }

        return json_dict
