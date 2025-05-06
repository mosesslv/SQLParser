# -*- coding:utf-8 -*-

import service.grpc_channel.oracle_sql_predict_pb2_grpc as oracle_sql_predict_pb2_grpc
import service.grpc_channel.oracle_sql_predict_pb2 as oracle_sql_predict_pb2
from service.AISQLReview.service_enter import ServiceEnter
from service.django_decorator.orm_decorator import decorator_close_db_connections
from service.AISQLReview.handle_exception import DBConnectException
import service.AISQLReview.AIError as AIError
import json
import logging
logger = logging.getLogger("grpc")


class OracleSQLPredictRequester(oracle_sql_predict_pb2_grpc.OracleSQLPredictRequesterServicer):

    @decorator_close_db_connections
    def SQLPredict(self, request, context):
        assert isinstance(request, oracle_sql_predict_pb2.OracleSQLPredictRequest)
        sql_predict_response = oracle_sql_predict_pb2.OracleSQLPredictResponse()

        parameter_dict = {}
        try:
            rpc_sequence = request.rpc_sequence
        except Exception as ex:
            msg = "rpc sequence invalid [{0}]".format(ex)
            sql_predict_response.rpc_sequence = ""
            sql_predict_response.ai_result = ""
            sql_predict_response.ai_recommend = ""
            sql_predict_response.message = str(msg)
            sql_predict_response.addition_json_string = ""
            return sql_predict_response

        logger.info("Oracle SQL Predict serivce recieve request [{0}]".format(rpc_sequence))

        try:
            parameter_dict["sequence"] = request.sql_sequence
            parameter_dict["userid"] = request.userid
            parameter_dict["tenant"] = request.tenant

            # parameter_dict["host"] = request.host
            # parameter_dict["port"] = request.port
            # parameter_dict["username"] = request.username
            # parameter_dict["passwd"] = request.passwd
            # parameter_dict["instance_name"] = request.instance_name
            parameter_dict["profile_name"] = request.profile_name

            parameter_dict["schema"] = request.schema
            parameter_dict["sql_text"] = request.sql_text
        except Exception as ex:
            msg = "parameter parser exception [{0}]".format(ex)
            sql_predict_response.rpc_sequence = rpc_sequence
            sql_predict_response.ai_result = ""
            sql_predict_response.ai_recommend = ""
            sql_predict_response.message = str(msg)
            sql_predict_response.addition_json_string = ""
            logger.info("{0}".format(msg))
            return sql_predict_response

        try:
            sql_predict_service = ServiceEnter(parameter_dict)
            sql_predict_result_dict = sql_predict_service.predict()
            # {
            #     "AI_RESULT": "PASS | NO PASS | EXCEPTION",
            #     "AI_RECOMMEND": "XXXXXXXXX",
            #     "MESSAGE": "xxxxxxx"
            # }
            sql_predict_response.rpc_sequence = rpc_sequence
            sql_predict_response.ai_result = sql_predict_result_dict["AI_RESULT"]
            sql_predict_response.ai_recommend = sql_predict_result_dict["AI_RECOMMEND"]
            sql_predict_response.message = sql_predict_result_dict["MESSAGE"]
            sql_predict_response.addition_json_string = ""

        except DBConnectException as ex:
            # sql_predict_response.rpc_sequence = rpc_sequence
            # sql_predict_response.ai_result = "INVALID"
            # sql_predict_response.ai_recommend = ""
            # sql_predict_response.message = ex.message
            # sql_predict_response.addition_json_string = ""

            ai_result = "INVALID"
            ai_recommend = ""
            ai_message = ""

            err_str = ex.message
            try:
                err_dict = json.loads(err_str)
                err_code = err_dict["AI_ERROR_CODE"]
                ext_msg = err_dict["MESSAGE"]
                type_str, desc_str = AIError.get_error_type_description(err_code)
                ai_message = desc_str
            except Exception as ex:
                ai_message = err_str

            sql_predict_response.rpc_sequence = rpc_sequence
            sql_predict_response.ai_result = ai_result
            sql_predict_response.ai_recommend = ai_recommend
            sql_predict_response.message = ai_message
            sql_predict_response.addition_json_string = ""

        except Exception as ex:
            msg = "sql predict exception [{0}]".format(ex)
            sql_predict_response.rpc_sequence = rpc_sequence
            sql_predict_response.ai_result = "INVALID"
            sql_predict_response.ai_recommend = ""
            sql_predict_response.message = msg
            sql_predict_response.addition_json_string = ""

        logger.info("[{0}] predict result [{1}\n{2}\n{3}]".format(
            sql_predict_response.rpc_sequence, sql_predict_response.ai_result,
            sql_predict_response.ai_recommend, sql_predict_response.message))
        return sql_predict_response
