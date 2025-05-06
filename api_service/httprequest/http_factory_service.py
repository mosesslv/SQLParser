# -*- coding: UTF-8 -*-

from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from service.ai_sr_models import AISrHttpApiLog
from api_service.httprequest.http_factory_common import HttpAbstractBaseObject
import common.utils
import json
from api_service.httprequest.http_factory_sql_predict import SQLPredict
from api_service.httprequest.http_factory_get_schemas_by_userid_tenant import CloudGetSchemasByUseridTenant
import logging

logger = logging.getLogger("")


class HttpFactory(object):
    """
    HTTP 接口工厂, 所有的处理类必须在此注册
    """
    def __init__(self, http_post_data_dict):
        assert isinstance(http_post_data_dict, dict)
        self._http_post_data_dict = http_post_data_dict
        try:
            self._method = self._http_post_data_dict["http_request_value"]["method"]
            # self._call_type = self._http_post_data_dict["http_request_value"]['call_type']
        except Exception as ex:
            msg = "http request parser exception [{0}]".format(ex)
            logger.error("[{0}] {1}".format(common.utils.get_current_class_methord_name(self), msg))
            raise Exception(msg)

    # noinspection PyMethodMayBeStatic
    def get_handle_class(self):
        """
        得到处理类, 类必须继承 HttpAbstractBaseObject , 且实现 handle 方法
        处理类必须注册到MAP
        """
        map_ = {
            "TEST": HttpTest({"method": "TEST", "call_type": "SYNC", "result": "", "message": ""}),
            "SQLPREDICT": SQLPredict(self._http_post_data_dict),
            "CLOUD_GET_SCHEMAS_BY_USERID_TENANT": CloudGetSchemasByUseridTenant(self._http_post_data_dict),

        }
        return map_[self._method]


class HttpTest(HttpAbstractBaseObject):
    """
    test class

    demo: curl -H "Content-Type: application/json" -X POST "http://127.0.0.1:8000/service/http_api" -d
    '{"method": "TEST", "call_type": "SYNC", "result": "", "message": ""}'
    """
    def handle(self):
        json_dic = {"method": "TEST", "call_type": "SYNC",
                    "result": "SUCCESS", "message": "this is response message, hello world"}
        return json_dic


class HttpService:
    """
    HTTP 服务入口
    """
    def __init__(self, request):
        self._request = request

    # noinspection PyMethodMayBeStatic
    def http_response(self, response_dict):
        """
        http 请求统一响应
        :param response_dict:
        :return:
        """
        return HttpResponse(json.dumps(response_dict), content_type="application/json")

    # noinspection PyMethodMayBeStatic
    def request_save_log(self, http_request_value_dic):
        """
        正常的请求记录DB
        :param http_request_value_dic: 请求数据字典
        :return: null
        """
        http_api_request_log = AISrHttpApiLog()
        http_api_request_log.ip = http_request_value_dic['ip']
        http_api_request_log.auth_data = http_request_value_dic['auth_data']
        http_api_request_log.request_type = http_request_value_dic['http_request_type']
        http_api_request_log.request_data = str(http_request_value_dic['http_request_value'])
        http_api_request_log.save()
        return http_api_request_log.id

    @csrf_exempt    # 解决CSRF cookie not set问题
    def http_service(self):
        """
        web服务统一调用入口,在这里由METHOD分发逻辑函数
        必须的参数：username,secret_key,mothod
        eg: http://127.0.0.1:8000/service/web

        返回json: 下面的4个KEY为必填KEY
            {
                'method': 'src',
                'call_type': 'async',
                'result': 'FAILED | SUCCESS',
                'message': "xxxxxx",
                ......
            }
        :return: httpresponse
        """
        ip = self._request.META.get('HTTP_X_FORWARDED_FOR') or self._request.META['REMOTE_ADDR']
        http_request_type = self._request.method

        if http_request_type.upper() == 'GET':
            http_request_value = self._request.GET
        elif http_request_type.upper() == 'POST':
            http_request_value = self._request.body
        else:
            msg = "未知的请求类型 [ip:{0}]".format(ip)
            logger.error(msg)
            json_dic = {"method": "UNKNOWN", "call_type": "UNKNOWN", "result": "FAILED", "message": msg}
            return self.http_response(json_dic)

        logger.info("GET {0} REQUEST: [{1}]".format(http_request_type, http_request_value))
        if http_request_type.upper() == 'POST':
            try:
                http_request_value = json.loads(http_request_value)
            except Exception as ex:
                msg = "post data json load exception [{0}]".format(ex)
                logger.error(msg)
                json_dic = {"method": "UNKNOWN", "call_type": "UNKNOWN", "result": "FAILED", "message": msg}
                return self.http_response(json_dic)

        http_request_value_dic = {
            "ip": ip,
            "auth_data": "",
            "http_request_type": http_request_type,
            "http_request_value": http_request_value
        }
        try:
            request_id = self.request_save_log(http_request_value_dic)
        except Exception as ex:
            request_id = 0
            msg = "save log exception [{0}]".format(ex)
            logger.error(msg)

        try:
            if http_request_type.upper() == 'POST':
                try:
                    method = http_request_value["method"]
                    call_type = http_request_value["call_type"]
                except Exception as ex:
                    msg = "[{0}]\npost data dict parser exception [{1}]".format(json.dumps(http_request_value), ex)
                    json_dic = {"method": "UNKNOWN", "call_type": "UNKNOWN", "result": "FAILED", "message": msg}
                    return self.http_response(json_dic)

                try:
                    handle_class = HttpFactory(http_request_value_dic).get_handle_class()
                    json_dic = handle_class.handle()
                except Exception as ex:
                    msg = "handle class exception [{0}] [{1}]".format(ex, http_request_value_dic)
                    json_dic = {"method": method, "call_type": call_type, "result": "FAILED", "message": msg}
            else:
                json_dic = {
                    'method': 'UNKKNOWN',
                    'call_type': 'UNKNOWN',
                    'result': 'FAILED',
                    'message': "CALL TYPE {0}, UNSUPPORT".format(http_request_type)
                }

        except Exception as ex:
            msg = "receive http service request, but data handle exception, error: {0}".format(ex)
            logger.error(msg)
            json_dic = {"method": "UNKKNOWN", "call_type": "UNKKNOWN", "result": "FAILED", "message": msg}

        logger.info("RESPONSE REQUEST: [{0}]".format(json_dic))

        try:
            if request_id > 0:
                http_api_request_obj = AISrHttpApiLog.objects.get(id=request_id)
                http_api_request_obj.response_data = json.dumps(json_dic)
                http_api_request_obj.save()
        except Exception as ex:
            msg = "http request find log exception [{0}]".format(ex)
            logger.error(msg)

        return self.http_response(json_dic)


@csrf_exempt
def http_service_factory(request):
    """
    请求入口
    :param request:
    :return:
    """
    hs = HttpService(request)
    return hs.http_service()
