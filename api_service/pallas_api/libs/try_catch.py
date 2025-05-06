# -*-coding:utf-8-*-
import logging
import inspect
from functools import wraps

from rest_framework.response import Response

from api_service.pallas_api.libs.response import success_response

logger = logging.getLogger()


def try_catch(func):
    cls_name = func.__self__.__class__ if hasattr(func, "im_class") else ""
    func_name = cls_name + "." + func.__name__

    @wraps(func)
    def _do_try_catch(*args, **params):
        try:
            return func(*args, **params)
        except Exception as e:
            args_dict = inspect.getcallargs(func, *args)
            for k, v in list(params.items()): args_dict.setdefault(k, v)
            if "self" in args_dict: del args_dict["self"]
            if "request" in args_dict: del args_dict["request"]
            logger.exception("Function_Name:[%s] Input_Params:[%s]" % (func_name, args_dict))
            if hasattr(e, "code") or hasattr(e, "status_code"):
                if hasattr(e, "code"):
                    e.status_code = e.code

                return Response(str(e), e.status_code)
            # raise e
            return_data = {
                'data': [],
                'status': 'fail',
                'message': str(e)
            }
            return success_response(**return_data)
    return _do_try_catch
