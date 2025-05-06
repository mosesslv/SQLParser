import json
from django.http import HttpResponse
from api_service.pallas_api.libs.self_defined_errors import SELF_DEFINED_ERROR_DICT


def success_response(**kwargs):
    return HttpResponse(json.dumps(kwargs), content_type='application/json')


def fail_response(status_code, message=None, data=None):

    if not message:
        message = SELF_DEFINED_ERROR_DICT.get(status_code, "")
    else:
        message = SELF_DEFINED_ERROR_DICT.get(status_code, "") + '\n' + message

    content = {
        'message': message,
        'data': data
    }
    return HttpResponse(json.dumps(content), content_type='application/json',
                        status=status_code)

