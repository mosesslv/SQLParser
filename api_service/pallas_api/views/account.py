import logging

from django.conf import settings
from rest_framework.views import APIView
from django.contrib.auth.hashers import make_password, check_password
from rest_framework.authtoken.models import Token

from api_service.pallas_api.libs import custom_schema
from api_service.pallas_api.libs.do_request import post
from api_service.pallas_api.libs.licensing.methods import Helpers
from api_service.pallas_api.libs.licensing.models import LicenseKey
from api_service.pallas_api.libs.response import success_response
from api_service.pallas_api.libs.try_catch import try_catch
from api_service.pallas_api.models import AiSrUser
from api_service.pallas_api.views.views import check_query_data

from django.contrib import auth

logger = logging.getLogger(__name__)


def verify_license():
    license_key = settings.LICENSE_KEY
    license_key = LicenseKey.load_from_string(license_key)
    print(type(license_key))
    print(license_key)
    if license_key is None or not Helpers.IsOnRightMachine(license_key):
        print("NOTE: This license file does not belong to this machine.")
        return False, "NOTE: This license file does not belong to this machine."
    else:
        print("Success")
        # print("License key: " + str(res[1].app_id))
    return True, "Success"


class LoginView(APIView):
    authentication_classes = []
    schema = custom_schema.APIViewSchema()

    def post(self, request, *args, **params):
        """
        登录接口
        """
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        try:
            username = request.data.get('username')
            password = request.data.get('password')
            user = AiSrUser.objects.filter(username=username).first()
            if not user:
                return success_response(**{'status': 'fail', 'message': '用户不存在'})
            if not check_password(password, user.password, 'pbkdf2_sha256'):
                return success_response(**{'status': 'fail', 'message': '用户名和密码不匹配'})
            # 校验通过
            # 删除原有的Token
            # user = auth.authenticate(request=request, username=username, password=password)
            # print(user)
            old_token = Token.objects.filter(user=user)
            old_token.delete()
            # 创建新的Token
            token = Token.objects.create(user=user)
            return_data['data'] = user.as_dict()
            return_data['token'] = token.key
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class AiSrUserView(APIView):
    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            if request.user.role != "admin":
                query_params['username'] = request.user.username
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params)
            detail = AiSrUser.objects
            if query_data:
                print(query_data)
                detail = detail.filter(**query_data)
            detail = detail.order_by('-created_at')[offset:offset + limit]
            return_data['data'] = [cr.as_dict() for cr in detail]
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)

    @try_catch
    def post(self, request, *args, **params):
        # request_data = request.POST.dict()
        request_data = request.data
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        if request.user.role != "admin":
            raise Exception('非管理员用户不可以添加')
        if AiSrUser.objects.filter(username=request_data['username']).exists():
            raise Exception('用户名已存在')
        request_data['created_by'] = request.user.username
        request_data['updated_by'] = request.user.username
        request_data['password'] = make_password(request_data['password'], None, 'pbkdf2_sha256')
        ret = AiSrUser().create(**request_data)
        return_data['data'] = ret.as_dict()
        # except Exception as e:
        #     return_data['status'] = 'fail'
        #     return_data['message'] = str(e)
        #     logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)

    def put(self, request, *args, **params):
        # request_data = request.POST.dict()
        request_data = request.data
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        try:
            request_data['updated_by'] = request.user.username
            if request_data.get('password'):
                request_data['password'] = make_password(request_data['password'], None, 'pbkdf2_sha256')
            obj = AiSrUser.objects.get(username=request_data['username'])
            ret = obj.update(**request_data)
            return_data['data'] = ret.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)

    def delete(self, request, *args, **params):
        # request_data = request.POST.dict()
        request_data = request.data
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        try:
            username = request_data.get('username')
            obj = AiSrUser.objects.filter(username=username)[0]
            obj.delete_mark = 0
            obj.updated_by = request.user.username
            obj.save()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class UserInfoView(APIView):
    @try_catch
    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        username = request.user.username
        user = AiSrUser.objects.filter(username=username).first()
        if not user:
            user = AiSrUser().create(**request.user.as_dict())
        return_data = {
            'data': user.as_dict(),
            'status': 'success',
            'message': 'OK'
        }
        return success_response(**return_data)

