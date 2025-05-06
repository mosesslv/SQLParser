import datetime
import logging

import requests
from django.conf import settings
from django.utils.translation import ugettext_lazy
from django.core.cache import cache
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
from rest_framework.authtoken.models import Token
from rest_framework import HTTP_HEADER_ENCODING

from api_service.pallas_api.libs.licensing.verify_licens import verify_license
from api_service.pallas_api.libs.response import fail_response
from api_service.pallas_api.utils import Dict2Obj

logger = logging.getLogger(__name__)


# 获取请求头信息
def get_authorization_header(request):
    auth = request.META.get('HTTP_AUTHORIZATION', b'')
    if not auth:
        auth = request.GET.get('token', b'')
    if isinstance(auth, type('')):
        auth = auth.encode(HTTP_HEADER_ENCODING)
    return auth


# 自定义认证方式，这个是后面要添加到设置文件的
class ExpiringTokenAuthentication(BaseAuthentication):
    model = Token

    def authenticate(self, request):
        try:
            method = request.META.get('REQUEST_METHOD')
            if method != 'GET':
                verify_license()
        except Exception as e:
            raise exceptions.AuthenticationFailed(str(e))

        auth = get_authorization_header(request)
        if not auth:
            # return None
            raise exceptions.AuthenticationFailed("认证失败")
        try:
            token = auth.decode('utf-8')
        except UnicodeError:
            msg = ugettext_lazy("无效的Token， Token头不应包含无效字符")
            raise exceptions.AuthenticationFailed(msg)

        token_cache = 'token_' + token
        cache_user = cache.get(token_cache)
        if cache_user:
            return cache_user, token

        if request.META.get('HTTP_PASSPORT') == '1' or request.GET.get('passport') == '1':
            cache_user, token = self.sso_login(token)
        else:
            cache_user, token = self.authenticate_credentials(token)
        if token:
            token_cache = 'token_' + token
            cache.set(token_cache, cache_user, 60 * 60 * 12)
        return cache_user, token

    def authenticate_credentials(self, key):
        # 尝试从缓存获取用户信息（设置中配置了缓存的可以添加，不加也不影响正常功能）
        token_cache = 'token_' + key
        """
        cache_user = cache.get(token_cache)
        if cache_user:
            # 在rest framework内部会将整个两个字段赋值给request，以供后续操作使用
            return cache_user, cache_user  # 这里需要返回一个列表或元组，原因不详
        """
        # 缓存获取到此为止

        # 下面开始获取请求信息进行验证
        try:
            print(key)
            token = self.model.objects.get(key=key)
        except self.model.DoesNotExist:
            print('xxxxxxx')
            raise exceptions.AuthenticationFailed("认证失败")
        """
        if not token.user.is_active:
            raise exceptions.AuthenticationFailed("用户被禁用")
        """
        # Token有效期时间判断（注意时间时区问题）
        # 我在设置里面设置了时区 USE_TZ = False，如果使用utc这里需要改变。
        if (datetime.datetime.now() - token.created) > datetime.timedelta(hours=0.1 * 5):
            raise exceptions.AuthenticationFailed({"message": "认证信息已过期"})
            # return fail_response(401, data={"message": "认证信息已过期"})

        token.created = datetime.datetime.now()
        token.save()

        """
        # 加入缓存增加查询速度，下面和上面是配套的，上面没有从缓存中读取，这里就不用保存到缓存中了
        if token:
            token_cache = 'token_' + key
            cache.set(token_cache, token.user, 600)
        """
        # 返回用户信息
        # 在rest framework内部会将整个两个字段赋值给request，以供后续操作使用
        return token.user, key

    def authenticate_header(self, request):
        return 'Token'

    def pallas_login(self, key):
        # 尝试从缓存获取用户信息（设置中配置了缓存的可以添加，不加也不影响正常功能）
        token_cache = 'token_' + key
        """
        cache_user = cache.get(token_cache)
        if cache_user:
            # 在rest framework内部会将整个两个字段赋值给request，以供后续操作使用
            return cache_user, cache_user  # 这里需要返回一个列表或元组，原因不详
        """
        # 缓存获取到此为止

        # 下面开始获取请求信息进行验证
        try:
            token = self.model.objects.get(key=key)
        except self.model.DoesNotExist:
            raise exceptions.AuthenticationFailed("认证失败")
        """
        if not token.user.is_active:
            raise exceptions.AuthenticationFailed("用户被禁用")
        """
        # Token有效期时间判断（注意时间时区问题）
        # 我在设置里面设置了时区 USE_TZ = False，如果使用utc这里需要改变。
        if (datetime.datetime.now() - token.created) > datetime.timedelta(hours=0.1 * 4):
            raise exceptions.AuthenticationFailed({"message": "认证信息已过期"})
            # return fail_response(401, data={"message": "认证信息已过期"})

        token.created = datetime.datetime.now()
        token.save()

        """
        # 加入缓存增加查询速度，下面和上面是配套的，上面没有从缓存中读取，这里就不用保存到缓存中了
        if token:
            token_cache = 'token_' + key
            cache.set(token_cache, token.user, 600)
        """
        # 返回用户信息
        # 在rest framework内部会将整个两个字段赋值给request，以供后续操作使用
        return token.user, token

    def sso_login(self, key):
        params = {"token": key,
                  "secret": settings.PALLAS['SSO_APP_SECRET'],
                  "app": settings.PALLAS['SSO_APP_ID']}
        try:
            user_info = requests.post("%s/account/user_info.json" % settings.PALLAS['SSO_URL'], params).json()
        except Exception as e:
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
            raise exceptions.AuthenticationFailed("认证失败")
        if user_info['code'] == 200:
            print(user_info['data'])
            if 'user.user' in user_info["data"]['permissions']:
                data = user_info.get("data", {})
                user = {
                    'username': data.get('um', ''),
                    'tenant': data.get('tenant_name', ''),
                    'username_chn': data.get('name', ''),
                    'role': 'admin' if "admin.admin" in data.get('permissions', '') else 'guest',
                }

                print()
                return Dict2Obj(user), key
            else:
                raise exceptions.AuthenticationFailed({"message": "没有登录权限"})
        elif user_info['code'] == 404:
            raise exceptions.AuthenticationFailed({"message": "token失效"})
        else:
            res = {'status': 'fail', 'message': 'passport error'}
            raise exceptions.AuthenticationFailed({"message": "passport error"})


if __name__ == '__main__':
    try:
        verify_license()
    except Exception as e:
        raise Exception({"message": "签发证书已过期"})

