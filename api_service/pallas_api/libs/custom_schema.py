from functools import reduce

import coreapi
import coreschema
from django.conf import settings
from rest_framework.schemas import AutoSchema

from api_service.pallas_api import models as pallas_models
from api_service.pallas_api.models import AiSrTask, AiSrProfileManage

common_fields = (coreapi.Field('token', location='query', schema=coreschema.String(description=u'token')),)
doc_host = settings.API_DOCS


class APIViewSchema(AutoSchema):
    def get_link(self, path, method, base_url):
        link = super(APIViewSchema, self).get_link(path, method, base_url)
        get_fields = [
            coreapi.Field('q', location='query', schema=coreschema.String(description=u'filter xxxx')),
        ]
        ai_sr_task_post_fields = [
            coreapi.Field('sql_text', location='form', schema=coreschema.String(description=u'sql_text')),
        ]
        ai_sr_task_valid_put = [
            coreapi.Field('task_id', location='form', required=True,
                          schema=coreschema.String(description=u'task_id', )),
            coreapi.Field('valid', location='form', required=True,
                          schema=coreschema.String(description=u'1为有效； 0为无效', )),
        ]
        login_post = [
            coreapi.Field('username', location='form', required=True,
                          schema=coreschema.String(description=u'username', )),
            coreapi.Field('password', location='form', required=True, type='password',
                          schema=coreschema.String(description=u'*****', )),
        ]
        repo_get = [
            coreapi.Field('repo_url', location='query', schema=coreschema.String(description=u'repo_url: git ...')),
        ]
        review_post = [
            coreapi.Field('db_type', location='form', required=True, schema=coreschema.String(description=u'数据库类型')),
            coreapi.Field('host', location='form', required=True, schema=coreschema.String(description=u'数据库地址')),
            coreapi.Field('port', location='form', required=True, schema=coreschema.String(description=u'数据库端口')),
            coreapi.Field('instance_name', location='form', required=True, schema=coreschema.String(description=u'实例名')),
            coreapi.Field('username', location='form', required=True, schema=coreschema.String(description=u'用户名')),
            coreapi.Field('passpord', location='form', required=True, schema=coreschema.String(description=u'密码')),
            coreapi.Field('schema_name', location='form', required=True, schema=coreschema.String(description=u'schema_name')),
            coreapi.Field('sql_text', location='form', required=True, schema=coreschema.String(description=u'sql语句')),
        ]
        if link.url == f'{doc_host}/pallas/api/v1/ai_sr_task' and method.lower() == 'get':
            fields = common_fields + tuple(AiSrTask().get_fields()) + link.fields
        elif link.url == f'{doc_host}/pallas/api/v1/ai_sr_task' and method.lower() == 'post':
            fields = common_fields + tuple(ai_sr_task_post_fields) + link.fields
        elif link.url == f'{doc_host}/pallas/api/v1/ai_sr_task/valid' and method.lower() == 'put':
            fields = common_fields + tuple(ai_sr_task_valid_put) + link.fields
        elif link.url == f'{doc_host}/pallas/api/v1/login' and method.lower() == 'post':
            fields = tuple(login_post) + link.fields
        elif link.url == f'{doc_host}/pallas/api/v1/repo' and method.lower() == 'get':
            fields = tuple(repo_get) + link.fields
        elif link.url == f'{doc_host}/pallas/api/v1/test_conn' and method.lower() == 'post':
            fields = common_fields + tuple(AiSrProfileManage().get_fields('form')) + link.fields
        elif link.url == f'{doc_host}/pallas/common/review' and method.lower() == 'post':
            fields = tuple(review_post) + link.fields
        else:
            fields = link.fields
        link = coreapi.Link(
            url=link.url,
            action=link.action,
            encoding=link.encoding,
            fields=fields,
            description=link.description)
        coreapi.document.Link()
        return link

    def get_common_view_fields(self, url, method):
        get_fields = [
            coreapi.Field('q', location='query', schema=coreschema.String(description=u'filter xxxx')),
        ]
        return


class CommonModelsSchema(AutoSchema):
    def get_link(self, path, method, base_url):
        link = super(CommonModelsSchema, self).get_link(path, method, base_url)
        model_fields = []
        location = 'form'
        table_name = link.url.split('/')[-1]
        table_model = reduce(lambda x, y: x + y, list(map(lambda x: x.capitalize(), table_name.split('_'))))
        if method.lower() == 'get':
            location = 'query'
        if hasattr(pallas_models, table_model):
            # a = getattr(pallas_models, table_model)
            # print(a)
            # print(a().get_fields())
            model_fields = getattr(pallas_models, table_model)().get_fields(location)
        fields = common_fields + tuple(model_fields) + link.fields
        link = coreapi.Link(
            url=link.url,
            action=link.action,
            encoding=link.encoding,
            fields=fields,
            description=link.description)
        coreapi.document.Link()
        return link


class OpenAPISchema(AutoSchema):
    def get_link(self, path, method, base_url):
        link = super(OpenAPISchema, self).get_link(path, method, base_url)
        check_review = [
            coreapi.Field('app_name', location='query', required=True, schema=coreschema.String(description=u'app_name')),
            coreapi.Field('tag', location='query', required=True, schema=coreschema.String(description=u'tag')),
        ]
        post_fields = [
            coreapi.Field('sql_text', location='form', schema=coreschema.String(description=u'sql_text')),
        ]
        sqlreview_by_git = [
            coreapi.Field('app_name', location='form', required=True,
                          schema=coreschema.String(description=u'app_name', )),
            coreapi.Field('tag', location='form', required=True,
                          schema=coreschema.String(description=u'tag', )),
        ]
        get_table_name_by_sql = [
            coreapi.Field('sql_text', location='form', required=True, schema=coreschema.String(description=u'sql_text'))
        ]
        get_sql_list_by_xml = [
            coreapi.Field('content', location='form', required=True, schema=coreschema.String(description=u'content'))
        ]

        if link.url == f'{doc_host}/pallas/openapi/check_review' and method.lower() == 'get':
            fields = tuple(check_review) + link.fields
        elif link.url == f'{doc_host}/pallas/openapi/sqlreview_by_git' and method.lower() == 'post':
            fields = tuple(sqlreview_by_git) + link.fields
        elif link.url == f'{doc_host}/pallas/openapi/get_table_name_by_sql' and method.lower() == 'post':
            fields = tuple(get_table_name_by_sql) + link.fields
        elif link.url == f'{doc_host}/pallas/openapi/get_sql_list_by_xml' and method.lower() == 'post':
            fields = tuple(get_sql_list_by_xml) + link.fields
        else:
            fields = link.fields
        link = coreapi.Link(
            url=link.url,
            action=link.action,
            encoding=link.encoding,
            fields=fields,
            description=link.description)
        coreapi.document.Link()
        return link