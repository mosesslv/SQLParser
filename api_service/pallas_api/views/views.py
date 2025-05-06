# Create your views here.
import copy
import logging
import uuid
from datetime import datetime
from functools import reduce

import coreapi
import coreschema
from django.db.models import Count, Q
from django.views.generic.base import View
# from django_filters import rest_framework
# from rest_framework import filters, pagination
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet
from rest_framework_extensions.cache.decorators import cache_response

from api_service.pallas_api import models as pallas_models
from api_service.pallas_api.common.dataBase import get_schema_table_by_oracle, get_schema_table_by_mysql
from api_service.pallas_api.libs import custom_schema
from api_service.pallas_api.libs.custom_schema import CommonModelsSchema
from api_service.pallas_api.libs.response import success_response
from api_service.pallas_api.libs.try_catch import try_catch
from api_service.pallas_api.models import AiSrSqlDetail, AiSrTask, AiSrTaskSql, AiSrProfileManage, \
    AiSrTenantSchema, AiSrSchemaTable, AiSrAppRepository
# from django.views.generic import View
# from api_service.pallas_api.serializers import AiSrTaskSerializer
from api_service.pallas_api.utils import get_schema_by_sql, get_exception_count
from service.AISQLReview.service_enter import ServiceEnter
from service.GitSQLReview.sqlreview_handle import SingleSQLHandleAndWrite

logger = logging.getLogger(__name__)


def check_query_data(data, request=None):
    data_copy = copy.deepcopy(data)
    for k, v in data.items():
        if k in ['limit', 'offset', 'token', 'passport', 'search'] or not data_copy.get(k):
            data_copy.pop(k)
        if v == 'None':
            data_copy[k] = None
    # if request:
    #     if request.user.role != 'admin':
    #         data_copy['tenant'] = request.user.tenant
    return data_copy


class ModelCommonQueryView(APIView):
    schema = CommonModelsSchema()

    def get(self, request, *args, **params):
        """
        通用查询接口，需要在models中建立映射
        \n支持django filter 语句
        """
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            table_name = params.get('table_name')
            table_model = reduce(lambda x, y: x+y, list(map(lambda x: x.capitalize(), table_name.split('_'))))
            if hasattr(pallas_models, table_model):
                return_data['data'] = table_model
                limit = int(query_params.get('limit', 15))
                offset = int(query_params.get('offset', 0))
                query_data = check_query_data(query_params, request)
                detail = getattr(pallas_models, table_model).objects
                if query_data:
                    print(query_data)
                    detail = detail.filter(**query_data)
                count = detail.count()
                detail = detail.order_by('-created_at')[offset:offset + limit]
                return_data['count'] = count
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
        table_name = params.get('table_name')
        table_model = reduce(lambda x, y: x + y, list(map(lambda x: x.capitalize(), table_name.split('_'))))
        view = table_model + 'View'
        if hasattr(pallas_models, table_model):
            pass

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
            table_name = params.get('table_name')
            table_model = reduce(lambda x, y: x + y, list(map(lambda x: x.capitalize(), table_name.split('_'))))
            if hasattr(pallas_models, table_model):
                return_data['data'] = table_model
                detail = getattr(pallas_models, table_model).objects
                obj = detail.get(id=request_data.get('id'))
                ret = obj.update(**request_data)
                return_data['data'] = ret.as_dict()
            request_data['updated_by'] = request.user.username
            obj = AiSrProfileManage.objects.get(id=request_data['id'])
            ret = obj.update(**request_data)
            return_data['data'] = ret.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class AiSrSqlDetailView(APIView):
    schema = custom_schema.APIViewSchema()
    def get(self, request, *args, **params):
        # print(dir(request))
        # print(request.GET.dict())
        # print(request.POST.dict())
        # return ""
        # query_params = request.query_params.dict()
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params, request)
            detail = AiSrSqlDetail.objects
            if query_data:
                print(query_data)
                detail = detail.filter(**query_data)
            detail = detail.order_by('-created_at')[offset:offset+limit]
            return_data['data'] = [cr.as_dict() for cr in detail]
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class AiSrTaskView(APIView):
    schema = custom_schema.APIViewSchema()

    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            search = query_params.get('search', '').strip()
            query_data = check_query_data(query_params)
            detail = AiSrTask.objects
            if query_data:
                print(query_data)
                detail = detail.filter(**query_data)
            if search:
                detail = detail.filter(Q(app_name__contains=search) | Q(current_tag__contains=search))
            count = detail.count()
            detail = detail.order_by('-created_at')[offset:offset+limit]
            return_data['count'] = count
            return_data['data'] = [cr.as_dict() for cr in detail]
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)

    def post(self, request, *args, **params):
        """
        单条任务发起接口
        """
        # request_data = request.POST.dict()
        request_data = request.data
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': {},
            'status': 'success',
            'message': 'OK'
        }
        try:
            _uuid = uuid.uuid4().hex.replace('-', '')
            schema_info = get_schema_by_sql(request_data.get('sql_text'),
                                            **{'tenant': request.user.tenant})
            # data = {
            #     "method": "SQLPREDICT",
            #     "call_type": "SYNC",
            #     "result": "",
            #     "message": "",
            #     "userid": request.user.username,
            #     "tenant": request.user.tenant.upper(),
            #     "profile_name": schema_info.profile_name,
            #     "schema": schema_info.schema_name,
            #     "sql_text": request_data.get('sql_text'),
            #     "sequence": _uuid
            # }
            # print(data)

            request_data.update({
                'userid': request.user.username,
                'created_by': request.user.username,
                'updated_by': request.user.username,
                'tenant': request.user.tenant,
                'task_status': 'SUCCESS',
                'sql_total': '1',
                'task_id': uuid.uuid4().hex.replace('-', '')
            })
            task_ret = AiSrTask().create(**request_data)
            request_data.update({
                'task_id': task_ret.task_id,
                'sql_sequence': _uuid,
                'sql_text': request_data.get('sql_text')
            })
            task_sql_ret = AiSrTaskSql().create(**request_data)
            sql_handle_and_write = SingleSQLHandleAndWrite(
                tenant=request.user.tenant,
                userid=request.user.username,
                profile_name=schema_info.profile_name,
                sql_text=request_data.get('sql_text'),
                sql_sequence=_uuid,
                db_type=schema_info.db_type,
                schema_name=schema_info.schema_name)
            result = sql_handle_and_write.sql_handle_and_write()
            task_ret.sql_need_optimize = 0 if result == 'PASS' else 1
            task_ret.save()
            return_data['data'] = task_ret.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


# class MyFormatResultsSetPagination(pagination.PageNumberPagination):
#
#     page_size_query_param = "page_size"
#     page_query_param = 'page'
#     page_size = 10
#     max_page_size = 1000
#
#     """
#     自定义分页方法
#     """
#     def get_paginated_response(self, data):
#         """
#         设置返回内容格式
#         """
#         return Response({
#             'results': data,
#             'pagination': self.page.paginator.count,
#             'page_size': self.page.paginator.per_page,
#             'page': self.page.start_index() // self.page.paginator.per_page + 1
#         })
#
#
# class ServerViewSet(ModelViewSet):
#     queryset = AiSrTask.objects.all()
#     serializer_class = AiSrTaskSerializer
#     pagination_class = MyFormatResultsSetPagination
#     filter_backends = (rest_framework.DjangoFilterBackend, filters.SearchFilter, filters.OrderingFilter,)
#     # filter_class = ServerFilter
#     search_fields = ('app_name', 'current_tag', '@comment')
#     # ordering_fields = ('cpus', 'ram', 'disk', 'product_date',)
#     ordering = ('-created_at',)
#     # pass


class AiSrTaskSqlView(APIView):
    schema = CommonModelsSchema()

    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params)
            detail = AiSrTaskSql.objects
            if query_data:
                print(query_data)
                detail = detail.filter(**query_data)
            count = detail.count()
            if query_data.get('task_id'):
                task_id = query_data.get('task_id')
                count_info = AiSrTaskSql.objects.filter(task_id=task_id)
                return_data['pass'] = count_info.filter(ai_result='PASS').count()
                return_data['nopass'] = count_info.filter(ai_result='NOPASS').count()
                return_data['invalid'] = count_info.filter(ai_result='INVALID').count()
                return_data['review_count'] = detail.filter(review_status=None).count()
                return_data['pallas_count'] = count_info.filter(review_status='0', updated_by='pallas').count()
                # group_ret = count_info.values('ai_error_type').annotate(Count('ai_error_type'))
                nopass_count, exception = get_exception_count(task_id)
                return_data['nopass_count'] = nopass_count
                return_data['exception'] = exception
            detail = detail.order_by('-created_at')[offset:offset + limit]
            return_data['count'] = count
            return_data['data'] = [cr.as_dict() for cr in detail]
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
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
            userid = request.user.username
            obj = AiSrTaskSql.objects.get(id=request_data['id'])
            ret = obj.update(**request_data)
            if request_data.get('review_status'):
                sql_list = AiSrTaskSql.objects.filter(task_id=obj.task_id)
                # sql_need_optimize = sql_list.exclude(ai_result='PASS').exclude(review_status='1').count()
                sql_need_optimize = sql_list.filter(review_status='0').count()
                u_task = {'sql_need_optimize': sql_need_optimize, 'tag_review_result': 'REVIEWING', 'updated_by': userid, 'updated_at': datetime.now()}
                # if not sql_list.exclude(ai_result='PASS').filter(review_status=None):
                #     if sql_list.filter(review_status='0').count() > 0:
                #         u_task['tag_review_result'] = 'FAIL'
                if sql_need_optimize == 0:
                    u_task['tag_review_result'] = 'SUCCESS'
                AiSrTask.objects.get(task_id=obj.task_id).update(**u_task)
            return_data['data'] = ret.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class AiSrTaskValidView(APIView):
    schema = custom_schema.APIViewSchema()

    def put(self, request, *args, **params):
        """
        标记为是否有效
        """
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
            task_id = request_data.get('task_id')
            valid = request_data.get('valid')
            obj = AiSrTask.objects.get(task_id=task_id)
            obj.valid = valid
            obj.save()
            AiSrTaskSql.objects.filter(task_id=task_id).update(valid=valid)
            return_data['data'] = obj.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class AiSrProfileManageView(APIView):
    schema = custom_schema.APIViewSchema()

    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params, request)
            detail = AiSrProfileManage.objects
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

    def post(self, request, *args, **params):
        # request_data = request.POST.dict()
        request_data = request.data
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        try:
            request_data['tenant'] = request.user.tenant
            request_data['created_by'] = request.user.username
            request_data['updated_by'] = request.user.username
            request_data['userid'] = request.user.username
            check = AiSrProfileManage.objects.filter(
                profile_name=request.data.get('profile_name'),
                db_type=request.data.get('db_type'),
                host=request.data.get('host'),
                port=request.data.get('port'),
                instance_name=request.data.get('instance_name'),
                passwd=request.data.get('passwd'),
            ).first()
            if check:
                raise Exception('配置信息已存在')
            ret = AiSrProfileManage().create(**request_data)
            if request.data.get('db_type', '').lower() == 'mysql':
                schema_info = get_schema_table_by_mysql(request_data)
            else:
                schema_info = get_schema_table_by_oracle(request_data)

            for schema_name, tables in schema_info.items():
                data = {
                    "created_by": request.user.username,
                    "updated_by": request.user.username,
                    "userid": request.user.username,
                    "tenant": request.user.tenant,
                    "profile_name": request_data.get('profile_name'),
                    "db_type": request_data.get('db_type'),
                    "schema_name": schema_name,
                }
                schema_obj = AiSrTenantSchema.objects.create(**data)
                list_to_insert = list()
                # try:
                for table_name in tables:
                    data = {
                        "created_by": request.user.username,
                        "updated_by": request.user.username,
                        "userid": request.user.username,
                        "tenant": request.user.tenant,
                        "schema_id": schema_obj.id,
                        "table_name": table_name
                    }
                    list_to_insert.append(AiSrSchemaTable(**data))
                AiSrSchemaTable.objects.bulk_create(list_to_insert)
                # except Exception:
                #     print(len(schema_info[schema_name]))
                #     print(schema_name)
                #     pset(schema_info[schema_name])
                #     raise Exception('xxx')

            return_data['data'] = ret.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
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
            obj = AiSrProfileManage.objects.get(id=request_data['id'])
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
            id = request_data.get('id')
            obj = AiSrProfileManage.objects.filter(id=id)[0]
            obj.delete_mark = 0
            obj.updated_by = request.user.username
            obj.save()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class AiSrTenantSchemaView(APIView):
    schema = custom_schema.APIViewSchema()

    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params, request)
            detail = AiSrTenantSchema.objects
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


class AiSrSchemaTableView(APIView):
    schema = custom_schema.APIViewSchema()

    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params, request)
            detail = AiSrSchemaTable.objects
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


class AiSrAppRepositoryView(APIView):
    schema = custom_schema.APIViewSchema()

    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            limit = int(query_params.get('limit', 15))
            offset = int(query_params.get('offset', 0))
            query_data = check_query_data(query_params)
            detail = AiSrProfileManage.objects
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

    def post(self, request, *args, **params):
        # request_data = request.POST.dict()
        request_data = request.data
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        try:
            request_data['tenant'] = request.user.tenant
            request_data['created_by'] = request.user.username
            request_data['updated_by'] = request.user.username
            ret = AiSrAppRepository().create(**request_data)
            return_data['data'] = ret.as_dict()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
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
            obj = AiSrAppRepository.objects.get(id=request_data['id'])
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
            id = request_data.get('id')
            obj = AiSrAppRepository.objects.filter(id=id)[0]
            obj.delete_mark = 0
            obj.updated_by = request.user.username
            obj.save()
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        return success_response(**return_data)


class PallasIndexDataView(APIView):
    authentication_classes = []
    schema = custom_schema.APIViewSchema()

    @cache_response(timeout=60*60*12, cache="default")
    def get(self, request, *args, **params):
        """
        获取首页统计数据
        """
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        try:
            task_total = AiSrTask.objects.count()
            review_total = AiSrTaskSql.objects.count()
            fm_db = AiSrTaskSql.objects.filter(valid='1').exclude(schema_name='').exclude(schema_name=None).values('schema_name').annotate(Count('schema_name')).order_by('-schema_name__count')[:5]
            PASS = AiSrTaskSql.objects.filter(ai_result='PASS').count()
            NOPASS = AiSrTaskSql.objects.filter(ai_result='NOPASS').count()
            # group_ret = AiSrTaskSql.objects.values('ai_result').annotate(Count('ai_result'))
            db_count = AiSrProfileManage.objects.count()
            return_data.update({
                'data': {
                    'task_total': task_total,
                    'review_total': review_total,
                    'fm_db': dict(map(lambda _d: list(_d.values()), fm_db)),
                    'review_count': {'PASS': PASS, 'NOPASS': NOPASS},
                    'db_count': db_count,
                }
            })
        except Exception as e:
            return_data['status'] = 'fail'
            return_data['message'] = str(e)
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
        # return success_response(**return_data)
        return Response(return_data, 200)
