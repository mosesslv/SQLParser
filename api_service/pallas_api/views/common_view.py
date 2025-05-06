import logging
import uuid
from datetime import datetime

import cx_Oracle
from django.db.models import Count
from rest_framework.response import Response
from rest_framework.views import APIView

from api_service.pallas_api.common.dataBase import get_schema_table_by_mysql, get_schema_table_by_oracle
from api_service.pallas_api.libs import custom_schema
from api_service.pallas_api.libs.response import success_response
from api_service.pallas_api.libs.try_catch import try_catch
from api_service.pallas_api.models import AiSrAppRepository, AiSrTask, AiSrReviewRequest, AiSrTaskSql
from api_service.pallas_api.tasks import async_exec_task_by_git
from api_service.pallas_api.utils import get_tag_list
from service.AISQLReview.AIError import get_error_type_description
from service.AISQLReview.service_enter import ServiceEnterV2

logger = logging.getLogger(__name__)


class TestConnectivityView(APIView):
    schema = custom_schema.APIViewSchema()

    @try_catch
    def post(self, request, *args, **params):
        """
        数据库连通性测试
        """
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        logger.info("request.data:%s", request.data)
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }

        db_type = request.data.get('db_type')
        if db_type.lower() == 'mysql':
            return_data['data'] = get_schema_table_by_mysql(request.data)
        else:
            return_data['data'] = get_schema_table_by_oracle(request.data)
        return success_response(**return_data)


class RepoView(APIView):
    authentication_classes = []
    schema = custom_schema.APIViewSchema()

    @try_catch
    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)

        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        try:
            git_info = AiSrAppRepository.objects.filter(repo_url=query_params.get('repo_url')).first()
            if git_info:
                tag_list = get_tag_list(app_name=git_info.app_name, repo_url=git_info.repo_url)[-400:]
                return_data['data'] = list(map(lambda _d : _d.split('refs/tags/')[1], filter(lambda _d: 'refs/tags/' in _d, tag_list)))
            else:
                return_data['data'] = []
        except Exception as e:
            return_data['status'],  return_data['message'] = 'fail', str(e)
            return success_response(**return_data)
        return success_response(**return_data)


def _exec_sql_review(username, tenant, app_name, tag, valid):
    """
    _exec_sql_review
    :return:
    """
    task_id = uuid.uuid4().hex.replace('-', '')
    repo_url = AiSrAppRepository.objects.filter(app_name=app_name).first().repo_url
    task_data = {
        'userid': username,
        'created_by': username,
        'updated_by': username,
        'tenant': tenant,
        'task_status': 'INIT',
        'task_type': 'GITREPOSITORY',
        'sql_total': '0',
        'sql_need_optimize': '0',
        'task_id': task_id,
        'app_name': app_name,
        'current_tag': tag,
        'tag_review_result': 'INIT',
        'valid': valid,
    }

    review_request_data = {
        'created_by': username,
        'updated_by': username,
        'task_id': task_id,
        'app_name': app_name,
        'repo_path': '/wls/source/%s' % app_name,
        'review_type': 'ALL',  # ALL全量, INC:增量',
        'review_status': 'INIT',
        'newer_version': tag,
        'sql_cnt': '0',
    }
    AiSrReviewRequest().create(**review_request_data)
    task_ret = AiSrTask().create(**task_data)
    logging.info('async_exec_task_by_git params:\n %s', (app_name, repo_url, task_id, tag, username, tenant))
    res = async_exec_task_by_git.apply_async((app_name, repo_url, str(task_id), tag, username, tenant))
    return res


class GitSqlReviewView(APIView):
    @try_catch
    def post(self, request, *args, **params):
        """

        :param request: repo_url tag
        :param args:
        :param params:
        :return:
        """
        logger.info("params:%s", request.data)
        username = request.user.username
        tenant = request.user.tenant
        # tenant = 'lufax'
        return_data = {
            'data': '',
            'status': 'success',
            'message': 'OK'
        }
        repo_url = request.data.get('git')
        tag = request.data.get('tag')
        app_name = request.data.get('app')
        valid = request.data.get('valid')
        _exec_sql_review(username, tenant, app_name, tag, valid)
        return success_response(**return_data)


class ExceptionCountView(APIView):
    authentication_classes = []

    @try_catch
    def get(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)



        task_id = query_params.get('task_id')

        ai_task = AiSrTaskSql.objects.filter(task_id=task_id)
        # AiSrTaskSql.objects.filter(task_id='8def7b8dfa35439eafffcfe48f8fda84', ai_result='NOPASS').values(
        #     'ai_program_type').annotate(Count('ai_program_type'))
        ai_program_type = ai_task.filter(ai_result='NOPASS').values('ai_program_type').annotate(
            Count('ai_program_type'))
        ai_error_code = ai_task.filter(ai_result='INVALID').values('ai_error_code').annotate(
            Count('ai_error_code')).order_by('-ai_error_code__count')

        nopass = [{'desc': item['ai_program_type'], 'count': item['ai_program_type__count']} for item in ai_program_type]
        invalid = []
        for item in ai_error_code:
            type, desc = get_error_type_description(item['ai_error_code'])
            invalid.append({
                'desc': desc, 'count': item['ai_error_code__count'], 'type': type
            })
        other = sum(dict(map(lambda _d: list(_d.values()), ai_error_code[4:])).values())
        invalid.append({'desc': '其他', 'count': other, 'type': 'other'})
        return_data = {
            'data': {
                'nopass': nopass,
                'invalid': invalid,
            },
            'status': 'success',
            'message': 'OK'
        }
        return success_response(**return_data)


class TagReviewView(APIView):
    # authentication_classes = []
    @try_catch
    def put(self, request, *args, **params):
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        logger.info("request.data:%s", request.data)
        id = request.data.get('id')
        tag_review_result = request.data.get('tag_review_result')
        userid = request.user.username
        task = AiSrTask.objects.get(id=id)
        if tag_review_result == 'SUCCESS':
            # AiSrTaskSql.objects.filter(task_id=task.task_id, ai_result__in=['NOPASS', 'INVALID']).update(review_status='1')
            AiSrTaskSql.objects.filter(task_id=task.task_id, review_status='0').update(review_status='1', updated_by=userid, updated_at=datetime.now())
            task.sql_need_optimize = '0'
        task.tag_review_result = tag_review_result
        task.updated_by = userid
        task.save()
        return success_response(**{'data': task.as_dict(), 'status': 'success', 'message': 'OK'})


class SQLReviewView(APIView):
    authentication_classes = []
    schema = custom_schema.APIViewSchema()

    @try_catch
    def post(self, request, *args, **params):
        logger.info("params:%s", request.data)
        request_data = request.data
        db_type = request.data.get('db_type')
        host = request.data.get('host')
        port = request.data.get('port')
        instance_name = request.data.get('instance_name')
        username = request.data.get('username')
        passpord = request.data.get('passpord')
        schema_name = request.data.get('schema_name')
        sql_text = request.data.get('sql_text')
        service_enter = ServiceEnterV2(db_type, host, port, instance_name, username, passpord)
        result_dict = service_enter.predict_sqltext(schema_name, sql_text)
        return success_response(**{'data': result_dict, 'status': 'success', 'message': 'OK'})

