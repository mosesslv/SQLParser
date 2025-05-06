import hashlib
import logging
import os
import uuid

import cx_Oracle
from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from api_service.pallas_api.common.dataBase import get_schema_table_by_mysql
from api_service.pallas_api.libs.custom_schema import OpenAPISchema
from api_service.pallas_api.libs.response import success_response
from api_service.pallas_api.libs.try_catch import try_catch
from api_service.pallas_api.models import AiSrAppRepository, AiSrTask, AiSrReviewRequest, AiSrTaskSql, AiSrReviewDetail, \
    AiSrCheckHistory
from api_service.pallas_api.tasks import async_exec_task_by_git
from api_service.pallas_api.utils import get_tag_list, switch_tag
from api_service.pallas_api.views.common_view import _exec_sql_review
from service.GitSQLReview.xml_sql_parser import XmlSqlParser
from service.sql_parser_graph.SQLParser import SQLParser

logger = logging.getLogger(__name__)


class CheckTagReviewResultView(APIView):
    authentication_classes = []
    schema = OpenAPISchema()

    @try_catch
    def get(self, request, *args, **params):
        """
        检查版本review是否通过
        """
        query_params = request.query_params.dict()
        logger.info("params:%s", query_params)
        app_name = query_params.get('app_name')
        tag = query_params.get('tag')
        try:
            task = AiSrTask.objects.filter(app_name=app_name, current_tag=tag).first()
            if not task:
                raise Exception('no review')
            if task.tag_review_result != 'SUCCESS':
                raise Exception('review未通过')
            logger.info("check task %s", task.as_dict())
            repo_url = AiSrAppRepository.objects.get(app_name=app_name).repo_url
            repo = switch_tag(app_name, repo_url, tag)
            sql_md5_set = list()

            xml_parser = XmlSqlParser(app_path=repo.repo_path)
            xml_info = xml_parser.get_sql_by_app()

            # for file_path in repo.search_files(filter_regex=".xml$"):
            #     xml_parser = XmlSqlParser(file_path=file_path)
            #     xml_info = xml_parser.get_sql_by_file()
            #     print(file_path)
            #     if not xml_info:
            #         continue
            for sql_obj in xml_info:
                sql_md5_set.append(hashlib.md5(sql_obj.sql_text.encode('utf-8')).hexdigest())
            task_id = AiSrTask.objects.filter(app_name=app_name, current_tag=tag).order_by('-id').first().task_id
            review_request_id = AiSrReviewRequest.objects.get(task_id=task_id).id
            md5_values = AiSrReviewDetail.objects.filter(review_request_id=review_request_id).values_list('sql_md5')
            review_sql_md5_list = list(map(lambda x: x[0], md5_values))
            if set(sql_md5_set) ^ set(review_sql_md5_list) and len(sql_md5_set) != len(review_sql_md5_list):
                raise Exception('no review')
            AiSrCheckHistory().create(**{'msg': '通过', 'result': 'True', 'app_name': app_name, 'tag': tag})
            return Response({'msg': '通过', 'result': 'True', 'status': 'SUCCESS'}, 200)
        except Exception as e:
            AiSrCheckHistory().create(**{'msg': str(e), 'result': 'FAIL', 'app_name': app_name, 'tag': tag})
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
            return Response({'msg': str(e), 'result': 'False', 'status': 'FAIL'}, 200)


class GitSqlReviewAPIView(APIView):
    authentication_classes = []
    schema = OpenAPISchema()

    def post(self, request, *args, **params):
        """
        git 发起批量任务
        :return:
        """
        try:
            logger.info("params:%s", request.data)
            username = "pallas"
            tenant = "lufax"
            tag = request.data.get('tag')
            app_name = request.data.get('app')
            valid = request.data.get('valid', '1')
            _exec_sql_review(username, tenant, app_name, tag, valid)
            return success_response(**{'result': 'True', 'msg': 'SUCCESS'})
        except Exception as e:
            logger.exception('pallas api %s error, %s' % (self.__class__.__name__, e))
            return Response({'msg': str(e), 'result': 'False'}, 500)


class GetTableNameBySQLView(APIView):
    authentication_classes = []
    schema = OpenAPISchema()

    @try_catch
    def post(self, request, *args, **params):
        """
        根据SQL语句解析表名
        :return: {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        """
        # query_params = request.query_params.dict()
        logger.info("params:%s", request.data)
        sql_text = request.data.get('sql_text', '')
        tables = SQLParser(sql_text).get_table_name()
        return_data = {
            'data': tables,
            'status': 'success',
            'message': 'OK'
        }
        return success_response(**return_data)


class GetSqlListByXMLView(APIView):
    authentication_classes = []
    schema = OpenAPISchema()

    @try_catch
    def post(self, request, *args, **params):
        """
        根据xml解析sql
        :return: {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        """
        # logger.info("request.data:%s", request.data)
        content = request.data.get('content', '')
        return_data = {
            'data': [],
            'status': 'success',
            'message': 'OK'
        }
        sql_list = []
        if not content:
            return_data['status'], return_data['message'] = 'fail', 'not content'
            return success_response(**return_data)
        content_md5 = hashlib.md5(content.encode('utf-8')).hexdigest()
        file_path = '/tmp/'+content_md5+'.xml'
        # print(file_path)
        with open(file_path, 'w') as f:
            f.write(content)
        xml_parser = XmlSqlParser(file_path=file_path)
        xml_info = xml_parser.get_sql_by_file()
        for sql_obj in xml_info:
            sql_list.append(sql_obj.sql_text)
        if os.path.exists(file_path):
            os.remove(file_path)
        return_data['data'] = sql_list
        return_data['count'] = len(sql_list)
        return success_response(**return_data)
