import hashlib
import time
import uuid
from datetime import datetime

import cx_Oracle
import logging

from django.db.models import Count

from api_service.pallas_api.common.dataBase import get_schema_table_by_oracle, get_schema_table_by_mysql
from api_service.pallas_api.models import AiSrSchemaTable, AiSrTenantSchema, AiSrTaskSql, AiSrTask, AiSrReviewRequest, \
    AiSrAppSqlmap, AiSrReviewDetail, AiSrProfileManage, AiSrAppRepository
from service.AISQLReview.AIError import get_error_type_description
from service.GitSQLReview.GitRepository import GitRepository
from service.GitSQLReview.sqlreview_handle import SQLReviewStruct, SQLReviewHandle, SQLHandleAndWrite
from service.sql_parser_graph.SQLParser import SQLParser
from service.GitSQLReview.xml_sql_parser import XmlSqlParser

logger = logging.getLogger(__name__)


class Dict2Obj(object):
    """将一个字典转换为类"""

    def __init__(self, dictionary):
        """Constructor"""
        for key in dictionary:
            setattr(self, key, dictionary[key])

    def __repr__(self):
        """print 或str 时，让实例对象以字符串格式输出"""
        return "<Dict2Obj: %s>" % self.__dict__

    def as_dict(self):
        return self.__dict__


def get_schema_by_sql(sql_text, **kwargs):
    """

    :param sql_text:
    :param kwargs: {"userid": "", "tenant": ""}
    :return:
    """
    tables = SQLParser(sql_text).get_table_name()
    print(tables)
    # assert (tables, 'not found table')
    if not tables:
        raise Exception('not found table')
    kwargs['table_name'] = tables[0]
    schema_table_list = AiSrSchemaTable.objects.filter(**kwargs)
    if len(schema_table_list) != 1:
        raise Exception('table 与 schema 信息不匹配')
    schema_id = schema_table_list[0].schema_id
    schema_info = AiSrTenantSchema.objects.get(id=schema_id)
    return schema_info


def get_tag_list(app_name, repo_url):
    """

    :param app_name:
    :param repo_url:
    :return:
    """
    repo = GitRepository(app_name=app_name, repo_url=repo_url)
    try:
        repo.run('{0} -C {1}  pull --tags '.format(repo.git_bin_path, repo.repo_path))
    except Exception as e:
        return []
    return list(filter(lambda _d: _d.strip(), repo.tag_list().split('\n')))


def switch_tag(app_name, repo_url, tag):
    """

    :param tag:
    :param app_name:
    :param repo_url:
    :return:
    """
    repo = GitRepository(app_name=app_name, repo_url=repo_url)
    try:
        repo.run('{0} -C {1}  pull --tags '.format(repo.git_bin_path, repo.repo_path))
    except Exception as e:
        return []
    repo.checkout(tag)
    return repo


def parse_repo_xml_by_tag(app_name, tag):
    """

    :param app_name:
    :param tag:
    :return:
    """
    xml_parser = XmlSqlParser(file_path="/soft/1xsd.xml")
    xml_parser.get_sql_by_file()


def exec_task_by_task_info(**task_info):
    """
    async_exec_task_by_task_info
    :param task_info:
    :return:
    """
    sql_info = Dict2Obj(task_info['sql_info'])
    task_id = task_info['task_id']
    obj = AiSrTaskSql.objects.filter(task_id=task_id)
    try:
        start = time.time()
        sql_handle_and_write = SQLHandleAndWrite(sql_info, task_info['review_request_id'],
                                                 task_info['sql_sequence'])
        ai_result = sql_handle_and_write.sql_handle_and_write()
        end = time.time()
        logging.info('task info time: %s %s, %s', end-start, task_info['sql_sequence'], ai_result)
        if ai_result:
            query_set = AiSrTaskSql.objects.raw(f"""
            SELECT * FROM ai_sr_task_sql a
            LEFT JOIN ai_sr_task b ON a.task_id = b.task_id 
            WHERE a.sql_md5 = '{task_info["sql_md5"]}'  AND b.app_name = '{task_info["app_name"]}'  AND a.review_status = '1' AND a.valid = '1'
            """)
            if query_set:
                review_compare = query_set[-1].sql_sequence
                logging.info('exec_task_by_task_info 人工审核通过 %s', query_set[-1].sql_sequence)
                current_task = AiSrTaskSql.objects.get(sql_sequence=task_info['sql_sequence'])
                current_task.review_status = '1'
                current_task.review_compare = review_compare
                current_task.save()
    except Exception as e:
        logger.error('exec_task_by_sql_list error, %s', task_info['sql_sequence'])
        logger.exception('pallas api exec_task_by_sql_list error, %s' % e)
    ai_ret = obj.filter(ai_result=None).count()
    if ai_ret == 0:
        # sql_need_optimize = obj.exclude(ai_result='PASS').exclude(review_status='1').count()
        sql_need_optimize = obj.filter(review_status='0').count()
        task_update = {"sql_need_optimize": sql_need_optimize, "task_status": "SUCCESS",
                       "updated_at": datetime.now(), "tag_review_result": 'READY'}
        if sql_need_optimize == 0:
            task_update['tag_review_result'] = 'SUCCESS'
        AiSrReviewRequest.objects.filter(task_id=task_id).update(sql_cnt=sql_need_optimize, review_status="SUCCESS",
                                                                 updated_at=datetime.now())
        AiSrTask.objects.filter(task_id=task_id).update(**task_update)
    return task_info['sql_sequence']


def exec_task_by_sql_sequence(sql_sequence):
    """
    执行单个任务
    :param sql_sequence:
    :return:
    """
    sql_detail = AiSrReviewDetail.objects.get(sequence=sql_sequence)
    task_detail = AiSrTaskSql.objects.get(sql_sequence=sql_sequence)
    app_name = AiSrTask.objects.get(task_id=task_detail.task_id).app_name
    sql_data = {
        'sql_text': sql_detail.sql_new_text,
        'sqlmap_files': [sql_detail.sqlmap_files],
        'namespace': sql_detail.namespace,
        'sqlmap_piece_id': sql_detail.sqlmap_id,
    }
    sql_info = Dict2Obj(sql_data)
    task_list = [
        {
            'sql_info': sql_info,
            'review_request_id': sql_detail.review_request_id,
            'sql_sequence': sql_sequence,
            'task_id': task_detail.task_id,
            'sql_md5': sql_detail.sql_md5,
            'app_name': app_name,
        }
    ]
    return exec_task_by_sql_list(task_list, update_task=False)


def exec_task_by_git(app_name, repo_url, task_id, tag, username, tenant):
    """

    :param app_name:
    :param repo_url:
    :param task_id:
    :param tag:
    :param username:
    :param tenant:
    review_status:  INIT-初始化, PARSING-分析中, PARSE FAILED- 分析失败, READY-就绪,
                    REVIWING-评审中, SUCCESS-通过, FAIL-不通过',
    version_type    ADD-新增; MODIFY-修改',
    :return:
    """
    review = AiSrReviewRequest.objects.filter(task_id=task_id)
    review_id = review[0].id
    review.update(review_status="PARSING")

    repo = switch_tag(app_name, repo_url, tag)
    # xml_parser = XmlSqlParser('/wls/source/%s' % app_name)
    task_sql_list = []
    task_list = []
    review_detail_list = []

    ai_task = AiSrTask.objects.filter(task_id=task_id)
    xml_parser = XmlSqlParser(app_path=repo.repo_path)
    xml_info = xml_parser.get_sql_by_app()
    for file_path in repo.search_files(filter_regex=".xml$"):
        with open(file_path, 'rb') as f:
            contents = f.read()
            md5 = hashlib.md5(contents).hexdigest()
        sql_map_data = {
            "created_by": username,
            "updated_by": username,
            'review_request_id': review_id,
            'version': tag,
            'version_type': 'ADD',
            'filepath': file_path,
            'md5': md5,
            'content': contents
        }
        sql_map_ret = AiSrAppSqlmap().create(**sql_map_data)

    for sql_obj in xml_info:
        _uuid = uuid.uuid4().hex.replace('-', '')
        # schema_info = get_schema_by_sql(sql_obj.sql_text, **{'userid': username, 'tenant': tenant})
        sql_md5 = hashlib.md5(sql_obj.sql_text.encode('utf-8')).hexdigest()
        data = {
            "created_by": username,
            "updated_by": 'pallas',
            # "userid": username,
            # "tenant": tenant,
            'task_id': task_id,
            'sql_sequence': _uuid,
            # 'db_type': schema_info.db_type,
            # 'schema_name': schema_info.schema_name,
            'sql_text': sql_obj.sql_text,
            'sql_md5': sql_md5,
            'valid': ai_task[0].valid,
        }
        task_sql_list.append(AiSrTaskSql(**data))
        review_detail_data = {
            "created_by": username,
            "updated_by": username,
            'review_request_id': review_id,
            'namespace': sql_obj.namespace,
            'sqlmap_id': sql_obj.sqlmap_piece_id,
            'sql_new_text': sql_obj.sql_text,
            'sqlmap_files': sql_obj.sqlmap_files,
            'sequence': _uuid,
            'sql_md5': sql_md5,
        }
        review_detail_list.append(AiSrReviewDetail(**review_detail_data))

        data['userid'] = username
        data['tenant'] = tenant
        data['review_request_id'] = review_id
        data['sql_info'] = sql_obj.__dict__
        data['sql_info']['sqlmap_files'] = list(data['sql_info']['sqlmap_files'])
        data['sql_md5'] = sql_md5
        data['app_name'] = app_name

        task_list.append(data)
    if task_list:
        ai_task.update(sql_total=len(task_list), task_status="PROCESSING",
                       updated_at=datetime.now(), tag_review_result='INIT')
        AiSrReviewRequest.objects.filter(task_id=task_id).update(review_status="REVIEWING", updated_at=datetime.now())
        AiSrTaskSql.objects.bulk_create(task_sql_list)
        AiSrReviewDetail.objects.bulk_create(review_detail_list)
    else:
        ai_task.update(task_status="SUCCESS", updated_at=datetime.now(), tag_review_result='SUCCESS')
    # exec_task_by_sql_list(task_list)
    return task_list


def exec_task_by_sql_list(task_list, update_task=True):
    """ t_ """
    sql_need_optimize = 0
    for task_info in task_list:
        sql_info = task_info['sql_info']
        try:
            sql_handle_and_write = SQLHandleAndWrite(sql_info, task_info['review_request_id'],
                                                     task_info['sql_sequence'])
            ai_result = sql_handle_and_write.sql_handle_and_write()
            if ai_result != 'PASS':
                sql_need_optimize += 1
        except Exception as e:
            sql_need_optimize += 1
            logger.exception('pallas api exec_task_by_sql_list error, %s' % e)

    if update_task:
        task_id = task_list[0]['task_id']
        AiSrReviewRequest.objects.filter(task_id=task_id).update(sql_cnt=sql_need_optimize, review_status="SUCCESS",
                                                                 updated_at=datetime.now())
        AiSrTask.objects.filter(task_id=task_id).update(sql_need_optimize=sql_need_optimize, task_status="SUCCESS",
                                                        updated_at=datetime.now())


def table_insert(schema_info, userid, tenant):
    profile_manage = AiSrProfileManage.objects.all()
    for conn in profile_manage:
        conn_data = conn.as_dict()
        if conn.db_type == 'oracle':
            schema_info = get_schema_table_by_oracle(conn_data)
    for schema_name, tables in schema_info.items():
        schema_obj = AiSrTenantSchema.objects.filter(schema_name=schema_name).first()
        data = {
            "created_by": userid,
            "updated_by": userid,
            "userid": userid,
            "tenant": tenant,
            "profile_name": '',
            "db_type": '',
            "schema_name": schema_name,
        }
        if not schema_obj:
            schema_obj = AiSrTenantSchema.objects.create(**data)
        list_to_insert = list()
        # try:
        for table_name in tables:
            data = {
                "created_by": userid,
                "updated_by": userid,
                "userid": userid,
                "tenant": tenant,
                "schema_id": schema_obj.id,
                "table_name": table_name
            }
            if not AiSrSchemaTable.objects.filter(schema_id=schema_obj.id, table_name=table_name).exists():
                list_to_insert.append(AiSrSchemaTable(**data))
        AiSrSchemaTable.objects.bulk_create(list_to_insert)


def u_table_schema_by_profile(schema_info, userid, tenant):
    profile_manage = AiSrProfileManage.objects.all()
    schema_info_list = []
    for conn in profile_manage:
        print(conn.profile_name)
        conn_data = conn.as_dict()
        if conn.db_type.lower() == 'oracle':
            schema_info = get_schema_table_by_oracle(conn_data)
        elif conn.db_type.lower() == 'mysql':
            schema_info = get_schema_table_by_mysql(conn_data)
        else:
            print('不支持的类型: %s' % conn_data)
            continue
        print('schema_name: ', len(schema_info.keys()))
        schema_info_list.append({'userid': conn.userid,
                                 'tenant': conn.tenant,
                                 'profile_name': conn.profile_name,
                                 'db_type': conn.db_type,
                                 'schema_info': schema_info})
        # u_table_schema_by_schema_info(schema_info, conn.userid, conn.tenant, conn.profile_name, conn.db_type)
    AiSrSchemaTable.objects.all().delete()
    AiSrTenantSchema.objects.all().delete()
    for conn in schema_info_list:
        print(conn['profile_name'])
        u_table_schema_by_schema_info(conn['schema_info'], conn['userid'], conn['tenant'], conn['profile_name'], conn['db_type'])


def u_table_schema_by_schema_info(schema_info, userid, tenant, profile_name, db_type):
    for schema_name, tables in schema_info.items():
        data = {
            "created_by": userid,
            "updated_by": userid,
            "userid": userid,
            "tenant": tenant,
            "profile_name": profile_name,
            "db_type": db_type,
            "schema_name": schema_name,
        }
        schema_obj = AiSrTenantSchema.objects.create(**data)
        list_to_insert = list()
        # try:
        # if AiSrSchemaTable.objects.filter(schema_id=schema_obj.id):
        AiSrSchemaTable.objects.filter(schema_id=schema_obj.id).delete()
        for table_name in tables:
            data = {
                "created_by": userid,
                "updated_by": userid,
                "userid": userid,
                "tenant": tenant,
                "schema_id": schema_obj.id,
                "table_name": table_name
            }
            # if not AiSrSchemaTable.objects.filter(schema_id=schema_obj.id, table_name=table_name).exists():
            list_to_insert.append(AiSrSchemaTable(**data))
        AiSrSchemaTable.objects.bulk_create(list_to_insert)
    return


def get_exception_count(task_id):
    ai_task = AiSrTaskSql.objects.filter(task_id=task_id)
    # AiSrTaskSql.objects.filter(task_id='8def7b8dfa35439eafffcfe48f8fda84', ai_result='NOPASS').values(
    #     'ai_program_type').annotate(Count('ai_program_type'))
    ai_program_type = ai_task.filter(ai_result='NOPASS').values('ai_program_type').annotate(
        Count('ai_program_type'))
    ai_error_code = ai_task.filter(ai_result='INVALID').values('ai_error_code').annotate(
        Count('ai_error_code')).order_by('-ai_error_code__count')

    nopass = [{'desc': item['ai_program_type'], 'count': item['ai_program_type__count']} for item in ai_program_type]
    invalid = []
    for item in ai_error_code[:4]:
        type, desc = get_error_type_description(item['ai_error_code'])
        invalid.append({
            'desc': desc, 'count': item['ai_error_code__count'], 'type': type
        })
    other = sum(dict(map(lambda _d: list(_d.values()), ai_error_code[4:])).values())
    invalid.append({'desc': '其他', 'count': other, 'type': 'other'})
    return nopass, invalid


