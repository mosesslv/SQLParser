#!/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'root'


from django.core.management.base import BaseCommand
from service.GitSQLReview.sqlreview_handle import SQLReviewStruct, SQLReviewHandle, SQLHandleAndWrite, SingleSQLHandleAndWrite
from service.ai_sr_models import AISrReviewRequest, AISrReviewDetail, AISrTaskSql, AISrSQLDetail, AISrTask
from api_service.pallas_api.utils import get_schema_by_sql

def sql_retry(task_id):
    sr_task = AISrTask.objects.filter(task_id=task_id)
    if len(sr_task) != 1:
        print("获取task信息失败，请确认任务号是否正确！")
        return
    if sr_task[0].task_type != 'SINGLE':
        print("不是单条SQL的任务，请确认任务号是否正确！")
        return

    task_sql = AISrTaskSql.objects.filter(task_id=sr_task[0].task_id)

    if len(task_sql) != 1:
        print("获取task sql失败，请确认任务号是否正确！")
        return
    tenant = sr_task[0].tenant
    userid = sr_task[0].userid
    sql_sequence = task_sql[0].sql_sequence
    sql_text = task_sql[0].sql_text
    schema_info = get_schema_by_sql(sql_text,**{'tenant': tenant})

    sql_handle_and_write = SingleSQLHandleAndWrite(
                tenant=tenant,
                userid=userid,
                profile_name=schema_info.profile_name,
                sql_text=sql_text,
                sql_sequence=sql_sequence,
                db_type=schema_info.db_type,
                schema_name=schema_info.schema_name)
    sql_handle_and_write.sql_handle_and_write()


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-t', '--task_id', dest='task_id', default=False, help='task_id')

    def handle(self, *args, **options):
        # if len(args) != 1:
        #     print("请输入唯一参数：任务号!")
        #     exit(1)
        task_id = options.get('task_id')
        sql_retry(str(task_id).strip().lower())





