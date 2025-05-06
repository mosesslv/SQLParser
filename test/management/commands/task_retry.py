#!/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'root'


from django.core.management.base import BaseCommand
from service.GitSQLReview.sqlreview_handle import SQLReviewStruct, SQLReviewHandle, SQLHandleAndWrite
from service.ai_sr_models import AISrReviewRequest, AISrReviewDetail, AISrTaskSql, AISrSQLDetail, AISrTask

class SQL:
    def __init__(self, namespace, sqlmap_piece_id, sql_text, sqlmap_files):
        self.namespace = namespace
        self.sqlmap_piece_id = sqlmap_piece_id
        self.sql_text = sql_text
        self.sqlmap_files = sqlmap_files

def task_retry(task_id):
    review_request = AISrReviewRequest.objects.filter(task_id=task_id)
    if len(review_request) != 1:
        print("获取Review Request信息失败，请确认任务号是否正确！")
        return
    # review_detail_list = AISrReviewDetail.objects.filter(review_request_id=review_request[0].id)

    # if len(review_detail_list) <= 0:
    #     print("获取 Review Detail 信息失败！")
    #     return
    for review_detail in AISrReviewDetail.objects.filter(review_request_id=review_request[0].id).iterator():
        sqlmap_files = []
        sqlmap_files.append(review_detail.sqlmap_files)
        sql_info = SQL(
            namespace=review_detail.namespace,
            sqlmap_piece_id=review_detail.sqlmap_id,
            sql_text=review_detail.sql_new_text,
            sqlmap_files=sqlmap_files
            )
        review_request_id = review_detail.review_request_id
        sequence = review_detail.sequence
        try:
            sql_handle_and_write = SQLHandleAndWrite(sql_info, review_request_id, sequence)
            sql_handle_and_write.sql_handle_and_write()
        except Exception as e:
            print(e)
            continue


class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-t', '--task_id', dest='task_id', default=False, help='task_id')

    def handle(self, *args, **options):
        # if len(args) != 1:
        #     print("请输入唯一参数：任务号!")
        #     exit(1)
        task_id = options.get('task_id')
        task_retry(str(task_id).strip().lower())





