#!/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'root'


from django.core.management.base import BaseCommand
from service.GitSQLReview.xml_sql_parser import XmlSqlParser
from service.GitSQLReview.sqlreview_handle import SQLReviewStruct, SQLReviewHandle
import service.ai_sr_models
from api_service.pallas_api.models import AiSrReviewDetail, AiSrTaskSql

# if __name__ == "__main__":
#     pass

def test():
    # xml_parser = XmlSqlParser(app_path="/wls/source/anshuo-app")
    # xml_parser.get_sql_by_app()
    # xml_parser = XmlSqlParser(file_path="/wls/source/anshuo-app/src/main/resources/config/loan/RepaymentRecord.xml")
    # sql_list = xml_parser.get_sql_by_file()
    # i = 1
    # for sql_info in sql_list:
    #     i = i + 1
    #     sql_struct = SQLReviewStruct()
    #     sql_struct.sql_text = sql_info.sql_text
    #     sql_struct.sqlmap_filename = list(sql_info.sqlmap_files)[0]
    #     sql_struct.namespace = sql_info.namespace
    #     sql_struct.sqlmap_id = sql_info.sqlmap_piece_id
    #     sql_struct.review_request_id = i
    #     sql_handle = SQLReviewHandle(sql_struct)
    #     result = sql_handle.write_database()
    #     print(result)
    review_detail_list = AiSrReviewDetail.objects.filter(delete_mark='1')
    if len(review_detail_list) > 0:
        for detail in review_detail_list:
            ai_recommend = ''
            ai_message = ''
            if detail.ai_result == 'NOPASS':
                ai_recommend = detail.ai_recommend
            elif detail.ai_result == 'INVALID':
                ai_message = detail.ai_message

            AiSrTaskSql.objects.filter(sql_sequence=detail.sequence).update(
                     ai_recommend=ai_recommend, ai_message=ai_message)


class Command(BaseCommand):
    def handle(self, *args, **options):
        test()
        print("finished")
