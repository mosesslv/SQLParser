#!/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'root'


from service.GitSQLReview.xml_sql_parser import XmlSqlParser
# from service.GitSQLReview.sqlreview_handle import SQLReviewStruct
from api_service.pallas_api.models import AiSrReviewDetail, AiSrTaskSql


if __name__ == "__main__":
    # xml_parser = XmlSqlParser(app_path="/wls/source/anshuo-app")
    # xml_parser.get_sql_by_app()
    # xml_parser = XmlSqlParser(file_path="/soft/RepaymentRecord.xml")
    # sql_list = xml_parser.get_sql_by_file()
    # print(list(sql_list))
    # for item in sql_list:
    #     print(item.sqlmap_files)
    #     print(dir(item))
    #     print(list(item.sqlmap_files)[0])

    review_detail_list = AiSrReviewDetail.objects.all()
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

